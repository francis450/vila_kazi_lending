"""
Event handlers for the Loan Application doctype.

Hook entry points (registered in hooks.py):
    before_submit          → setup: routing flags, payday, eligibility, appraisal, dupe check
    on_submit              → gate check routing: fast-lane vs standard review
    on_update_after_submit → side-effects: agreement generation (Approved), RR creation (Disbursed)
    on_update              → stage-change notification dispatch
    before_workflow_action → fast-lane safety guard (validate_fast_lane)
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import add_days, nowdate

from vila_kazi_lending.utils import (
	check_auto_approval_gate,
	compute_max_eligible,
	get_payday_date,
	route_workflow,
)


# ---------------------------------------------------------------------------
# before_submit — setup work before the document is committed to docstatus=1
# ---------------------------------------------------------------------------


def before_submit(doc, method=None):
	"""Run all setup checks and population before a Loan Application is submitted.

	This fires while the transaction is still open (docstatus still 0 in memory),
	so mutations on `doc` will be committed when the submit completes.

	Steps:
	1. Route workflow flags (vk_is_refinancing, vk_is_repeat_borrower, vk_loan_stage).
	2. Resolve the next payday date from the Payday Calendar.
	3. Compute max eligible amount from VK Lending Settings ratio.
	4. Auto-populate framework agreement from Borrower Profile if not already linked.
	5. Create a Loan Appraisal record (idempotent).
	6. Detect duplicate applications submitted within the last 24 hours.
	"""
	try:
		# 1. Routing flags — sets vk_is_refinancing/vk_is_repeat_borrower/vk_loan_stage
		route_workflow(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: route_workflow")

	try:
		# 2. Payday date — use posting_date as the reference; fall back to today
		_resolve_payday_date(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: payday")

	try:
		# 3. Max eligible amount
		_compute_eligibility(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: eligibility")

	try:
		# 4. Framework agreement + bank from Borrower Profile (best-effort enrichment)
		_populate_framework_agreement(doc)
		_populate_borrower_bank(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: framework_agreement")

	try:
		# 5. Loan Appraisal — idempotent; stores name in doc.vk_appraisal
		_create_loan_appraisal(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: appraisal")

	try:
		# 6. Duplicate detection — sets vk_duplicate_flag=1 and adds a comment if found
		_detect_duplicate_application(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.before_submit: duplicate_check")


# ---------------------------------------------------------------------------
# on_submit — gate-check routing (fires after docstatus flips to 1)
# ---------------------------------------------------------------------------


def on_submit(doc, method=None):
	"""Route the application to Pending Lender Confirm or Standard Review.

	Only applies to repeat borrowers on a non-refinancing path.  The gate
	check evaluates Framework Agreement, amount eligibility, repayment history,
	and Watch-category status.

	Persist changes back to the DB via db_update() because Frappe has already
	written docstatus=1 before this hook fires.
	"""
	try:
		# Only the fast-lane path needs gate routing here.
		# Refinancing and new-borrower paths have their stage set by route_workflow.
		if doc.get("vk_is_repeat_borrower") and not doc.get("vk_is_refinancing"):
			_run_auto_approval_gate(doc)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "LoanApplication.on_submit: gate_routing")

	# Persist any changes written to doc fields above
	doc.db_update()


# ---------------------------------------------------------------------------
# on_update_after_submit — side-effects on stage transitions
# ---------------------------------------------------------------------------


def on_update_after_submit(doc, method=None):
	"""React to vk_loan_stage transitions on a submitted Loan Application.

	Fires on every save after submit.  Checks whether the stage changed and
	routes to the appropriate handler.

	"Approved"  → Generate (or skip) Framework Agreement PDF;
	              advance stage to "Pending Agreement Signing".
	"Disbursed" → Auto-create Repayment Reconciliation record.
	"""
	if not doc.has_value_changed("vk_loan_stage"):
		return

	new_stage = doc.vk_loan_stage

	if new_stage == "Approved":
		try:
			_handle_approval(doc)
		except Exception:
			frappe.log_error(frappe.get_traceback(), "LoanApplication.on_update_after_submit: Approved")

	elif new_stage == "Disbursed":
		try:
			_handle_disbursement(doc)
		except Exception:
			frappe.log_error(frappe.get_traceback(), "LoanApplication.on_update_after_submit: Disbursed")

	# Always log stage transition for audit trail
	_handle_stage_transition(doc)


# ---------------------------------------------------------------------------
# on_update — notification dispatch (stage-change emails via Frappe fixtures)
# ---------------------------------------------------------------------------


def on_update(doc, method=None):
	"""Fires on every save after submit.  Drives email notifications on stage changes."""
	if not doc.has_value_changed("vk_loan_stage"):
		return
	_handle_stage_transition(doc)


# ---------------------------------------------------------------------------
# before_workflow_action — fast-lane safety guard
# ---------------------------------------------------------------------------


def validate_fast_lane(doc, method=None):
	"""before_workflow_action handler.

	1. Auto-populate vk_borrower_bank from Borrower Profile if empty, so that
	   mandatory validation doesn't block workflow transitions on older records.
	2. Server-side guard for the Fast Lane Approve action.
	3. Guard for Confirm Disbursement: requires a confirmed Loan Disbursement Source.
	"""
	# Auto-populate missing bank from Borrower Profile (handles legacy records)
	if not doc.get("vk_borrower_bank") and doc.applicant:
		bank = frappe.db.get_value(
			"Borrower Profile", {"customer": doc.applicant}, "bank"
		)
		if bank:
			doc.vk_borrower_bank = bank

	# Fast Lane guard
	if frappe.flags.workflow_action == "Fast Lane Approve":
		if not doc.vk_has_framework_agreement:
			frappe.throw(_("Fast Lane requires an active Framework Agreement."))
		if not doc.vk_clean_repayment_history:
			frappe.throw(_("Fast Lane requires a clean repayment history."))

	# Confirm Disbursement guard — require a confirmed Loan Disbursement Source
	if frappe.flags.workflow_action == "Confirm Disbursement":
		_validate_disbursement_source(doc)


def _validate_disbursement_source(doc):
	"""Guard for 'Confirm Disbursement' workflow action.

	Requires a Loan Disbursement Source with status='Confirmed' linked to
	the Loan that was created from this Loan Application.
	"""
	loan_name = frappe.db.get_value("Loan", {"loan_application": doc.name}, "name")
	if not loan_name:
		frappe.throw(
			_(
				"Cannot confirm disbursement: no Loan record is linked to this application. "
				"Please create the Loan first."
			)
		)

	confirmed_lds = frappe.db.get_value(
		"Loan Disbursement Source",
		{"loan": loan_name, "status": "Confirmed"},
		"name",
	)
	if not confirmed_lds:
		# Check if any LDS exists at all (to give a more helpful message)
		any_lds = frappe.db.get_value(
			"Loan Disbursement Source", {"loan": loan_name}, "name"
		)
		if any_lds:
			frappe.throw(
				_(
					"Cannot confirm disbursement: the Loan Disbursement Source ({0}) "
					"has not been confirmed yet. Please set its status to 'Confirmed' first."
				).format(any_lds)
			)
		else:
			frappe.throw(
				_(
					"Cannot confirm disbursement: no Loan Disbursement Source record exists "
					"for loan {0}. Please record the actual M-Pesa transfer first."
				).format(loan_name)
			)


# ---------------------------------------------------------------------------
# Private helpers — before_submit
# ---------------------------------------------------------------------------


def _resolve_payday_date(doc):
	"""Resolve the next payday date and store in doc.vk_payday_date.

	Uses doc.posting_date as the reference date (falls back to today).
	ValidationError from get_payday_date propagates up to before_submit's
	try/except and is logged rather than surfaced to the user.
	"""
	bank = doc.get("vk_borrower_bank")
	if not bank:
		return

	ref_date = doc.get("posting_date") or nowdate()
	payday = get_payday_date(bank, ref_date)
	doc.vk_payday_date = payday


def _compute_eligibility(doc):
	"""Compute max eligible amount from vk_net_salary and vk_existing_liabilities."""
	net_salary = doc.get("vk_net_salary") or 0.0
	existing_liabilities = doc.get("vk_existing_liabilities") or 0.0
	doc.vk_max_eligible_amount = compute_max_eligible(net_salary, existing_liabilities)


def _populate_framework_agreement(doc):
	"""Fetch the active framework agreement from the Borrower Profile if not already set."""
	if doc.get("vk_framework_agreement"):
		return  # already populated — skip

	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["framework_agreement"],
		as_dict=True,
	)
	if profile and profile.framework_agreement:
		doc.vk_framework_agreement = profile.framework_agreement


def _populate_borrower_bank(doc):
	"""Copy the bank from Borrower Profile if vk_borrower_bank is not set."""
	if doc.get("vk_borrower_bank") or not doc.applicant:
		return
	bank = frappe.db.get_value("Borrower Profile", {"customer": doc.applicant}, "bank")
	if bank:
		doc.vk_borrower_bank = bank


def _create_loan_appraisal(doc) -> str | None:
	"""Create a Loan Appraisal record linked 1:1 to this application.

	Idempotent — returns immediately if one already exists.
	Stores the appraisal name in doc.vk_appraisal.

	Returns:
	    str | None – the Loan Appraisal document name, or None on failure.
	"""
	# Idempotency: Loan Appraisal.loan_application has unique=1
	existing = frappe.db.get_value("Loan Appraisal", {"loan_application": doc.name}, "name")
	if existing:
		doc.vk_appraisal = existing
		return existing

	# Snapshot salary from Borrower Profile; fall back to the custom field on the application
	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["net_salary"],
		as_dict=True,
	)
	net_salary = (profile.net_salary if profile else None) or doc.get("vk_net_salary") or 0.0
	existing_liabilities = doc.get("vk_existing_liabilities") or 0.0
	max_eligible = doc.get("vk_max_eligible_amount") or compute_max_eligible(
		net_salary, existing_liabilities
	)
	loan_amount = doc.loan_amount or 0.0

	appraisal = frappe.get_doc(
		{
			"doctype": "Loan Appraisal",
			"loan_application": doc.name,
			"borrower": doc.applicant,       # Loan Appraisal uses 'borrower' (Link→Customer)
			"net_salary": net_salary,
			"existing_liabilities": existing_liabilities,
			"max_eligible_amount": max_eligible,
			"requested_amount": loan_amount,  # Loan Appraisal field is 'requested_amount'
			"within_limit": 1 if loan_amount <= max_eligible else 0,
		}
	)
	appraisal.insert(ignore_permissions=True)

	doc.vk_appraisal = appraisal.name
	return appraisal.name


def _detect_duplicate_application(doc):
	"""Flag possible duplicate applications submitted within the last 24 hours.

	A duplicate is any submitted Loan Application (docstatus=1) for the same
	applicant + posting_date + loan_amount, created within 24 hours of now,
	excluding the current document itself.

	On detection:
	  - Sets doc.vk_duplicate_flag = 1 (does NOT change vk_loan_stage).
	  - Adds a visible comment to the document.
	  - Logs to frappe error log for audit trail.
	"""
	if not doc.name:
		return  # defensive; shouldn't happen at before_submit

	cutoff = add_days(nowdate(), -1)  # 24-hour lookback window

	duplicates = frappe.db.sql(
		"""
		SELECT name
		FROM `tabLoan Application`
		WHERE applicant    = %(applicant)s
		  AND posting_date = %(posting_date)s
		  AND ABS(COALESCE(loan_amount, 0) - %(loan_amount)s) < 1
		  AND name         != %(name)s
		  AND docstatus     = 1
		  AND creation     >= %(cutoff)s
		LIMIT 1
		""",
		{
			"applicant": doc.applicant,
			"posting_date": doc.posting_date or nowdate(),
			"loan_amount": doc.loan_amount or 0,
			"name": doc.name,
			"cutoff": cutoff,
		},
		as_dict=True,
	)

	if not duplicates:
		return

	existing_name = duplicates[0]["name"]
	doc.vk_duplicate_flag = 1

	doc.add_comment(
		comment_type="Comment",
		text=_("Possible duplicate detected: {0}.").format(existing_name),
	)

	frappe.log_error(
		f"Duplicate flag on {doc.name}: matches {existing_name} "
		f"(applicant={doc.applicant}, date={doc.posting_date}, amount={doc.loan_amount})",
		"Duplicate Loan Application",
	)


# ---------------------------------------------------------------------------
# Private helpers — on_submit
# ---------------------------------------------------------------------------


def _run_auto_approval_gate(doc):
	"""Run the fast-lane gate and set vk_loan_stage accordingly.

	check_auto_approval_gate returns {"passed": bool, "failed_conditions": list[str]}.
	On pass  → "Pending Lender Confirm"
	On fail  → "Standard Review"; failed messages appended to vk_decision_notes.
	"""
	result = check_auto_approval_gate(doc.name)
	passed = result.get("passed", False)
	failed_conditions = result.get("failed_conditions", [])

	doc.vk_auto_approved = 1 if passed else 0

	if passed:
		doc.vk_loan_stage = "Pending Lender Confirm"
	else:
		doc.vk_loan_stage = "Standard Review"
		if failed_conditions:
			gate_note = "[Gate Check] " + "; ".join(failed_conditions)
			doc.vk_decision_notes = (
				((doc.vk_decision_notes or "") + "\n" + gate_note).strip()
			)


# ---------------------------------------------------------------------------
# Private helpers — on_update_after_submit
# ---------------------------------------------------------------------------


def _handle_approval(doc):
	"""Handle the "Approved" stage transition.

	If the borrower already has an Active Framework Agreement:
	    → skip agreement generation; advance to "Pending Agreement Signing".

	Otherwise:
	    → render the current Loan Agreement Template as HTML, convert to PDF,
	      attach to this document, create a Loan Framework Agreement record
	      (status=Pending Signature), link it on vk_framework_agreement, then
	      advance stage.

	Always sets vk_loan_stage = "Pending Agreement Signing" and persists
	via db_update().
	"""
	# Check if borrower already has an active FA (no need to re-generate)
	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["framework_agreement"],
		as_dict=True,
	)
	fa_status = None
	if profile and profile.framework_agreement:
		fa_status = frappe.db.get_value(
			"Loan Framework Agreement", profile.framework_agreement, "status"
		)

	if fa_status != "Active":
		# No active FA — generate PDF and create a new FA record
		pdf_url, template_name, template_version = _generate_and_attach_agreement(doc)
		_create_framework_agreement(doc, pdf_url, template_name, template_version)

	doc.vk_loan_stage = "Pending Agreement Signing"
	doc.db_update()


def _generate_and_attach_agreement(doc):
	"""Render the current Loan Agreement Template to PDF and attach to *doc*.

	Looks up the Loan Agreement Template with is_current=1.
	Renders template_content (Jinja2 HTML) with borrower context variables.
	Converts HTML → PDF and saves as a private File record attached to the
	Loan Application.

	Returns:
	    tuple: (file_url, template_name, template_version) — all None on failure.
	"""
	template = frappe.db.get_value(
		"Loan Agreement Template",
		{"is_current": 1},
		["name", "template_content", "version"],
		as_dict=True,
	)
	if not template or not template.template_content:
		frappe.log_error(
			f"No current Loan Agreement Template found while processing {doc.name}",
			"Agreement Generation Failed",
		)
		return None, None, None

	# Gather borrower context variables expected by the Jinja2 template
	customer_name = (
		frappe.db.get_value("Customer", doc.applicant, "customer_name") or doc.applicant
	)
	bp = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["national_id_number", "employer_name"],
		as_dict=True,
	) or frappe._dict()

	context = {
		"borrower_name": customer_name,
		"national_id": bp.national_id_number or "",
		"employer": bp.employer_name or "",
		"loan_amount": frappe.utils.fmt_money(doc.loan_amount or 0, currency="KES"),
		"payday_date": doc.get("vk_payday_date") or "",
		"interest_rate": doc.get("rate_of_interest") or 0,
		"loan_security_fee": frappe.utils.fmt_money(
			_compute_security_fee(doc), currency="KES"
		),
		"signed_date": frappe.utils.today(),
	}

	# Render template and convert to PDF bytes
	html = frappe.utils.jinja.render_template(template.template_content, context)

	try:
		from frappe.utils.pdf import get_pdf
		pdf_bytes = get_pdf(html)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "Agreement PDF generation failed")
		return None, template.name, template.version

	# Save as a private File record attached to this Loan Application
	fname = f"VK-Agreement-{doc.name}.pdf"
	_file = frappe.get_doc(
		{
			"doctype": "File",
			"file_name": fname,
			"content": pdf_bytes,
			"attached_to_doctype": "Loan Application",
			"attached_to_name": doc.name,
			"is_private": 1,
		}
	)
	_file.save(ignore_permissions=True)

	# Persist URL to custom field if present on the doctype
	if hasattr(doc, "vk_agreement_pdf"):
		doc.vk_agreement_pdf = _file.file_url

	return _file.file_url, template.name, template.version


def _create_framework_agreement(doc, pdf_url, template_name, template_version):
	"""Create a Loan Framework Agreement record (status=Pending Signature) and
	link it to the Loan Application via vk_framework_agreement.

	Idempotent — skips creation if vk_framework_agreement is already set.
	"""
	if doc.get("vk_framework_agreement"):
		return  # already linked

	if not template_name:
		frappe.log_error(
			f"Cannot create Framework Agreement for {doc.name}: no template available.",
			"Framework Agreement Creation Failed",
		)
		return

	fa = frappe.get_doc(
		{
			"doctype": "Loan Framework Agreement",
			"borrower": doc.applicant,
			"agreement_template": template_name,
			"clause_version": template_version or "v1",
			"status": "Pending Signature",
			"generated_pdf": pdf_url or "",
			"valid_from": frappe.utils.today(),
		}
	)
	fa.insert(ignore_permissions=True)

	# Link the new FA back to the loan application
	doc.vk_framework_agreement = fa.name
	frappe.logger("vila_kazi_lending").info(
		f"[Approval] Created Framework Agreement {fa.name} for {doc.name}"
	)


def _handle_disbursement(doc):
	"""Auto-create a Repayment Reconciliation when a Loan Application is Disbursed.

	Idempotent — does nothing if a Repayment Reconciliation already exists
	for the linked Loan.

	expected_amount = loan_amount + flat_interest + security_fee
	Flat interest   = loan_amount × (rate_of_interest / 100)
	                  (one-cycle payday loan assumption)
	"""
	# Locate the Loan created from this Loan Application
	loan_name = frappe.db.get_value("Loan", {"loan_application": doc.name}, "name")
	if not loan_name:
		frappe.log_error(
			f"No Loan found for Loan Application {doc.name} at Disbursed stage. "
			"Repayment Reconciliation not created.",
			"Repayment Reconciliation Auto-Create Failed",
		)
		return

	# Idempotency: Repayment Reconciliation.loan has unique=1
	if frappe.db.get_value("Repayment Reconciliation", {"loan": loan_name}, "name"):
		return

	loan_amount = doc.loan_amount or 0.0
	rate = frappe.db.get_value("Loan", loan_name, "rate_of_interest") or 0.0
	interest = loan_amount * rate / 100  # flat one-cycle interest
	security_fee = _compute_security_fee(doc)
	expected_amount = loan_amount + interest + security_fee

	rr = frappe.get_doc(
		{
			"doctype": "Repayment Reconciliation",
			"loan": loan_name,
			"borrower": doc.applicant,
			"expected_date": doc.get("vk_payday_date") or nowdate(),
			"expected_amount": expected_amount,
			"status": "Expected",
		}
	)
	rr.insert(ignore_permissions=True)


def _compute_security_fee(doc) -> float:
	"""Return the security fee amount for *doc*.

	Uses doc.vk_security_fee_amount if already computed; otherwise calculates
	loan_amount × (security_fee_percentage / 100) from VK Lending Settings.
	"""
	if doc.get("vk_security_fee_amount"):
		return float(doc.vk_security_fee_amount)
	pct = (
		frappe.db.get_single_value("VK Lending Settings", "security_fee_percentage") or 5.0
	)
	return (doc.loan_amount or 0.0) * pct / 100


# ---------------------------------------------------------------------------
# Stage-transition dispatch
# ---------------------------------------------------------------------------


def _handle_stage_transition(doc):
	"""Log stage transitions for audit trail.

	Stage-change emails are delegated to Frappe Notification fixtures
	VK-N04 through VK-N17.  This function is the hook point for any
	non-email side-effects on stage transitions.
	"""
	frappe.logger("vila_kazi_lending").debug(
		f"[Workflow] {doc.name} stage → {doc.vk_loan_stage}"
	)


# ---------------------------------------------------------------------------
# Email / notification helpers
# ---------------------------------------------------------------------------


def _get_borrower_email(doc) -> str | None:
	email = doc.get("applicant_email_address")
	if email:
		return email
	if doc.applicant:
		return frappe.db.get_value("Customer", doc.applicant, "email_id")
	return None


def _get_borrower_mpesa(doc) -> str | None:
	if doc.applicant:
		return frappe.db.get_value(
			"Borrower Profile", {"customer": doc.applicant}, "mpesa_number"
		)
	return None


def _get_outstanding_balance(doc) -> float:
	if doc.get("vk_refinancing_of_loan"):
		rr = frappe.db.get_value(
			"Repayment Reconciliation",
			{"loan": doc.vk_refinancing_of_loan},
			["expected_amount", "received_amount"],
			as_dict=True,
		)
		if rr:
			return (rr.expected_amount or 0) - (rr.received_amount or 0)
	return 0.0


def _notify_internal_users(subject: str, message: str) -> None:
	"""Send email to all Lender Manager/Staff users, or the configured override address."""
	try:
		override_email = frappe.db.get_single_value(
			"VK Lending Settings", "lender_notification_email"
		)
	except Exception:
		override_email = None

	if override_email:
		_send_email(recipients=[override_email], subject=subject, message=message)
		return

	recipients = frappe.db.sql(
		"""
		SELECT DISTINCT u.email
		FROM `tabUser` u
		JOIN `tabHas Role` hr ON hr.parent = u.name
		WHERE hr.role IN ('Lender Manager', 'Lender Staff')
		  AND u.enabled = 1
		  AND u.email IS NOT NULL
		  AND u.email != ''
		""",
		as_list=True,
	)
	emails = [r[0] for r in recipients if r[0]]
	if emails:
		_send_email(recipients=emails, subject=subject, message=message)


def _send_email(recipients: list[str], subject: str, message: str) -> None:
	try:
		frappe.sendmail(
			recipients=recipients,
			subject=subject,
			message=message,
			now=frappe.flags.in_test,
		)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"Email send failed: {subject[:80]}")
