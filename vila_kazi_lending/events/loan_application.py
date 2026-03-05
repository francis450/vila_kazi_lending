"""
Event handlers for the Loan Application doctype.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import nowdate

from vila_kazi_lending.utils import (
	check_auto_approval_gate,
	compute_max_eligible,
	get_payday_date,
)

# Maps fast-lane gate condition numbers → human-readable note
_GATE_FAIL_NOTES = {
	1: "Framework agreement not active — re-signing required.",
	2: "Requested amount exceeds 50% net salary limit.",
	3: "Recent repayment history does not meet auto-approval threshold.",
}


def on_submit(doc, method=None):
	"""
	Fired on Loan Application submit.

	Steps:
	1.  Resolve payday date from Payday Calendar.
	2.  Compute max_eligible_amount.
	3.  Auto-populate framework_agreement from Borrower Profile.
	4.  Create a linked Loan Appraisal record.
	5.  Run auto-approval gate.
	6.  Detect duplicate applications (Amendment 1).
	7.  Route to the correct workflow stage.
	8.  Send N-01 internal notification.
	"""
	_resolve_payday_date(doc)
	_compute_eligibility(doc)
	_populate_framework_agreement(doc)
	appraisal_name = _create_loan_appraisal(doc)
	_run_auto_approval(doc, appraisal_name)
	_detect_duplicate_application(doc)
	_route_workflow(doc)

	# Persist changes made to the doc during the on_submit handler
	doc.db_update()
	# N-01 New Application email is now sent via the VK-N01 Frappe Notification (event: Submit).


# ---------------------------------------------------------------------------
# on_update — stage-transition email dispatch
# ---------------------------------------------------------------------------


def on_update(doc, method=None):
	"""Fires on every save after submit. Drives email notifications on stage changes."""
	if not doc.has_value_changed("vk_loan_stage"):
		return
	_handle_stage_transition(doc)


# ---------------------------------------------------------------------------


def _resolve_payday_date(doc):
	bank = doc.get("vk_borrower_bank")
	if not bank:
		return

	payday = get_payday_date(bank, nowdate())
	if payday:
		doc.vk_payday_date = payday
	else:
		frappe.msgprint(
			_("No active Payday Calendar record found for bank {0}. vk_payday_date not set.").format(bank),
			indicator="orange",
			alert=True,
		)


def _compute_eligibility(doc):
	net_salary = doc.get("vk_net_salary") or 0.0
	existing_liabilities = doc.get("vk_existing_liabilities") or 0.0
	doc.vk_max_eligible_amount = compute_max_eligible(net_salary, existing_liabilities)


def _populate_framework_agreement(doc):
	"""Fetch the active framework agreement from the Borrower Profile if not already linked."""
	if doc.get("vk_framework_agreement"):
		return

	customer = doc.applicant
	if not customer:
		return

	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": customer},
		["framework_agreement"],
		as_dict=True,
	)
	if profile and profile.framework_agreement:
		doc.vk_framework_agreement = profile.framework_agreement


def _create_loan_appraisal(doc) -> str | None:
	"""Create a Loan Appraisal linked to this application. Returns the new record name."""
	# Don't create a duplicate if one already exists
	existing = frappe.db.get_value("Loan Appraisal", {"loan_application": doc.name}, "name")
	if existing:
		doc.vk_appraisal = existing
		return existing

	# Fetch net_salary snapshot from Borrower Profile
	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["net_salary", "credit_score"],
		as_dict=True,
	)
	net_salary = (profile.net_salary if profile else None) or doc.get("vk_net_salary") or 0.0

	appraisal = frappe.get_doc(
		{
			"doctype": "Loan Appraisal",
			"loan_application": doc.name,
			"borrower": doc.applicant,
			"net_salary": net_salary,
			"existing_liabilities": doc.get("vk_existing_liabilities") or 0.0,
			"requested_amount": doc.loan_amount or 0.0,
		}
	)
	appraisal.insert(ignore_permissions=True)

	doc.vk_appraisal = appraisal.name
	return appraisal.name


def _run_auto_approval(doc, appraisal_name: str | None):
	approved = check_auto_approval_gate(doc.name)
	doc.vk_auto_approved = 1 if approved else 0

	if appraisal_name:
		frappe.db.set_value("Loan Appraisal", appraisal_name, "auto_approved", doc.vk_auto_approved)

	if approved:
		frappe.msgprint(
			_("Loan Application {0} passed all auto-approval criteria.").format(doc.name),
			indicator="green",
			alert=True,
		)


# ---------------------------------------------------------------------------
# Amendment 1 — Duplicate detection (tightened key)
# ---------------------------------------------------------------------------


def _detect_duplicate_application(doc):
	"""
	Hard duplicate:  same applicant + posting_date + loan_amount → stage = Duplicate - Review.
	Soft duplicate:  same applicant + posting_date, different amount → log warning only.

	Skipped if vk_loan_stage is already set (e.g. refinancing flag set it).
	"""
	if doc.vk_loan_stage:
		return
	if not doc.name:
		return

	same_day = frappe.db.sql(
		"""
		SELECT name, loan_amount
		FROM `tabLoan Application`
		WHERE applicant = %s
		  AND posting_date = %s
		  AND name != %s
		  AND docstatus = 1
		""",
		(doc.applicant, doc.posting_date or nowdate(), doc.name),
		as_dict=True,
	)

	for existing in same_day:
		if abs((existing.loan_amount or 0) - (doc.loan_amount or 0)) < 1:
			# Hard duplicate — same applicant, date, and amount
			doc.vk_loan_stage = "Duplicate - Review"
			frappe.log_error(
				f"Hard duplicate: {doc.name} matches {existing['name']} "
				f"(applicant={doc.applicant}, date={doc.posting_date}, amount={doc.loan_amount})",
				"Duplicate Loan Application",
			)
			return
		else:
			# Soft duplicate — different amount, warn but do not block
			frappe.log_error(
				f"Potential duplicate: {doc.name} and {existing['name']} share applicant + date "
				f"but differ in amount ({doc.loan_amount} vs {existing['loan_amount']}). "
				"Manual review recommended.",
				"Potential Duplicate Loan Application",
			)


# ---------------------------------------------------------------------------
# Workflow routing
# ---------------------------------------------------------------------------


def _route_workflow(doc):
	"""
	Sets vk_loan_stage and vk_is_repeat_borrower based on borrower history.
	Skipped if duplicate detection already set the stage.
	"""
	if doc.vk_loan_stage == "Duplicate - Review":
		return

	# WF-03: Refinancing path
	if doc.get("vk_is_refinancing"):
		doc.vk_loan_stage = "Refinancing Requested"
		return

	# Look up Borrower Profile
	profile = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant},
		["kyc_status", "framework_agreement"],
		as_dict=True,
	)

	if not profile or profile.kyc_status != "Verified":
		# New borrower — WF-01
		doc.vk_loan_stage = "Draft"
		doc.vk_is_repeat_borrower = 0
		return

	fa_status = None
	if profile.framework_agreement:
		fa_status = frappe.db.get_value(
			"Loan Framework Agreement", profile.framework_agreement, "status"
		)

	if fa_status == "Active":
		# WF-02: Repeat borrower fast lane
		doc.vk_loan_stage = "Intake"
		doc.vk_is_repeat_borrower = 1
		_run_gate_check(doc)
	else:
		# Profile exists but FA not active — abbreviated re-sign path (WF-01 variant)
		doc.vk_loan_stage = "Draft"
		doc.vk_is_repeat_borrower = 0


def _run_gate_check(doc):
	"""
	Evaluates the three fast-lane eligibility conditions.
	Sets stage to Pending Lender Confirm (pass) or Standard Review (fail).
	"""
	passed, failed_conditions = _check_fast_lane_gate(doc.name)

	if passed:
		doc.vk_loan_stage = "Pending Lender Confirm"
	else:
		doc.vk_loan_stage = "Standard Review"
		notes = "; ".join(
			_GATE_FAIL_NOTES[c] for c in failed_conditions if c in _GATE_FAIL_NOTES
		)
		if notes:
			doc.vk_decision_notes = (doc.vk_decision_notes or "") + f"[Gate Check] {notes}"


def _check_fast_lane_gate(loan_application_name: str) -> tuple[bool, list[int]]:
	"""
	Evaluates the three fast-lane gate conditions.
	Returns (passed, list_of_failed_condition_numbers).
	"""
	app = frappe.db.get_value(
		"Loan Application",
		loan_application_name,
		["applicant", "loan_amount", "vk_max_eligible_amount", "vk_framework_agreement"],
		as_dict=True,
	)
	if not app:
		return False, [1, 2, 3]

	failed = []

	# Condition 1: Active Framework Agreement
	fa_status = None
	if app.vk_framework_agreement:
		fa_status = frappe.db.get_value(
			"Loan Framework Agreement", app.vk_framework_agreement, "status"
		)
	if fa_status != "Active":
		failed.append(1)

	# Condition 2: Within eligibility limit
	if (app.loan_amount or 0) > (app.vk_max_eligible_amount or 0):
		failed.append(2)

	# Condition 3: Last 3 loans on time
	last_three = frappe.db.sql(
		"""
		SELECT rr.status, rr.received_date, rr.expected_date
		FROM `tabRepayment Reconciliation` rr
		WHERE rr.borrower = %s
		  AND rr.status IN ('Received', 'Partial', 'Overdue')
		ORDER BY rr.expected_date DESC
		LIMIT 3
		""",
		app.applicant,
		as_dict=True,
	)
	if not last_three:
		failed.append(3)
	else:
		for rec in last_three:
			if rec.status != "Received":
				failed.append(3)
				break
			if rec.received_date and rec.expected_date and rec.received_date > rec.expected_date:
				failed.append(3)
				break

	return len(failed) == 0, failed


# ---------------------------------------------------------------------------
# Stage-transition email notifications
# ---------------------------------------------------------------------------


def _send_new_application_notification(doc):
	"""N-01 is now delegated to the VK-N01 Frappe Notification fixture (event: Submit)."""
	pass


def _handle_stage_transition(doc):
	"""
	Stage-change emails are now delegated to Frappe Notification fixtures VK-N04 through VK-N17.
	This function is retained as a hook point for non-email side-effects on stage transitions.
	"""
	frappe.logger("vila_kazi_lending").debug(
		f"[Workflow] {doc.name} stage → {doc.vk_loan_stage}"
	)


# ---------------------------------------------------------------------------
# Email helpers
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
	"""Send email to all users with Lender Manager or Lender Staff roles."""
	# Check for single override email in VK Lending Settings
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