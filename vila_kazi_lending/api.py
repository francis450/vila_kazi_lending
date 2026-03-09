"""
Vila Kazi Lending — whitelisted API endpoints called from client-side JavaScript.

All form action buttons (Approve, Decline, Confirm Fast Lane, etc.) call these
functions via frappe.call(). Server-side permission checks are the authoritative
guard — the JS client-side role checks are UX only.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import nowdate


# ---------------------------------------------------------------------------
# Loan Application actions
# ---------------------------------------------------------------------------


@frappe.whitelist()
def set_loan_stage(docname: str, stage: str) -> None:
	"""Generic stage setter. Used by 'Submit for KYC' and similar transitions."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager", "Lender Staff"])
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_loan_stage = stage
	doc.save(ignore_permissions=True)


@frappe.whitelist()
def reject_kyc(docname: str, reason: str) -> None:
	"""Mark KYC as Rejected and hold the application."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager", "Lender Staff"])
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_rejection_reason = reason
	doc.vk_loan_stage = "Pending KYC Verification"
	doc.save(ignore_permissions=True)
	# Trigger N-03 via Borrower Profile kyc_status change
	profile_name = frappe.db.get_value("Borrower Profile", {"customer": doc.applicant}, "name")
	if profile_name:
		frappe.db.set_value("Borrower Profile", profile_name, "kyc_status", "Rejected")


@frappe.whitelist()
def lender_approve(docname: str) -> None:
	"""Lender approves the application from Appraisal Complete or Standard Review."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager"])
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_loan_stage = "Approved"
	doc.save(ignore_permissions=True)


@frappe.whitelist()
def lender_decline(docname: str, reason: str) -> None:
	"""Lender declines the application. Records reason and sends N-05."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager"])
	is_refinancing = frappe.db.get_value("Loan Application", docname, "vk_is_refinancing")
	stage = "Refinancing Declined" if is_refinancing else "Declined"
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_rejection_reason = reason
	doc.vk_loan_stage = stage
	doc.save(ignore_permissions=True)


@frappe.whitelist()
def lender_override_approve(docname: str, notes: str) -> None:
	"""
	Lender overrides a Review Required recommendation.
	Decision notes are mandatory and logged with the approving user.
	"""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager"])
	if not notes or not notes.strip():
		frappe.throw(_("Override notes are required when overriding an appraisal recommendation."))
	stamped_note = f"[Override by {frappe.session.user} on {nowdate()}] {notes.strip()}"
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_decision_notes = stamped_note
	doc.vk_loan_stage = "Approved"
	doc.save(ignore_permissions=True)


@frappe.whitelist()
def lender_confirm_fast_lane(docname: str) -> None:
	"""Lender presses the single Confirm button on the fast-lane card."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager"])
	current_stage = frappe.db.get_value("Loan Application", docname, "vk_loan_stage")
	if current_stage != "Pending Lender Confirm":
		frappe.throw(
			_("Cannot confirm: application is in stage '{0}', expected 'Pending Lender Confirm'.").format(
				current_stage
			)
		)
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_loan_stage = "Pending Disbursement"
	doc.save(ignore_permissions=True)


@frappe.whitelist()
def approve_refinancing(docname: str) -> None:
	"""Lender approves a refinancing request. Triggers compute_refinancing_amounts."""
	_assert_loan_app_exists(docname)
	_require_role(["Lender Manager"])
	doc = frappe.get_doc("Loan Application", docname)
	doc.vk_loan_stage = "Refinancing Approved"
	doc.save(ignore_permissions=True)
	# Trigger calculation (moves stage to Pending Disbursement)
	from vila_kazi_lending.utils import compute_refinancing_amounts

	compute_refinancing_amounts(docname)


@frappe.whitelist()
def get_confirm_card_data(docname: str) -> dict:
	"""Return borrower profile data for the fast-lane confirm card."""
	app = frappe.db.get_value(
		"Loan Application",
		docname,
		["applicant", "applicant_name", "loan_amount", "vk_max_eligible_amount", "vk_payday_date"],
		as_dict=True,
	)
	if not app:
		return {}
	bp = (
		frappe.db.get_value(
			"Borrower Profile",
			{"customer": app.applicant},
			["credit_category", "on_time_repayment_rate", "mpesa_number"],
			as_dict=True,
		)
		or {}
	)
	return {
		"credit_category": bp.get("credit_category", "—"),
		"on_time_rate": bp.get("on_time_repayment_rate", 0),
		"mpesa_number": bp.get("mpesa_number", "—"),
	}


# ---------------------------------------------------------------------------
# Repayment Reconciliation actions (also called from controller)
# ---------------------------------------------------------------------------


@frappe.whitelist()
def compute_max_eligible_preview(net_salary: float, existing_liabilities: float = 0.0) -> float:
	"""Live preview of max eligible amount for the Loan Application form."""
	from vila_kazi_lending.utils import compute_max_eligible

	return compute_max_eligible(float(net_salary or 0), float(existing_liabilities or 0))


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _assert_loan_app_exists(docname: str) -> None:
	if not frappe.db.exists("Loan Application", docname):
		frappe.throw(_("Loan Application {0} not found.").format(docname))


def _require_role(roles: list[str]) -> None:
	# System Manager is a super-role — always permitted
	if "System Manager" in frappe.get_roles():
		return
	if not any(r in frappe.get_roles() for r in roles):
		frappe.throw(
			_("You do not have permission to perform this action. Required role: {0}.").format(
				" or ".join(roles)
			),
			frappe.PermissionError,
		)


def _trigger_on_update_email(docname: str, stage: str) -> None:
	"""Kept for backward compatibility. No longer called — all stage setters now use
	doc.save() which fires on_update_after_submit automatically."""
	pass


# ---------------------------------------------------------------------------
# Borrower Portal API — Customer Self-Service
# ---------------------------------------------------------------------------

# Allowlist of BorrowerProfile fields a borrower may upload via the portal.
_KYC_UPLOAD_FIELDS = frozenset({"national_id_scan", "employment_letter"})


def _get_portal_customer() -> str:
	"""Return the Customer name for the current portal session.

	Raises frappe.PermissionError when the session user is not a Borrower or
	has no linked Customer record.  Called by every borrower API method.
	"""
	from vila_kazi_lending.utils import get_portal_customer

	return get_portal_customer()


@frappe.whitelist()
def submit_loan_application_portal(
	loan_product: str, loan_amount: float, purpose: str
) -> dict:
	"""Create and submit a Loan Application on behalf of the logged-in borrower.

	Pre-fills vk_net_salary and vk_existing_liabilities from the borrower's
	BorrowerProfile.  The application is submitted immediately so it enters
	the workflow (before_submit fires route_workflow).

	Args:
	    loan_type: Name of the Loan Type document.
	    loan_amount: Requested loan amount (KES, positive float).
	    purpose: Short free-text description of the loan purpose.

	Returns:
	    dict with key ``name`` — the created Loan Application name.

	Raises:
	    frappe.PermissionError: If the session user is not a Borrower.
	    frappe.ValidationError: If the borrower already has an active pending
	                            application or if the amount is invalid.
	"""
	customer = _get_portal_customer()

	loan_amount = float(loan_amount or 0)
	if loan_amount <= 0:
		frappe.throw(_("Loan amount must be greater than zero."))

	# Guard: block if an active (non-terminal) application exists
	_TERMINAL_STAGES = {"Approved", "Declined", "Refinancing Declined", "Disbursed", "Repaid"}
	active = frappe.db.get_all(
		"Loan Application",
		filters={
			"applicant": customer,
			"applicant_type": "Customer",
			"docstatus": ["!=", 2],
		},
		fields=["name", "vk_loan_stage"],
		limit=10,
	)
	for rec in active:
		if rec.vk_loan_stage not in _TERMINAL_STAGES:
			frappe.throw(
				_("You already have an active loan application ({0}) in progress. "
				  "Please wait for it to be resolved before applying again.").format(rec.name)
			)

	# Pull borrower financial data from profile
	bp = frappe.db.get_value(
		"Borrower Profile",
		customer,
		["net_salary", "bank"],
		as_dict=True,
	) or {}

	doc = frappe.new_doc("Loan Application")
	doc.applicant_type = "Customer"
	doc.applicant = customer
	doc.loan_product = loan_product
	doc.loan_amount = loan_amount
	doc.description = purpose
	doc.vk_net_salary = bp.get("net_salary") or 0
	doc.vk_borrower_bank = bp.get("bank") or None
	doc.insert(ignore_permissions=True)
	doc.submit()

	return {"name": doc.name}


@frappe.whitelist()
def sign_framework_agreement(fa_name: str, signature_data: str = "") -> dict:
	"""Borrower digitally signs their Loan Framework Agreement.

	Validates ownership, renders agreement + signature block to HTML,
	generates a signed PDF, saves it as a private File, then activates
	the agreement.

	Args:
	    fa_name:        Name of the Loan Framework Agreement document.
	    signature_data: Base64 data-URL of the signature image (PNG/JPG).

	Returns:
	    dict with ``success: True`` and ``signed_pdf_url``.
	"""
	customer = _get_portal_customer()

	fa = frappe.db.get_value(
		"Loan Framework Agreement",
		fa_name,
		["name", "borrower", "status", "agreement_template", "clause_version"],
		as_dict=True,
	)
	if not fa:
		frappe.throw(_("Framework Agreement {0} not found.").format(fa_name))

	if fa.borrower != customer:
		frappe.throw(
			_("You do not have permission to sign this agreement."),
			frappe.PermissionError,
		)

	if fa.status != "Pending Signature":
		frappe.throw(
			_("This agreement cannot be signed: current status is '{0}'.").format(fa.status)
		)

	# Validate signature: must be a base64 image data URL
	if not signature_data or not signature_data.startswith("data:image/"):
		frappe.throw(_("A valid signature image is required."), frappe.ValidationError)

	# Generate the signed PDF
	signed_pdf_url = _generate_signed_agreement_pdf(fa, customer, signature_data)

	update_fields = {
		"status": "Active",
		"signed_date": frappe.utils.today(),
	}
	if signed_pdf_url:
		update_fields["signed_document"] = signed_pdf_url

	frappe.db.set_value("Loan Framework Agreement", fa_name, update_fields)
	frappe.db.commit()

	return {"success": True, "signed_pdf_url": signed_pdf_url or ""}


def _generate_signed_agreement_pdf(fa, customer: str, signature_data: str) -> str | None:
	"""Render the agreement template + embedded signature to PDF and save as a File."""
	try:
		from frappe.utils.pdf import get_pdf
	except ImportError:
		return None

	if not fa.agreement_template:
		return None

	template = frappe.db.get_value(
		"Loan Agreement Template",
		fa.agreement_template,
		["template_content", "version"],
		as_dict=True,
	)
	if not template or not template.template_content:
		return None

	customer_name = frappe.db.get_value("Customer", customer, "customer_name") or customer
	bp = frappe.db.get_value(
		"Borrower Profile",
		{"customer": customer},
		["national_id_number", "employer_name"],
		as_dict=True,
	) or frappe._dict()

	app = frappe.db.get_value(
		"Loan Application",
		{"vk_framework_agreement": fa.name},
		["loan_amount", "vk_payday_date", "rate_of_interest", "vk_loan_security_fee"],
		as_dict=True,
		order_by="creation desc",
	) or frappe._dict()

	loan_amount = app.get("loan_amount") or 0
	security_fee = app.get("vk_loan_security_fee") or 0
	if not security_fee and loan_amount:
		pct = frappe.db.get_single_value("VK Lending Settings", "security_fee_percentage") or 5.0
		security_fee = loan_amount * pct / 100

	today_str = frappe.utils.today()

	jinja_context = {
		"borrower_name": customer_name,
		"national_id": bp.national_id_number or "",
		"employer": bp.employer_name or "",
		"loan_amount": frappe.utils.fmt_money(loan_amount, currency="KES"),
		"payday_date": app.get("vk_payday_date") or "",
		"interest_rate": app.get("rate_of_interest") or 0,
		"loan_security_fee": frappe.utils.fmt_money(security_fee, currency="KES"),
		"signed_date": today_str,
	}

	agreement_body = frappe.utils.jinja.render_template(
		template.template_content, jinja_context
	)

	sig_block = """
<div style="margin-top:48px;padding-top:24px;border-top:2px solid #333;page-break-inside:avoid;">
  <table style="width:100%;border-collapse:collapse;">
    <tr>
      <td style="width:55%;padding-right:24px;vertical-align:bottom;">
        <div style="border-top:1px solid #555;padding-top:6px;">
          <img src="{sig}" alt="Borrower Signature"
               style="max-height:80px;max-width:260px;display:block;margin-bottom:6px;">
          <p style="margin:0;font-size:12px;"><strong>{name}</strong></p>
          <p style="margin:2px 0;font-size:11px;color:#555;">Borrower Signature</p>
        </div>
      </td>
      <td style="width:45%;vertical-align:bottom;">
        <div style="border-top:1px solid #555;padding-top:6px;">
          <p style="margin:0;font-size:13px;">{date}</p>
          <p style="margin:2px 0;font-size:11px;color:#555;">Date Signed</p>
        </div>
      </td>
    </tr>
  </table>
  <p style="margin-top:16px;font-size:11px;color:#888;">
    Agreement Reference: {fa_name} &nbsp;|&nbsp; Clause Version: {version}
  </p>
</div>
""".format(
		sig=signature_data,
		name=customer_name,
		date=today_str,
		fa_name=fa.name,
		version=fa.clause_version or template.version or "v1",
	)

	full_html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: Arial, sans-serif; font-size: 13px; color: #222;
         margin: 30px 40px; line-height: 1.6; }}
</style>
</head>
<body>
{body}
{sig_block}
</body>
</html>""".format(body=agreement_body, sig_block=sig_block)

	try:
		pdf_bytes = get_pdf(full_html)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "Signed Agreement PDF generation failed")
		return None

	fname = "VK-Signed-Agreement-{0}.pdf".format(fa.name)
	_file = frappe.get_doc({
		"doctype": "File",
		"file_name": fname,
		"content": pdf_bytes,
		"attached_to_doctype": "Loan Framework Agreement",
		"attached_to_name": fa.name,
		"is_private": 1,
	})
	_file.save(ignore_permissions=True)
	return _file.file_url


@frappe.whitelist()
def upload_kyc_document(fieldname: str, file_url: str) -> dict:
	"""Attach a KYC document URL to the borrower's BorrowerProfile.

	Only the fields in ``_KYC_UPLOAD_FIELDS`` may be written via this endpoint
	(national_id_scan, employment_letter).  The write uses ignore_permissions
	because the Borrower role has read-only access on BorrowerProfile — the
	allowlist enforces safety instead.

	Args:
	    fieldname: The BorrowerProfile field to update ("national_id_scan"
	               or "employment_letter").
	    file_url: The uploaded file URL (from a prior Frappe file upload).

	Returns:
	    dict with key ``success: True``.

	Raises:
	    frappe.PermissionError: If the session user is not a Borrower.
	    frappe.ValidationError: If fieldname is not in the allowlist.
	"""
	customer = _get_portal_customer()

	if fieldname not in _KYC_UPLOAD_FIELDS:
		frappe.throw(
			_("Invalid field '{0}'. Only KYC document fields may be updated via this endpoint.").format(
				fieldname
			),
			frappe.ValidationError,
		)

	# Validate that file_url is an internal Frappe file (not an external URL)
	if file_url and not file_url.startswith("/files/"):
		frappe.throw(_("Only files uploaded to this system may be attached."))

	frappe.db.set_value(
		"Borrower Profile",
		customer,
		fieldname,
		file_url,
		update_modified=True,
	)
	frappe.db.commit()

	return {"success": True}
