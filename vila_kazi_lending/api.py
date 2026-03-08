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
