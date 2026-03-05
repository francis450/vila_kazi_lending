"""
Event handlers for the Borrower Profile doctype.
"""

from __future__ import annotations

import frappe
from frappe import _


def on_update(doc, method=None):
	"""
	Fires on every save of Borrower Profile.
	Handles KYC status transitions that need to advance linked Loan Applications.
	"""
	if not doc.has_value_changed("kyc_status"):
		return

	if doc.kyc_status == "Verified":
		_advance_applications_to_appraisal(doc)
		# N-02 is sent via VK-N02 Frappe Notification on the Loan Application (stage → Pending Appraisal).
	elif doc.kyc_status == "Rejected":
		pass  # N-03 is sent via VK-N03 Frappe Notification (vk_rejection_reason change on Loan Application).


# ---------------------------------------------------------------------------


def _advance_applications_to_appraisal(doc):
	"""
	When KYC is verified, advance all linked Loan Applications that are
	waiting in Pending KYC Verification to Pending Appraisal.
	"""
	pending_apps = frappe.db.get_all(
		"Loan Application",
		filters={
			"applicant": doc.customer,
			"vk_loan_stage": "Pending KYC Verification",
			"docstatus": 1,
		},
		pluck="name",
	)
	for app_name in pending_apps:
		frappe.db.set_value("Loan Application", app_name, "vk_loan_stage", "Pending Appraisal")
		frappe.logger("vila_kazi_lending").info(
			f"[KYC Workflow] Advanced {app_name} to Pending Appraisal after KYC verified."
		)


# N-02 and N-03 emails are now sent via Frappe Notification fixtures VK-N02 and VK-N03.
# They trigger on vk_loan_stage and vk_rejection_reason changes on the Loan Application.

def _get_customer_email(customer: str) -> str | None:
	return frappe.db.get_value("Customer", customer, "email_id")
