"""
Event handlers for the Loan Disbursement Source doctype.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import today


def on_update(doc, method=None):
	"""
	Fires on every save of Loan Disbursement Source.

	When status → Confirmed, activates the loan and handles the
	refinancing original-loan close (Amendment 2 safe sequence).
	"""
	if not doc.has_value_changed("status") or doc.status != "Confirmed":
		return

	loan_doc = _get_loan(doc)
	if not loan_doc:
		return

	loan_app = _get_loan_application(loan_doc)

	# Amendment 2: close original loan ONLY after new disbursement is confirmed
	if loan_app and loan_app.get("vk_is_refinancing") and loan_app.get("vk_refinancing_of_loan"):
		_close_original_loan(loan_app, loan_doc.name)

	# Advance Loan Application stage: Disbursed → Active
	if loan_app:
		frappe.db.set_value("Loan Application", loan_app.name, "vk_loan_stage", "Disbursed")
		frappe.db.set_value("Loan Application", loan_app.name, "vk_loan_stage", "Active")

	_notify_borrower_disbursement(doc, loan_doc, loan_app)


# ---------------------------------------------------------------------------


def _get_loan(doc):
	if not doc.loan:
		return None
	return frappe.db.get_value(
		"Loan",
		doc.loan,
		["name", "loan_application", "applicant"],
		as_dict=True,
	)


def _get_loan_application(loan_doc):
	if not loan_doc or not loan_doc.loan_application:
		return None
	return frappe.db.get_value(
		"Loan Application",
		loan_doc.loan_application,
		[
			"name",
			"applicant",
			"applicant_name",
			"loan_amount",
			"vk_payday_date",
			"vk_is_refinancing",
			"vk_refinancing_of_loan",
		],
		as_dict=True,
	)


def _close_original_loan(loan_app, new_loan_name: str):
	"""
	Amendment 2: Close the original loan only AFTER the new disbursement is confirmed.

	Sequence:
	  1. Set original RepaymentReconciliation.status = Received (book entry)
	  2. Set original Loan.vk_refinancing_of back-link on the new loan
	"""
	original_loan_name = loan_app.vk_refinancing_of_loan

	# Book-entry close of original repayment reconciliation
	original_rr = frappe.db.get_value(
		"Repayment Reconciliation", {"loan": original_loan_name}, "name"
	)
	if original_rr:
		frappe.db.set_value(
			"Repayment Reconciliation",
			original_rr,
			{
				"status": "Received",
				"payment_reference": f"Closed via refinancing — new loan {new_loan_name}",
				"received_date": today(),
			},
		)

	# Set refinancing back-link on the new Loan
	frappe.db.set_value("Loan", new_loan_name, "vk_refinancing_of", original_loan_name)

	frappe.logger("vila_kazi_lending").info(
		f"[Refinancing Close] Original loan {original_loan_name} closed. New loan: {new_loan_name}"
	)


def _notify_borrower_disbursement(doc, loan_doc, loan_app):
	"""N-08: Notify borrower of confirmed disbursement with repayment instructions."""
	if not loan_app:
		return
	email = frappe.db.get_value("Customer", loan_app.applicant, "email_id")
	if not email:
		return

	rr = frappe.db.get_value(
		"Repayment Reconciliation",
		{"loan": loan_doc.name},
		["expected_amount", "expected_date", "paybill_account_ref"],
		as_dict=True,
	)
	settings = (
		frappe.db.get_value(
			"VK Lending Settings",
			None,
			["paybill_number", "paybill_account_ref_prefix"],
			as_dict=True,
		)
		or {}
	)

	expected_amount = (rr.expected_amount if rr else None) or loan_app.loan_amount or 0
	due_date = (rr.expected_date if rr else None) or loan_app.vk_payday_date or "as agreed"
	paybill = settings.get("paybill_number") or "See agreement"
	account_ref = (
		(rr.paybill_account_ref if rr else None)
		or settings.get("paybill_account_ref_prefix")
		or "Your registered phone number"
	)

	try:
		frappe.sendmail(
			recipients=[email],
			subject=f"Loan Disbursed — {loan_doc.name}",
			message=(
				f"Dear {loan_app.applicant_name or loan_app.applicant},<br><br>"
				f"KES {frappe.utils.fmt_money(loan_app.loan_amount or 0)} "
				"has been disbursed to your M-Pesa.<br><br>"
				f"<b>Repayment Amount:</b> KES {frappe.utils.fmt_money(expected_amount)}<br>"
				f"<b>Due Date:</b> {due_date}<br>"
				f"<b>Paybill Number:</b> {paybill}<br>"
				f"<b>Account Reference:</b> {account_ref}<br><br>"
				"Please ensure your payment is made on time. "
				"Contact us if you need any assistance."
			),
			now=frappe.flags.in_test,
		)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "N-08 disbursement email failed")
