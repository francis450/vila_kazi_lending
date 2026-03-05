"""
Event handlers for the Loan doctype.
"""

import frappe
from frappe import _


def on_submit(doc, method=None):
	"""
	Fired on Loan submit.

	Steps:
	1. Set vk_accrual_cap = loan_amount × 2 (Kenyan legal cap).
	2. Copy vk_due_date from the linked Loan Application if not already set.
	3. Create a Repayment Reconciliation record with status = Expected.
	"""
	_set_accrual_cap(doc)
	_copy_due_date_from_application(doc)
	_create_repayment_reconciliation(doc)

	doc.db_update()


# ---------------------------------------------------------------------------


def _set_accrual_cap(doc):
	loan_amount = doc.loan_amount or 0.0
	doc.vk_accrual_cap = loan_amount * 2.0


def _copy_due_date_from_application(doc):
	if doc.get("vk_due_date"):
		return  # Already set

	if not doc.loan_application:
		return

	payday_date = frappe.db.get_value(
		"Loan Application", doc.loan_application, "vk_payday_date"
	)
	if payday_date:
		doc.vk_due_date = payday_date


def _create_repayment_reconciliation(doc):
	"""
	Create one Repayment Reconciliation record per loan on disbursement.
	Skip if one already exists.
	"""
	existing = frappe.db.get_value(
		"Repayment Reconciliation", {"loan": doc.name}, "name"
	)
	if existing:
		doc.vk_repayment_reconciliation = existing
		return

	due_date = doc.get("vk_due_date")
	if not due_date:
		frappe.msgprint(
			_("vk_due_date is not set on Loan {0}. Repayment Reconciliation not created.").format(
				doc.name
			),
			indicator="orange",
			alert=True,
		)
		return

	# expected_amount = loan_amount + total_interest_payable + any charges
	# Using total_payment if available (standard field on Loan), else loan_amount
	expected_amount = doc.get("total_payment") or doc.loan_amount or 0.0

	rec = frappe.get_doc(
		{
			"doctype": "Repayment Reconciliation",
			"loan": doc.name,
			"borrower": doc.applicant,
			"expected_date": due_date,
			"expected_amount": expected_amount,
			"status": "Expected",
		}
	)
	rec.insert(ignore_permissions=True)

	doc.vk_repayment_reconciliation = rec.name
