"""
Vila Kazi Lending — Loans List page
Route: /portal/loans
"""
import frappe

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context

	customer = setup_portal_context(context, "/portal/loans")
	context.title = "My Loans"

	context.applications = frappe.db.get_all(
		"Loan Application",
		filters={"applicant": customer, "applicant_type": "Customer", "docstatus": ["!=", 2]},
		fields=[
			"name", "loan_amount", "loan_product", "vk_loan_stage",
			"creation", "vk_payday_date", "vk_auto_approved",
		],
		order_by="creation desc",
	)

	context.loans = frappe.db.get_all(
		"Loan",
		filters={"applicant": customer},
		fields=[
			"name", "loan_amount", "loan_product", "status",
			"disbursement_date", "vk_due_date", "vk_borrower_category",
		],
		order_by="creation desc",
	)
