"""
Vila Kazi Lending — Borrower Portal Dashboard
Route: /portal
"""
import frappe
from frappe.utils import fmt_money

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context

	customer = setup_portal_context(context, "/portal")
	context.title = "My Dashboard"

	bp = context.borrower_profile or {}

	# Active loan applications (non-terminal stages)
	_TERMINAL_STAGES = {"Approved", "Declined", "Refinancing Declined", "Disbursed", "Repaid"}
	all_apps = frappe.db.get_all(
		"Loan Application",
		filters={"applicant": customer, "applicant_type": "Customer", "docstatus": ["!=", 2]},
		fields=["name", "loan_amount", "vk_loan_stage", "creation", "loan_product"],
		order_by="creation desc",
		limit=10,
	)

	active_apps = [a for a in all_apps if a.vk_loan_stage not in _TERMINAL_STAGES]
	context.has_active_application = len(active_apps) > 0

	# Recent loans (submitted Loan docs)
	context.recent_loans = frappe.db.get_all(
		"Loan",
		filters={"applicant": customer},
		fields=["name", "loan_amount", "status", "vk_due_date", "disbursement_date", "loan_product"],
		order_by="creation desc",
		limit=5,
	)

	# Next repayment due
	context.next_repayment = frappe.db.get_value(
		"Repayment Reconciliation",
		{"borrower": customer, "status": ["in", ["Expected", "Partial"]]},
		["name", "expected_date", "expected_amount", "status", "loan"],
		as_dict=True,
		order_by="expected_date asc",
	)

	# Recent 5 applications for the activity table
	context.recent_applications = all_apps[:5]

	# KPI data from BorrowerProfile
	context.outstanding_balance = bp.get("outstanding_balance") or 0
	context.on_time_rate = bp.get("on_time_repayment_rate") or 0
	context.active_loan_count = len(active_apps)
	context.credit_category = bp.get("credit_category") or "New"
