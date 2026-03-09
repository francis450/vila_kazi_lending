"""
Vila Kazi Lending — Apply for Loan page
Route: /portal/apply
"""
import frappe
from frappe import _

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context, compute_max_eligible

	customer = setup_portal_context(context, "/portal/apply")
	context.title = "Apply for a Loan"

	bp = context.borrower_profile or {}

	# Guard: block if an active pending application already exists
	_TERMINAL_STAGES = {"Approved", "Declined", "Refinancing Declined", "Disbursed", "Repaid"}
	active_apps = frappe.db.get_all(
		"Loan Application",
		filters={"applicant": customer, "applicant_type": "Customer", "docstatus": ["!=", 2]},
		fields=["name", "vk_loan_stage"],
		limit=10,
	)
	pending = [a for a in active_apps if a.vk_loan_stage not in _TERMINAL_STAGES]
	context.has_active_application = len(pending) > 0
	context.active_application_name = pending[0].name if pending else None

	# Available Loan Types
	context.loan_types = frappe.db.get_all(
		"Loan Product",
		fields=["name", "loan_product_name", "maximum_loan_amount"],
		order_by="loan_product_name asc",
	)

	# Pre-fill financial data from profile
	context.net_salary = bp.get("net_salary") or 0
	context.max_eligible = compute_max_eligible(
		bp.get("net_salary") or 0,
		0,  # existing_liabilities not on BorrowerProfile directly; default 0
	)

	# KYC guard: warn if KYC not verified
	context.kyc_verified = bp.get("kyc_status") == "Verified"
