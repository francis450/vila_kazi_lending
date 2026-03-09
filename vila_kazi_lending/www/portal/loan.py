"""
Vila Kazi Lending — Loan / Application detail page
Route: /portal/loan?name=<docname>
"""
import frappe
from frappe import _

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context, get_settings

	customer = setup_portal_context(context, "/portal/loans")
	context.title = "Loan Detail"

	docname = frappe.form_dict.get("name")
	if not docname:
		frappe.throw(_("No document name provided."), frappe.DoesNotExistError)

	# Try Loan Application first, then Loan
	context.doc_type = None
	context.app = None
	context.loan = None
	context.rr = None

	# ---- Loan Application ----
	if frappe.db.exists("Loan Application", docname):
		# Ownership check before fetching full doc
		owner = frappe.db.get_value("Loan Application", docname, "applicant")
		if owner != customer:
			frappe.throw(_("You do not have permission to view this document."), frappe.PermissionError)
		context.doc_type = "Loan Application"
		context.app = frappe.db.get_value(
			"Loan Application",
			docname,
			[
				"name", "applicant", "loan_amount", "loan_product", "vk_loan_stage",
				"vk_payday_date", "vk_max_eligible_amount", "vk_rejection_reason",
				"vk_decision_notes", "vk_auto_approved", "vk_has_framework_agreement",
				"vk_framework_agreement", "creation", "modified",
			],
			as_dict=True,
		)

	# ---- Loan (submitted) ----
	elif frappe.db.exists("Loan", docname):
		owner = frappe.db.get_value("Loan", docname, "applicant")
		if owner != customer:
			frappe.throw(_("You do not have permission to view this document."), frappe.PermissionError)
		context.doc_type = "Loan"
		context.loan = frappe.db.get_value(
			"Loan",
			docname,
			[
				"name", "applicant", "loan_amount", "loan_product", "status",
				"disbursement_date", "vk_due_date", "vk_repayment_paybill_ref",
				"vk_repayment_reconciliation", "vk_accrual_cap", "vk_borrower_category",
			],
			as_dict=True,
		)
		# Fetch linked Repayment Reconciliation
		rr_name = context.loan.vk_repayment_reconciliation
		if rr_name:
			context.rr = frappe.db.get_value(
				"Repayment Reconciliation",
				rr_name,
				[
					"name", "status", "expected_date", "expected_amount",
					"received_date", "received_amount", "variance", "days_overdue",
					"paybill_account_ref",
				],
				as_dict=True,
			)
	else:
		frappe.throw(_("Document {0} not found.").format(docname), frappe.DoesNotExistError)

	# Paybill info from settings (for repayment instructions)
	settings = get_settings()
	context.paybill_number = settings.paybill_number or "—"
