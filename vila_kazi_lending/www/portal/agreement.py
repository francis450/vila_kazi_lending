"""
Vila Kazi Lending — Framework Agreement page
Route: /portal/agreement
"""
import frappe

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context

	customer = setup_portal_context(context, "/portal/agreement")
	context.title = "Framework Agreement"

	context.fa = frappe.db.get_value(
		"Loan Framework Agreement",
		{"borrower": customer},
		[
			"name", "borrower", "status", "clause_version",
			"generated_pdf", "signed_document", "signed_date",
			"valid_from", "valid_until", "revocation_reason",
		],
		as_dict=True,
		order_by="creation desc",
	)
