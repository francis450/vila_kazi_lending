"""
Vila Kazi Lending — Repayments page
Route: /portal/repayments
"""
import frappe

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context, get_settings

	customer = setup_portal_context(context, "/portal/repayments")
	context.title = "My Repayments"

	context.repayments = frappe.db.get_all(
		"Repayment Reconciliation",
		filters={"borrower": customer},
		fields=[
			"name", "loan", "status", "expected_date", "expected_amount",
			"received_date", "received_amount", "variance", "days_overdue",
			"paybill_account_ref",
		],
		order_by="expected_date asc",
	)

	# Summary totals
	total_expected = sum(r.expected_amount or 0 for r in context.repayments)
	total_received = sum(r.received_amount or 0 for r in context.repayments)
	context.total_expected = total_expected
	context.total_received = total_received
	context.total_outstanding = max(0, total_expected - total_received)

	settings = get_settings()
	context.paybill_number = settings.paybill_number or "—"
