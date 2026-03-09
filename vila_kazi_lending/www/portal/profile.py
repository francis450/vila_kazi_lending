"""
Vila Kazi Lending — Borrower Profile page
Route: /portal/profile
"""
import frappe

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context

	customer = setup_portal_context(context, "/portal/profile")
	context.title = "My Profile"
	context.kyc_statuses = ["Pending", "Verified", "Rejected"]
