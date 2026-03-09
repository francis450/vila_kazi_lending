"""
Vila Kazi Lending — Custom branded login page
Route: /vk-login
"""
import frappe

no_cache = 1


def get_context(context):
	# Already logged-in borrowers go straight to the portal (or the requested page)
	if frappe.session.user != "Guest":
		has_customer = frappe.db.get_value(
			"Portal User",
			{"user": frappe.session.user, "parenttype": "Customer"},
			"parent",
		)
		if has_customer and "Borrower" in frappe.get_roles():
			next_url = frappe.form_dict.get("next") or "/portal"
			if not next_url.startswith("/"):
				next_url = "/portal"
			frappe.local.response["type"] = "redirect"
			frappe.local.response["location"] = next_url
			raise frappe.Redirect(302)
		# Logged in but not a valid borrower — fall through to show login page
		# so they can log out or see an error, rather than looping

	context.title = "Borrower Login — Vila Kazi Lending"
	context.redirect_to = frappe.form_dict.get("next") or "/portal"
	# Strip any protocol-relative or external redirects (open-redirect guard)
	redirect = context.redirect_to
	if redirect and not redirect.startswith("/"):
		context.redirect_to = "/portal"
	context.no_breadcrumbs = True
