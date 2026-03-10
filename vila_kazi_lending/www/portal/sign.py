"""
Vila Kazi Lending - Framework Agreement Signing page
Route: /portal/sign
"""
import frappe

no_cache = 1


def get_context(context):
	from vila_kazi_lending.utils import setup_portal_context

	customer = setup_portal_context(context, "/portal/sign")
	context.title = "Sign Your Agreement"

	context.fa = frappe.db.get_value(
		"Loan Framework Agreement",
		{"borrower": customer},
		[
			"name", "borrower", "status", "clause_version",
			"agreement_template", "generated_pdf", "signed_document", "signed_date",
			"valid_from", "valid_until", "revocation_reason",
		],
		as_dict=True,
		order_by="creation desc",
	)

	context.agreement_html = ""
	if context.fa and context.fa.status == "Pending Signature" and context.fa.agreement_template:
		try:
			context.agreement_html = _render_agreement_html(customer, context.fa)
		except Exception:
			frappe.log_error(frappe.get_traceback(), "Sign portal: render failed")


def _render_agreement_html(customer, fa):
	template = frappe.db.get_value(
		"Loan Agreement Template",
		fa.agreement_template,
		["template_content", "version"],
		as_dict=True,
	)
	if not template or not template.template_content:
		return ""

	customer_name = frappe.db.get_value("Customer", customer, "customer_name") or customer
	bp = frappe.db.get_value(
		"Borrower Profile",
		{"customer": customer},
		["national_id_number", "employer_name"],
		as_dict=True,
	) or frappe._dict()

	app = frappe.db.get_value(
		"Loan Application",
		{"vk_framework_agreement": fa.name},
		["loan_amount", "vk_payday_date", "rate_of_interest", "vk_loan_security_fee"],
		as_dict=True,
		order_by="creation desc",
	) or frappe._dict()

	loan_amount = app.get("loan_amount") or 0
	security_fee = app.get("vk_loan_security_fee") or 0
	if not security_fee and loan_amount:
		pct = frappe.db.get_single_value("VK Lending Settings", "security_fee_percentage") or 5.0
		security_fee = loan_amount * pct / 100

	jinja_context = {
		"borrower_name": customer_name,
		"national_id": bp.national_id_number or "",
		"employer": bp.employer_name or "",
		"loan_amount": frappe.utils.fmt_money(loan_amount, currency="KES"),
		"payday_date": app.get("vk_payday_date") or "",
		"interest_rate": app.get("rate_of_interest") or 0,
		"loan_security_fee": frappe.utils.fmt_money(security_fee, currency="KES"),
		"signed_date": frappe.utils.today(),
	}

	return frappe.utils.jinja.render_template(template.template_content, jinja_context)
