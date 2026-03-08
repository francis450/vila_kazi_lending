"""
Retroactively create Loan Framework Agreement records for Loan Applications
that are in 'Pending Agreement Signing' but have no vk_framework_agreement linked.
"""
import frappe
from frappe.utils import today


def execute():
	apps = frappe.get_all(
		"Loan Application",
		filters={"vk_loan_stage": "Pending Agreement Signing", "docstatus": 1},
		fields=["name", "applicant", "vk_framework_agreement", "loan_amount", "rate_of_interest", "vk_payday_date"],
	)

	template = frappe.db.get_value(
		"Loan Agreement Template",
		{"is_current": 1},
		["name", "version"],
		as_dict=True,
	)

	for app in apps:
		if app.vk_framework_agreement:
			continue  # already linked, skip

		# Find the generated PDF attachment for this application
		pdf_url = frappe.db.get_value(
			"File",
			{
				"attached_to_name": app.name,
				"attached_to_doctype": "Loan Application",
				"file_name": ("like", "VK-Agreement-%.pdf"),
			},
			"file_url",
			order_by="creation desc",
		)

		if not template:
			frappe.log_error(
				f"No current Loan Agreement Template found — skipping {app.name}",
				"create_missing_framework_agreements",
			)
			continue

		fa = frappe.get_doc({
			"doctype": "Loan Framework Agreement",
			"borrower": app.applicant,
			"agreement_template": template.name,
			"clause_version": template.version or "v1",
			"status": "Pending Signature",
			"generated_pdf": pdf_url or "",
			"valid_from": today(),
		})
		fa.insert(ignore_permissions=True)

		frappe.db.set_value("Loan Application", app.name, "vk_framework_agreement", fa.name)
		frappe.db.commit()

		frappe.logger("vila_kazi_lending").info(
			f"[Patch] Created FA {fa.name} for {app.name} (borrower: {app.applicant})"
		)
		print(f"Created {fa.name} for {app.name}")
