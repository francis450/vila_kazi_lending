import frappe
from frappe.model.document import Document


class LoanAgreementTemplate(Document):
	def on_update(self):
		self._enforce_single_current()
		if self.has_value_changed("version"):
			self._expire_linked_agreements()

	def _enforce_single_current(self):
		"""Only one template can be is_current = 1 at a time."""
		if self.is_current:
			frappe.db.sql(
				"""
				UPDATE `tabLoan Agreement Template`
				SET is_current = 0
				WHERE name != %s AND is_current = 1
				""",
				self.name,
			)

	def _expire_linked_agreements(self):
		"""
		When a template version changes, all Loan Framework Agreements
		that reference this template should be marked Expired so borrowers
		are asked to re-sign against the new version.
		"""
		linked_agreements = frappe.db.get_all(
			"Loan Framework Agreement",
			filters={"agreement_template": self.name, "status": "Active"},
			pluck="name",
		)
		for agreement_name in linked_agreements:
			frappe.db.set_value(
				"Loan Framework Agreement", agreement_name, "status", "Expired"
			)
		if linked_agreements:
			frappe.msgprint(
				f"{len(linked_agreements)} active agreement(s) have been marked Expired "
				f"because the template version changed. Borrowers will need to re-sign.",
				indicator="orange",
			)
