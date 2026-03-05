import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import today


class LoanFrameworkAgreement(Document):
	def on_update(self):
		# If signed_document was just uploaded and signed_date is not yet set,
		# auto-activate the agreement.
		if self.signed_document and not self.signed_date:
			self._activate_agreement()

	def _activate_agreement(self):
		self.db_set("signed_date", today(), notify=True)
		self.db_set("status", "Active", notify=True)
		if not self.valid_from:
			self.db_set("valid_from", today(), notify=True)

		# Update the linked Borrower Profile's active framework agreement
		profile = frappe.db.get_value(
			"Borrower Profile", {"customer": self.borrower}, "name"
		)
		if profile:
			frappe.db.set_value("Borrower Profile", profile, "framework_agreement", self.name)

		# Advance linked Loan Application to Agreement Signed
		self._advance_loan_application_to_signed()

		# N-06: Notify lender + assistant that agreement is active
		self._notify_lender_agreement_active()

		frappe.msgprint(
			_("Agreement activated. Borrower Profile updated."),
			indicator="green",
			alert=True,
		)

	def _advance_loan_application_to_signed(self):
		"""
		Find the most recent open Loan Application for this borrower that is
		in Pending Agreement Signing, and advance it to Agreement Signed.
		"""
		app_name = frappe.db.get_value(
			"Loan Application",
			{
				"applicant": self.borrower,
				"vk_loan_stage": "Pending Agreement Signing",
				"docstatus": 1,
			},
			"name",
			order_by="creation desc",
		)
		if app_name:
			frappe.db.set_value("Loan Application", app_name, "vk_loan_stage", "Agreement Signed")
			# Immediately advance to Pending Disbursement
			frappe.db.set_value(
				"Loan Application", app_name, "vk_loan_stage", "Pending Disbursement"
			)
			frappe.logger("vila_kazi_lending").info(
				f"[FA Activate] Advanced {app_name} to Pending Disbursement after agreement signed."
			)

	def _notify_lender_agreement_active(self):
		"""N-06: Notify lender + assistant that the framework agreement is now active."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		recipients = frappe.db.sql(
			"""
			SELECT DISTINCT u.email
			FROM `tabUser` u
			JOIN `tabHas Role` hr ON hr.parent = u.name
			WHERE hr.role IN ('Lender Manager', 'Lender Staff')
			  AND u.enabled = 1
			  AND u.email IS NOT NULL
			  AND u.email != ''
			""",
			as_list=True,
		)
		emails = [r[0] for r in recipients if r[0]]
		if not emails:
			return
		try:
			frappe.sendmail(
				recipients=emails,
				subject=f"Framework Agreement Active — {customer_name or self.borrower}",
				message=(
					f"The framework agreement for "
					f"<b>{customer_name or self.borrower}</b> "
					f"has been signed and is now active.<br><br>"
					f"<b>Agreement:</b> {self.name}<br>"
					f"<b>Signed Date:</b> {today()}<br><br>"
					"The loan application is now ready for disbursement."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"N-06 agreement active email failed: {self.name}")

	def on_trash(self):
		# Clear the link on the Borrower Profile if this is the active agreement
		profile = frappe.db.get_value(
			"Borrower Profile",
			{"customer": self.borrower, "framework_agreement": self.name},
			"name",
		)
		if profile:
			frappe.db.set_value("Borrower Profile", profile, "framework_agreement", None)
