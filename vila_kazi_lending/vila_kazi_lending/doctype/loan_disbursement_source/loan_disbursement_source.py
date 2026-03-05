import frappe
from frappe import _
from frappe.model.document import Document


class LoanDisbursementSource(Document):
	def validate(self):
		self._validate_recipient_mpesa()

	def _validate_recipient_mpesa(self):
		"""Warn if the recipient M-Pesa number does not match the Borrower Profile."""
		if not self.loan or not self.recipient_mpesa:
			return

		applicant = frappe.db.get_value("Loan", self.loan, "applicant")
		if not applicant:
			return

		profile_mpesa = frappe.db.get_value(
			"Borrower Profile", {"customer": applicant}, "mpesa_number"
		)
		if profile_mpesa and profile_mpesa != self.recipient_mpesa:
			frappe.msgprint(
				_(
					"Warning: Recipient M-Pesa number {0} does not match the Borrower Profile "
					"M-Pesa number {1}. Please verify before confirming."
				).format(self.recipient_mpesa, profile_mpesa),
				indicator="orange",
				alert=True,
			)

	def on_update(self):
		if self.has_value_changed("status") and self.status == "Confirmed":
			self._check_full_disbursement()

	def _check_full_disbursement(self):
		"""
		Check whether the total of all Confirmed Loan Disbursement Source
		records equals the Loan.loan_amount.  Notifies lender if fully funded.
		"""
		loan_amount = frappe.db.get_value("Loan", self.loan, "loan_amount") or 0.0

		total_confirmed = frappe.db.sql(
			"""
			SELECT COALESCE(SUM(amount_disbursed), 0)
			FROM `tabLoan Disbursement Source`
			WHERE loan = %s AND status = 'Confirmed' AND docstatus != 2
			""",
			self.loan,
		)[0][0] or 0.0

		if total_confirmed >= loan_amount:
			frappe.publish_realtime(
				"loan_fully_disbursed",
				{"loan": self.loan, "total_confirmed": total_confirmed},
			)
