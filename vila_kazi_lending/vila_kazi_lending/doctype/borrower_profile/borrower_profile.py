import frappe
from frappe.model.document import Document


class BorrowerProfile(Document):
	def before_save(self):
		self._recompute_credit_history()

	def _recompute_credit_history(self):
		"""
		Recompute roll-up fields from linked Loan and Repayment Reconciliation
		records. These are read-only on the form — never set manually.
		"""
		customer = self.customer
		if not customer:
			return

		# Total disbursed principal across all loans for this customer
		total_borrowed = frappe.db.sql(
			"""
			SELECT COALESCE(SUM(loan_amount), 0)
			FROM `tabLoan`
			WHERE applicant = %s
			  AND applicant_type = 'Customer'
			  AND docstatus = 1
			  AND status NOT IN ('Draft', 'Cancelled')
			""",
			customer,
		)[0][0] or 0.0

		# Total received repayments (Repayment Reconciliation.received_amount
		# where status in Received / Partial)
		total_repaid = frappe.db.sql(
			"""
			SELECT COALESCE(SUM(received_amount), 0)
			FROM `tabRepayment Reconciliation`
			WHERE borrower = %s
			  AND status IN ('Received', 'Partial')
			""",
			customer,
		)[0][0] or 0.0

		# On-time repayment rate: closed loans where received_date <= expected_date
		closed_loans = frappe.db.sql(
			"""
			SELECT COUNT(*)
			FROM `tabRepayment Reconciliation`
			WHERE borrower = %s
			  AND status = 'Received'
			""",
			customer,
		)[0][0] or 0

		on_time_loans = frappe.db.sql(
			"""
			SELECT COUNT(*)
			FROM `tabRepayment Reconciliation`
			WHERE borrower = %s
			  AND status = 'Received'
			  AND received_date <= expected_date
			""",
			customer,
		)[0][0] or 0

		on_time_rate = (on_time_loans / closed_loans * 100) if closed_loans > 0 else 0.0

		self.total_borrowed = total_borrowed
		self.total_repaid = total_repaid
		self.outstanding_balance = total_borrowed - total_repaid
		self.on_time_repayment_rate = round(on_time_rate, 2)

		# Derive credit_category from repayment history if not manually set
		self._update_credit_category(closed_loans, on_time_rate)

	def _update_credit_category(self, closed_loans, on_time_rate):
		if closed_loans == 0:
			self.credit_category = "New"
		elif 1 <= closed_loans <= 3:
			self.credit_category = "Silver"
		elif 4 <= closed_loans <= 9:
			self.credit_category = "Gold"
		elif closed_loans >= 10 and on_time_rate == 100.0:
			self.credit_category = "Platinum"
		else:
			self.credit_category = "Gold"

	def on_update(self):
		if self.has_value_changed("kyc_status") and self.kyc_status == "Verified":
			self._notify_lender_kyc_verified()

	def _notify_lender_kyc_verified(self):
		frappe.publish_realtime(
			"borrower_kyc_verified",
			{"borrower": self.customer, "name": self.name},
		)
