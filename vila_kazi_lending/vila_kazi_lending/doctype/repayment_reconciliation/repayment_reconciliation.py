import json

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import date_diff, nowdate, today

# Credit category order for demotion logic
_CATEGORY_ORDER = ["New", "Silver", "Gold", "Platinum"]


class RepaymentReconciliation(Document):
	def before_save(self):
		self._compute_variance()
		self._compute_days_overdue()
		self._auto_set_status()
		self._validate_write_off()

	def _compute_variance(self):
		received = self.received_amount or 0.0
		expected = self.expected_amount or 0.0
		self.variance = received - expected

	def _compute_days_overdue(self):
		if self.status == "Overdue" and self.expected_date:
			self.days_overdue = max(0, date_diff(today(), self.expected_date))
		else:
			self.days_overdue = 0

	def _auto_set_status(self):
		"""
		Auto-advance status based on received_amount:
		  received >= expected → Received
		  0 < received < expected → Partial
		  received == 0 and not Waived → leave unchanged
		"""
		received = self.received_amount or 0.0
		expected = self.expected_amount or 0.0

		if self.status == "Waived":
			return

		if received >= expected and expected > 0:
			self.status = "Received"
		elif 0 < received < expected:
			self.status = "Partial"

	def _validate_write_off(self):
		"""Require a reason when writing off."""
		if self.vk_collections_stage == "Written Off" and not (self.vk_write_off_reason or "").strip():
			frappe.throw(_("Please enter a write-off reason before saving."))

	def on_update(self):
		status_changed = self.has_value_changed("status")
		collections_changed = self.has_value_changed("vk_collections_stage")

		if status_changed and self.status == "Received":
			self._update_borrower_profile()
			self._advance_loan_app_to_repaid()
			self._apply_category_impact()
			# N-14 full-repayment email is sent via VK-N14 Frappe Notification on the
			# Loan Application (vk_loan_stage → Repaid), triggered by _advance_loan_app_to_repaid().

		elif status_changed and self.status == "Partial":
			self._notify_borrower_repayment_partial()

		if collections_changed and self.vk_collections_stage == "Written Off":
			self._handle_write_off()

	# ---------------------------------------------------------------------------
	# Credit history & Borrower Profile
	# ---------------------------------------------------------------------------

	def _update_borrower_profile(self):
		"""Trigger a full credit history recompute on the Borrower Profile."""
		if not self.borrower:
			return
		profile_name = frappe.db.get_value(
			"Borrower Profile", {"customer": self.borrower}, "name"
		)
		if profile_name:
			profile = frappe.get_doc("Borrower Profile", profile_name)
			profile.save(ignore_permissions=True)

	def _advance_loan_app_to_repaid(self):
		"""Set the linked Loan Application's vk_loan_stage to Repaid."""
		if not self.loan:
			return
		app_name = frappe.db.get_value(
			"Loan", self.loan, "loan_application"
		)
		if app_name:
			frappe.db.set_value("Loan Application", app_name, "vk_loan_stage", "Repaid")

	def _apply_category_impact(self):
		"""
		Adjust borrower credit category based on days_overdue at time of repayment:
		  days_overdue <= 3          → no change (note added)
		  days_overdue 4–7           → demote one level
		  days_overdue >= 8          → set Watch, block fast lane
		"""
		if not self.borrower:
			return
		days = self.days_overdue or 0

		profile = frappe.db.get_value(
			"Borrower Profile",
			{"customer": self.borrower},
			["name", "credit_category"],
			as_dict=True,
		)
		if not profile:
			return

		if days <= 3:
			# No change — just note it
			return

		current_category = profile.credit_category or "New"

		if 4 <= days <= 7:
			# Demote one level
			if current_category in _CATEGORY_ORDER:
				idx = _CATEGORY_ORDER.index(current_category)
				new_category = _CATEGORY_ORDER[max(0, idx - 1)]
			else:
				new_category = "New"
			frappe.db.set_value(
				"Borrower Profile",
				profile.name,
				"credit_category",
				new_category,
			)
			frappe.logger("vila_kazi_lending").info(
				f"[Category Impact] {self.borrower}: {current_category} → {new_category} "
				f"(repaid {days} days late on {self.name})"
			)

		elif days >= 8:
			# Set to Watch — blocks fast-lane gate
			frappe.db.set_value(
				"Borrower Profile",
				profile.name,
				"credit_category",
				"Watch",
			)
			frappe.logger("vila_kazi_lending").info(
				f"[Category Impact] {self.borrower}: set to Watch "
				f"(repaid {days} days late on {self.name})"
			)

	def _handle_write_off(self):
		"""
		When vk_collections_stage is set to Written Off:
		  - Set Borrower Profile category to Watch permanently
		  - Notify lender
		"""
		if not self.borrower:
			return
		profile_name = frappe.db.get_value(
			"Borrower Profile", {"customer": self.borrower}, "name"
		)
		if profile_name:
			frappe.db.set_value("Borrower Profile", profile_name, "credit_category", "Watch")

		# Notify lender of write-off
		self._notify_lender_write_off()

	# ---------------------------------------------------------------------------
	# Email notifications
	# ---------------------------------------------------------------------------

	# N-14 is now delegated to VK-N14 Frappe Notification (vk_loan_stage → Repaid on Loan Application).

	def _notify_borrower_repayment_partial(self):
		"""N-15: Partial repayment received."""
		email = frappe.db.get_value("Customer", self.borrower, "email_id")
		if not email:
			return
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		outstanding = (self.expected_amount or 0) - (self.received_amount or 0)
		try:
			frappe.sendmail(
				recipients=[email],
				subject="Partial Payment Received",
				message=(
					f"Dear {customer_name or self.borrower},<br><br>"
					f"We have received a partial payment of "
					f"<b>KES {frappe.utils.fmt_money(self.received_amount or 0)}</b>.<br><br>"
					f"<b>Outstanding Balance:</b> KES {frappe.utils.fmt_money(outstanding)}<br>"
					f"<b>Due Date:</b> {self.expected_date or 'As agreed'}<br><br>"
					"Please contact us to arrange the remaining balance."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"N-15 partial repayment email failed: {self.name}")

	def _notify_lender_write_off(self):
		"""Internal notification when a loan is written off."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		outstanding = (self.expected_amount or 0) - (self.received_amount or 0)

		recipients = frappe.db.sql(
			"""
			SELECT DISTINCT u.email FROM `tabUser` u
			JOIN `tabHas Role` hr ON hr.parent = u.name
			WHERE hr.role IN ('Lender Manager', 'Lender Staff')
			  AND u.enabled = 1 AND u.email IS NOT NULL AND u.email != ''
			""",
			as_list=True,
		)
		emails = [r[0] for r in recipients if r[0]]
		if not emails:
			return
		try:
			frappe.sendmail(
				recipients=emails,
				subject=f"Loan Written Off — {self.name}",
				message=(
					f"Loan repayment record <b>{self.name}</b> for "
					f"<b>{customer_name or self.borrower}</b> has been written off.<br><br>"
					f"<b>Outstanding Balance Written Off:</b> KES {frappe.utils.fmt_money(outstanding)}<br>"
					f"<b>Reason:</b> {self.vk_write_off_reason or 'Not provided'}<br><br>"
					"The borrower's credit category has been set to <b>Watch</b>."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"Write-off notification failed: {self.name}")

	# ---------------------------------------------------------------------------
	# Whitelisted instance methods (called from JS via frm.call)
	# ---------------------------------------------------------------------------

	@frappe.whitelist()
	def log_contact_attempt(
		self,
		contact_date: str,
		channel: str,
		outcome: str,
		next_followup: str = "",
	) -> None:
		"""Append a contact attempt entry to vk_contact_log (JSON array)."""
		try:
			log = json.loads(self.vk_contact_log or "[]")
		except Exception:
			log = []

		log.append(
			{
				"date": contact_date,
				"channel": channel,
				"outcome": outcome,
				"next_followup": next_followup,
				"logged_by": frappe.session.user,
			}
		)
		self.db_set("vk_contact_log", json.dumps(log, indent=2), notify=True)

	@frappe.whitelist()
	def set_collections_active(self) -> None:
		"""Move the reconciliation into Collections Active stage."""
		self.db_set("vk_collections_stage", "Collections Active", notify=True)
		# The existing on_update will pick up the status change if needed

	@frappe.whitelist()
	def write_off_loan(self, reason: str) -> None:
		"""Write off the outstanding balance. Requires Lender Manager role."""
		if not frappe.db.exists(
			"Has Role", {"parent": frappe.session.user, "role": "Lender Manager"}
		):
			frappe.throw(_("Only a Lender Manager can write off a loan."), frappe.PermissionError)
		if not (reason or "").strip():
			frappe.throw(_("Write-off reason is required."))
		self.db_set(
			{
				"vk_collections_stage": "Written Off",
				"vk_write_off_reason": reason.strip(),
			},
			notify=True,
		)
		self._handle_write_off()
