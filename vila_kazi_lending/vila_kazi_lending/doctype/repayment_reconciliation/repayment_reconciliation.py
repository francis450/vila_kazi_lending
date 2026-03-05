import json

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import date_diff, nowdate, today

# Credit category order for demotion logic
_CATEGORY_ORDER = ["New", "Silver", "Gold", "Platinum"]

# Collections stages that are active (not terminal)
_ACTIVE_STAGES = {"Pending Review", "Collections Active", "Partially Paid", "Promise to Pay", "Escalated"}
# Terminal stages
_TERMINAL_STAGES = {"Paid", "Recovered", "Written Off"}


class RepaymentReconciliation(Document):
	def before_save(self):
		self._compute_variance()
		self._compute_days_overdue()
		self._auto_set_status()
		self._validate_write_off()
		self._validate_promise_to_pay()
		self._validate_escalation()

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

	def _validate_promise_to_pay(self):
		"""Require promise_date when entering Promise to Pay stage."""
		if self.vk_collections_stage == "Promise to Pay" and not self.vk_promise_date:
			frappe.throw(_("Promise-to-Pay Date is required before logging a promise."))

	def _validate_escalation(self):
		"""Require escalation_reason when entering Escalated stage."""
		if self.vk_collections_stage == "Escalated" and not (self.vk_escalation_reason or "").strip():
			frappe.throw(_("Escalation Reason is required before escalating."))

	def on_update(self):
		stage_changed = self.has_value_changed("vk_collections_stage")
		status_changed = self.has_value_changed("status")

		# --- status-driven side effects ---
		if status_changed and self.status == "Received":
			self._update_borrower_profile()
			self._advance_loan_app_to_repaid()
			self._apply_category_impact()

		elif status_changed and self.status == "Partial":
			self._notify_borrower_repayment_partial()

		# --- stage-driven side effects ---
		if stage_changed:
			stage = self.vk_collections_stage
			if stage == "Collections Active":
				self._notify_lender_collections_active()
			elif stage == "Promise to Pay":
				self._notify_borrower_promise_logged()
			elif stage == "Escalated":
				self._notify_lender_escalation()
			elif stage == "Paid":
				# Treat Paid as a full repayment via collections
				self._update_borrower_profile()
				self._advance_loan_app_to_repaid()
				self._apply_category_impact()
			elif stage == "Recovered":
				self._handle_recovery()
			elif stage == "Written Off":
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
		  days_overdue <= 3          → no change
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
			return

		current_category = profile.credit_category or "New"

		if 4 <= days <= 7:
			if current_category in _CATEGORY_ORDER:
				idx = _CATEGORY_ORDER.index(current_category)
				new_category = _CATEGORY_ORDER[max(0, idx - 1)]
			else:
				new_category = "New"
			frappe.db.set_value(
				"Borrower Profile", profile.name, "credit_category", new_category
			)
			frappe.logger("vila_kazi_lending").info(
				f"[Category Impact] {self.borrower}: {current_category} → {new_category} "
				f"(repaid {days} days late on {self.name})"
			)

		elif days >= 8:
			frappe.db.set_value(
				"Borrower Profile", profile.name, "credit_category", "Watch"
			)
			frappe.logger("vila_kazi_lending").info(
				f"[Category Impact] {self.borrower}: set to Watch "
				f"(repaid {days} days late on {self.name})"
			)

	def _handle_write_off(self):
		"""Set Borrower Profile category to Watch and notify lender."""
		if not self.borrower:
			return
		profile_name = frappe.db.get_value(
			"Borrower Profile", {"customer": self.borrower}, "name"
		)
		if profile_name:
			frappe.db.set_value("Borrower Profile", profile_name, "credit_category", "Watch")
		self._notify_lender_write_off()

	def _handle_recovery(self):
		"""Post-escalation recovery: update profile and notify lender."""
		if not self.borrower:
			return
		# Recovery is positive but DPD-impacted — still demote category (was in escalation)
		profile_name = frappe.db.get_value(
			"Borrower Profile", {"customer": self.borrower}, "name"
		)
		if profile_name:
			# Set to Watch for the recovery period (lender can manually upgrade later)
			frappe.db.set_value("Borrower Profile", profile_name, "credit_category", "Watch")
		self._notify_lender_recovery()

	# ---------------------------------------------------------------------------
	# Email notifications
	# ---------------------------------------------------------------------------

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
			frappe.log_error(
				frappe.get_traceback(), f"N-15 partial repayment email failed: {self.name}"
			)

	def _notify_lender_collections_active(self):
		"""Notify lender team that a loan has entered active collections."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		outstanding = (self.expected_amount or 0) - (self.received_amount or 0)
		emails = self._get_lender_emails()
		if not emails:
			return
		try:
			frappe.sendmail(
				recipients=emails,
				subject=f"Loan in Active Collections — {self.name}",
				message=(
					f"Repayment record <b>{self.name}</b> for "
					f"<b>{customer_name or self.borrower}</b> has been moved to "
					f"<b>Collections Active</b>.<br><br>"
					f"<b>Outstanding Balance:</b> KES {frappe.utils.fmt_money(outstanding)}<br>"
					f"<b>Days Overdue:</b> {self.days_overdue or 0}<br><br>"
					"Please review and begin collections follow-up."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(
				frappe.get_traceback(), f"Collections active notification failed: {self.name}"
			)

	def _notify_borrower_promise_logged(self):
		"""Notify borrower that their promise-to-pay has been recorded."""
		email = frappe.db.get_value("Customer", self.borrower, "email_id")
		if not email:
			return
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		try:
			frappe.sendmail(
				recipients=[email],
				subject="Your Loan Repayment Commitment Has Been Recorded",
				message=(
					f"Dear {customer_name or self.borrower},<br><br>"
					f"We have recorded your commitment to repay "
					f"<b>KES {frappe.utils.fmt_money(self.vk_promise_amount or 0)}</b> "
					f"by <b>{self.vk_promise_date}</b>.<br><br>"
					"Please ensure payment is made on or before the committed date to avoid further action."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(
				frappe.get_traceback(), f"Promise-to-pay borrower notification failed: {self.name}"
			)

	def _notify_lender_escalation(self):
		"""Notify lender team of escalation to formal recovery."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		outstanding = (self.expected_amount or 0) - (self.received_amount or 0)
		emails = self._get_lender_emails()
		if not emails:
			return
		try:
			frappe.sendmail(
				recipients=emails,
				subject=f"Loan Escalated to Formal Recovery — {self.name}",
				message=(
					f"Repayment record <b>{self.name}</b> for "
					f"<b>{customer_name or self.borrower}</b> has been <b>Escalated</b>.<br><br>"
					f"<b>Outstanding Balance:</b> KES {frappe.utils.fmt_money(outstanding)}<br>"
					f"<b>Days Overdue:</b> {self.days_overdue or 0}<br>"
					f"<b>Escalation Reason:</b> {self.vk_escalation_reason or 'Not provided'}<br><br>"
					"Formal recovery procedures should now be initiated."
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(
				frappe.get_traceback(), f"Escalation notification failed: {self.name}"
			)

	def _notify_lender_recovery(self):
		"""Notify lender of successful recovery."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		emails = self._get_lender_emails()
		if not emails:
			return
		try:
			frappe.sendmail(
				recipients=emails,
				subject=f"Loan Recovered — {self.name}",
				message=(
					f"Repayment record <b>{self.name}</b> for "
					f"<b>{customer_name or self.borrower}</b> has been marked <b>Recovered</b>.<br><br>"
					f"<b>Recovered Amount:</b> KES {frappe.utils.fmt_money(self.vk_recovery_amount or 0)}<br>"
					f"<b>Recovery Date:</b> {self.vk_recovery_date or 'Not specified'}<br>"
					f"<b>Notes:</b> {self.vk_recovery_notes or 'None'}"
				),
				now=frappe.flags.in_test,
			)
		except Exception:
			frappe.log_error(
				frappe.get_traceback(), f"Recovery notification failed: {self.name}"
			)

	def _notify_lender_write_off(self):
		"""Internal notification when a loan is written off."""
		customer_name = frappe.db.get_value("Customer", self.borrower, "customer_name")
		outstanding = (self.expected_amount or 0) - (self.received_amount or 0)
		emails = self._get_lender_emails()
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
			frappe.log_error(
				frappe.get_traceback(), f"Write-off notification failed: {self.name}"
			)

	def _get_lender_emails(self) -> list[str]:
		"""Return email addresses of all enabled Lender Manager and Lender Staff users."""
		result = frappe.db.sql(
			"""
			SELECT DISTINCT u.email FROM `tabUser` u
			JOIN `tabHas Role` hr ON hr.parent = u.name
			WHERE hr.role IN ('Lender Manager', 'Lender Staff')
			  AND u.enabled = 1 AND u.email IS NOT NULL AND u.email != ''
			""",
			as_list=True,
		)
		return [r[0] for r in result if r[0]]

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
	def activate_collections(self) -> None:
		"""Move from Pending Review → Collections Active."""
		self.db_set("vk_collections_stage", "Collections Active", notify=True)
		self._notify_lender_collections_active()

	@frappe.whitelist()
	def mark_partial_payment(
		self,
		received_amount: float,
		received_date: str,
		payment_reference: str = "",
	) -> None:
		"""Record a partial payment and move to Partially Paid."""
		self.db_set(
			{
				"received_amount": float(received_amount),
				"received_date": received_date,
				"payment_reference": payment_reference,
				"status": "Partial",
				"vk_collections_stage": "Partially Paid",
			},
			notify=True,
		)
		self._compute_variance()
		self.db_set("variance", self.variance)
		self._notify_borrower_repayment_partial()

	@frappe.whitelist()
	def resume_collections(self) -> None:
		"""Move from Partially Paid → Collections Active to continue follow-up."""
		self.db_set("vk_collections_stage", "Collections Active", notify=True)

	@frappe.whitelist()
	def log_promise_to_pay(
		self,
		promise_date: str,
		promise_amount: float,
		notes: str = "",
	) -> None:
		"""Record a borrower promise and move to Promise to Pay."""
		if not promise_date:
			frappe.throw(_("Promise-to-Pay Date is required."))
		self.db_set(
			{
				"vk_promise_date": promise_date,
				"vk_promise_amount": float(promise_amount or 0),
				"vk_collections_stage": "Promise to Pay",
			},
			notify=True,
		)
		if notes:
			self._append_contact_log_entry("Promise logged", notes)
		self._notify_borrower_promise_logged()

	@frappe.whitelist()
	def promise_kept(
		self,
		received_amount: float,
		received_date: str,
		payment_reference: str = "",
	) -> None:
		"""Borrower honoured their promise. Record full payment, advance to Paid."""
		self.db_set(
			{
				"received_amount": float(received_amount),
				"received_date": received_date,
				"payment_reference": payment_reference,
				"status": "Received",
				"vk_collections_stage": "Paid",
			},
			notify=True,
		)
		self._compute_variance()
		self.db_set("variance", self.variance)
		self._update_borrower_profile()
		self._advance_loan_app_to_repaid()
		self._apply_category_impact()

	@frappe.whitelist()
	def promise_broken(self, notes: str = "") -> None:
		"""Borrower failed to honour their promise. Return to Collections Active."""
		self.db_set("vk_collections_stage", "Collections Active", notify=True)
		if notes:
			self._append_contact_log_entry("Promise broken", notes)

	@frappe.whitelist()
	def escalate(self, reason: str) -> None:
		"""Escalate to formal recovery. Requires Lender Manager role."""
		if not frappe.db.exists(
			"Has Role", {"parent": frappe.session.user, "role": "Lender Manager"}
		):
			frappe.throw(_("Only a Lender Manager can escalate a loan."), frappe.PermissionError)
		if not (reason or "").strip():
			frappe.throw(_("Escalation reason is required."))
		self.db_set(
			{
				"vk_escalation_reason": reason.strip(),
				"vk_collections_stage": "Escalated",
			},
			notify=True,
		)
		self._notify_lender_escalation()

	@frappe.whitelist()
	def mark_paid(
		self,
		received_amount: float,
		received_date: str,
		payment_reference: str = "",
	) -> None:
		"""Mark as fully paid via collections (no escalation required)."""
		self.db_set(
			{
				"received_amount": float(received_amount),
				"received_date": received_date,
				"payment_reference": payment_reference,
				"status": "Received",
				"vk_collections_stage": "Paid",
			},
			notify=True,
		)
		self._compute_variance()
		self.db_set("variance", self.variance)
		self._update_borrower_profile()
		self._advance_loan_app_to_repaid()
		self._apply_category_impact()

	@frappe.whitelist()
	def mark_recovered(
		self,
		recovery_date: str,
		recovery_amount: float,
		notes: str = "",
	) -> None:
		"""Mark as recovered post-escalation. Requires Lender Manager role."""
		if not frappe.db.exists(
			"Has Role", {"parent": frappe.session.user, "role": "Lender Manager"}
		):
			frappe.throw(
				_("Only a Lender Manager can mark a loan as recovered."), frappe.PermissionError
			)
		self.db_set(
			{
				"vk_recovery_date": recovery_date,
				"vk_recovery_amount": float(recovery_amount or 0),
				"vk_recovery_notes": notes,
				"vk_collections_stage": "Recovered",
			},
			notify=True,
		)
		self._handle_recovery()

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

	# ---------------------------------------------------------------------------
	# Internal helpers
	# ---------------------------------------------------------------------------

	def _append_contact_log_entry(self, event: str, notes: str) -> None:
		"""Append a system-generated entry to vk_contact_log."""
		try:
			log = json.loads(self.vk_contact_log or "[]")
		except Exception:
			log = []
		log.append(
			{
				"date": today(),
				"channel": "System",
				"outcome": f"{event}: {notes}",
				"next_followup": "",
				"logged_by": frappe.session.user,
			}
		)
		self.db_set("vk_contact_log", json.dumps(log, indent=2))

	# ---------------------------------------------------------------------------
	# Backward compat alias (old JS called set_collections_active)
	# ---------------------------------------------------------------------------

	@frappe.whitelist()
	def set_collections_active(self) -> None:
		"""Backward-compat alias → activate_collections."""
		self.activate_collections()
