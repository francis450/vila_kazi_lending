"""
Vila Kazi Lending — background jobs and scheduled tasks
"""

from __future__ import annotations

import frappe
from frappe.utils import date_diff, today


# ---------------------------------------------------------------------------
# Background job: parse M-Pesa statement
# ---------------------------------------------------------------------------


def parse_mpesa_statement(doc_name: str) -> None:
	"""
	Background job triggered when an M-Pesa Statement file is uploaded.

	Phase 1 stub: marks the statement as Parsed and sets placeholder values.
	The full AI/PDF parsing engine will be wired in a later phase.
	"""
	try:
		doc = frappe.get_doc("M-Pesa Statement", doc_name)

		# --- Placeholder: in production this section runs the actual parser ---
		# from vila_kazi_lending.mpesa_parser import parse
		# results = parse(doc.statement_file, doc.period_from, doc.period_to)
		# doc.update(results)
		# ----------------------------------------------------------------------

		doc.db_set("parse_status", "Parsed", notify=True)
		frappe.publish_realtime(
			"mpesa_statement_parsed",
			{"doc_name": doc_name, "status": "Parsed"},
		)

		# If a Loan Appraisal is already linked to this statement, trigger scoring
		linked_appraisals = frappe.db.get_all(
			"Loan Appraisal",
			filters={"mpesa_statement": doc_name},
			pluck="name",
		)
		for appraisal_name in linked_appraisals:
			frappe.enqueue(
				"vila_kazi_lending.tasks.run_appraisal_scoring",
				appraisal_name=appraisal_name,
				queue="long",
				now=frappe.flags.in_test,
			)

	except Exception as exc:
		frappe.db.set_value("M-Pesa Statement", doc_name, "parse_status", "Failed")
		frappe.db.set_value(
			"M-Pesa Statement", doc_name, "parse_error_log", str(exc)
		)
		frappe.log_error(frappe.get_traceback(), "parse_mpesa_statement failed")


# ---------------------------------------------------------------------------
# Background job: AI appraisal scoring (stub for Phase 2)
# ---------------------------------------------------------------------------


def run_appraisal_scoring(appraisal_name: str) -> None:
	"""
	Run the AI scoring engine on a Loan Appraisal.

	Phase 1 stub: no-op. The AI scoring engine will be implemented in a
	later phase. When implemented, it will call doc.set_ai_results(...)
	"""
	pass  # noqa: PIE790


# ---------------------------------------------------------------------------
# Scheduled daily task: mark overdue repayments
# ---------------------------------------------------------------------------


def mark_overdue_repayments() -> None:
	"""
	Daily scheduled job (06:00 EAT).

	1. Marks Expected/Partial records past due as Overdue.
	2. Recomputes days_overdue on all existing Overdue records.
	3. Sends escalation notifications:
	     Day 1  → N-11 borrower email
	     Day 3  → N-12 lender escalation
	     Day 7  → N-13 lender second escalation
	     Day 14+ → weekly digest (Amendment 3: persisted in VK Lending Settings)
	"""
	# --- Mark newly overdue records ---
	newly_overdue = frappe.db.sql(
		"""
		SELECT name, expected_date
		FROM `tabRepayment Reconciliation`
		WHERE status IN ('Expected', 'Partial')
		  AND expected_date < %s
		""",
		today(),
		as_dict=True,
	)

	for rec in newly_overdue:
		days = max(0, date_diff(today(), rec["expected_date"]))
		frappe.db.set_value(
			"Repayment Reconciliation",
			rec["name"],
			{"status": "Overdue", "days_overdue": days, "vk_collections_stage": "Pending Review"},
		)

	if newly_overdue:
		frappe.logger("vila_kazi_lending").info(
			f"mark_overdue_repayments: marked {len(newly_overdue)} records as Overdue."
		)

	# --- Recompute days_overdue and send notifications for all Overdue records ---
	all_overdue = frappe.db.sql(
		"""
		SELECT rr.name, rr.expected_date, rr.borrower,
		       rr.expected_amount, rr.received_amount,
		       c.email_id AS borrower_email, c.customer_name
		FROM `tabRepayment Reconciliation` rr
		LEFT JOIN `tabCustomer` c ON c.name = rr.borrower
		WHERE rr.status = 'Overdue'
		""",
		as_dict=True,
	)

	day1_records, day3_records, day7_records, day14plus_records = [], [], [], []

	for rec in all_overdue:
		days = max(0, date_diff(today(), rec["expected_date"]))
		frappe.db.set_value(
			"Repayment Reconciliation",
			rec["name"],
			"days_overdue",
			days,
			update_modified=False,
		)
		rec["days_overdue"] = days

		if days == 1:
			day1_records.append(rec)
		elif days == 3:
			day3_records.append(rec)
		elif days == 7:
			day7_records.append(rec)
		elif days >= 14:
			day14plus_records.append(rec)

	# Day 1 — N-11 borrower email
	for rec in day1_records:
		_send_overdue_borrower_email(
			rec,
			subject="Loan Repayment Overdue — Action Required",
			message=(
				f"Dear {rec.customer_name or rec.borrower},<br><br>"
				f"Your loan repayment of <b>KES {frappe.utils.fmt_money(rec.expected_amount or 0)}</b> "
				f"is now <b>1 day overdue</b> (was due {rec.expected_date}).<br><br>"
				"Please contact us immediately to arrange payment and avoid additional charges."
			),
		)

	# Day 3 — N-12 lender escalation
	if day3_records:
		_send_lender_escalation(
			day3_records,
			subject_prefix="3-Day Overdue Escalation",
			action_note="Action required.",
		)

	# Day 7 — N-13 lender escalation
	if day7_records:
		_send_lender_escalation(
			day7_records,
			subject_prefix="7-Day Overdue Escalation",
			action_note="Consider initiating a refinancing discussion or formal collections.",
		)

	# Day 14+ — weekly digest (Amendment 3: persistent via VK Lending Settings)
	if day14plus_records:
		_maybe_send_weekly_digest(day14plus_records)


def send_pre_due_reminders() -> None:
	"""
	Daily scheduled job (06:00 EAT).

	Sends pre-due reminders to borrowers:
	  N-09: expected_date = today + 3 days
	  N-10: expected_date = today
	"""
	from frappe.utils import add_days

	three_days_out = add_days(today(), 3)

	# N-09: 3-day reminder
	upcoming = frappe.db.sql(
		"""
		SELECT rr.name, rr.borrower, rr.expected_amount, rr.expected_date,
		       c.email_id AS borrower_email, c.customer_name
		FROM `tabRepayment Reconciliation` rr
		LEFT JOIN `tabCustomer` c ON c.name = rr.borrower
		WHERE rr.status = 'Expected'
		  AND rr.expected_date = %s
		""",
		three_days_out,
		as_dict=True,
	)
	for rec in upcoming:
		_send_reminder_email(
			rec,
			subject="Loan Repayment Due in 3 Days",
			message=(
				f"Dear {rec.customer_name or rec.borrower},<br><br>"
				f"This is a reminder that your loan repayment of "
				f"<b>KES {frappe.utils.fmt_money(rec.expected_amount or 0)}</b> "
				f"is due on <b>{rec.expected_date}</b>.<br><br>"
				"Please ensure your M-Pesa paybill payment is ready on time."
			),
		)

	# N-10: Due today reminder
	due_today = frappe.db.sql(
		"""
		SELECT rr.name, rr.borrower, rr.expected_amount, rr.expected_date,
		       c.email_id AS borrower_email, c.customer_name
		FROM `tabRepayment Reconciliation` rr
		LEFT JOIN `tabCustomer` c ON c.name = rr.borrower
		WHERE rr.status = 'Expected'
		  AND rr.expected_date = %s
		""",
		today(),
		as_dict=True,
	)
	for rec in due_today:
		_send_reminder_email(
			rec,
			subject="Loan Repayment Due Today",
			message=(
				f"Dear {rec.customer_name or rec.borrower},<br><br>"
				f"Your loan repayment of <b>KES {frappe.utils.fmt_money(rec.expected_amount or 0)}</b> "
				f"is due <b>today</b> ({rec.expected_date}).<br><br>"
				"Please make your payment now via the paybill."
			),
		)

	total = len(upcoming) + len(due_today)
	if total:
		frappe.logger("vila_kazi_lending").info(
			f"send_pre_due_reminders: sent {len(upcoming)} 3-day and {len(due_today)} due-date reminders."
		)


# ---------------------------------------------------------------------------
# Notification helpers
# ---------------------------------------------------------------------------


def _send_overdue_borrower_email(rec: dict, subject: str, message: str) -> None:
	email = rec.get("borrower_email")
	if not email:
		return
	try:
		frappe.sendmail(
			recipients=[email],
			subject=subject,
			message=message,
			now=frappe.flags.in_test,
		)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"Overdue email failed for {rec.get('name')}")


def _send_reminder_email(rec: dict, subject: str, message: str) -> None:
	email = rec.get("borrower_email")
	if not email:
		return
	try:
		frappe.sendmail(
			recipients=[email],
			subject=subject,
			message=message,
			now=frappe.flags.in_test,
		)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"Reminder email failed for {rec.get('name')}")


def _send_lender_escalation(records: list, subject_prefix: str, action_note: str) -> None:
	"""Send a batch escalation email to internal lender users."""
	rows = "".join(
		f"<tr>"
		f"<td style='padding:4px 8px;'>{r['name']}</td>"
		f"<td style='padding:4px 8px;'>{r['customer_name'] or r['borrower']}</td>"
		f"<td style='padding:4px 8px;'>KES {frappe.utils.fmt_money((r['expected_amount'] or 0) - (r['received_amount'] or 0))}</td>"
		f"<td style='padding:4px 8px;'>{r['days_overdue']} days</td>"
		f"</tr>"
		for r in records
	)
	table = (
		f"<table border='1' cellspacing='0' cellpadding='0' style='border-collapse:collapse;'>"
		f"<tr style='background:#f0f0f0;'>"
		f"<th style='padding:4px 8px;'>Reconciliation</th>"
		f"<th style='padding:4px 8px;'>Borrower</th>"
		f"<th style='padding:4px 8px;'>Outstanding</th>"
		f"<th style='padding:4px 8px;'>Days Overdue</th>"
		f"</tr>{rows}</table>"
	)

	recipients = _get_lender_emails()
	if not recipients:
		return

	subject = f"[Vila Kazi Lending] {subject_prefix} — {len(records)} loan(s)"
	try:
		frappe.sendmail(
			recipients=recipients,
			subject=subject,
			message=f"{table}<br><br>{action_note}",
			now=frappe.flags.in_test,
		)
	except Exception:
		frappe.log_error(frappe.get_traceback(), f"Lender escalation email failed: {subject}")


def _maybe_send_weekly_digest(records: list) -> None:
	"""
	Amendment 3: Send the weekly 14+ day overdue digest.
	Uses VK Lending Settings.last_overdue_digest_sent for persistence.
	"""
	try:
		settings = frappe.get_single("VK Lending Settings")
		last_sent = settings.last_overdue_digest_sent
	except Exception:
		last_sent = None

	if last_sent and date_diff(today(), str(last_sent)) < 7:
		return  # Digest already sent this week

	rows = "".join(
		f"<tr>"
		f"<td style='padding:4px 8px;'>{r['name']}</td>"
		f"<td style='padding:4px 8px;'>{r['customer_name'] or r['borrower']}</td>"
		f"<td style='padding:4px 8px;'>KES {frappe.utils.fmt_money((r['expected_amount'] or 0) - (r['received_amount'] or 0))}</td>"
		f"<td style='padding:4px 8px;'>{r['days_overdue']} days</td>"
		f"</tr>"
		for r in records
	)
	table = (
		f"<table border='1' cellspacing='0' cellpadding='0' style='border-collapse:collapse;'>"
		f"<tr style='background:#f0f0f0;'>"
		f"<th style='padding:4px 8px;'>Reconciliation</th>"
		f"<th style='padding:4px 8px;'>Borrower</th>"
		f"<th style='padding:4px 8px;'>Outstanding</th>"
		f"<th style='padding:4px 8px;'>Days Overdue</th>"
		f"</tr>{rows}</table>"
	)

	recipients = _get_lender_emails()
	if not recipients:
		return

	try:
		frappe.sendmail(
			recipients=recipients,
			subject=f"[Vila Kazi Lending] Weekly Digest — {len(records)} loan(s) 14+ days overdue",
			message=(
				f"The following loans are 14 or more days overdue and require your personal attention:<br><br>"
				f"{table}<br><br>"
				"No further automated borrower contact has been sent. "
				"All further communication is at your discretion."
			),
			now=frappe.flags.in_test,
		)
		# Persist the sent date (Amendment 3 — not a Redis cache)
		frappe.db.set_single_value("VK Lending Settings", "last_overdue_digest_sent", today())
	except Exception:
		frappe.log_error(frappe.get_traceback(), "Weekly overdue digest email failed")


def _get_lender_emails() -> list[str]:
	"""Return email addresses of all enabled Lender Manager users."""
	try:
		override = frappe.db.get_single_value("VK Lending Settings", "lender_notification_email")
		if override:
			return [override]
	except Exception:
		pass

	result = frappe.db.sql(
		"""
		SELECT DISTINCT u.email
		FROM `tabUser` u
		JOIN `tabHas Role` hr ON hr.parent = u.name
		WHERE hr.role = 'Lender Manager'
		  AND u.enabled = 1
		  AND u.email IS NOT NULL
		  AND u.email != ''
		""",
		as_list=True,
	)
	return [r[0] for r in result if r[0]]
