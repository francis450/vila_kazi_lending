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

	Reads the attached PDF using vila_kazi_lending.mpesa_parser, writes the
	computed metrics back to the document, then triggers appraisal scoring for
	any Loan Appraisals already linked to this statement.
	"""
	try:
		doc = frappe.get_doc("M-Pesa Statement", doc_name)

		from vila_kazi_lending.mpesa_parser import parse

		results = parse(doc.statement_file, doc.period_from, doc.period_to)

		# Write all scalar metrics in one SQL round-trip
		frappe.db.set_value(
			"M-Pesa Statement",
			doc_name,
			{
				"parse_status": "Parsed",
				"parse_error_log": "",
				"monthly_avg_inflow": results["monthly_avg_inflow"],
				"monthly_avg_outflow": results["monthly_avg_outflow"],
				"avg_monthly_balance": results["avg_monthly_balance"],
				"salary_credit_regularity": results["salary_credit_regularity"],
				"loan_repayments_detected": results["loan_repayments_detected"],
				"net_cashflow_trend": results["net_cashflow_trend"],
				"gambling_transactions_detected": results["gambling_transactions_detected"],
				"gambling_total": results["gambling_total"],
				"parsed_transactions": results["parsed_transactions"],
			},
		)

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
		frappe.db.set_value(
			"M-Pesa Statement",
			doc_name,
			{"parse_status": "Failed", "parse_error_log": str(exc)},
		)
		frappe.log_error(frappe.get_traceback(), "parse_mpesa_statement failed")


# ---------------------------------------------------------------------------
# Background job: rule-based appraisal scoring
# ---------------------------------------------------------------------------

# Score model — maximum points per dimension (total = 100):
#   salary_regularity  0–25   % of months that had a salary/B2C credit
#   cashflow_trend     0–20   Improving / Stable / Declining
#   competing_loan     0–20   loan_repayments_detected / monthly_avg_inflow ratio
#   payday_behavior    0–15   avg_monthly_balance / monthly_avg_inflow ratio (buffer proxy)
#   gambling           0–10   gambling_total / monthly_avg_inflow ratio
#   request_ratio      0–10   requested_amount / max_eligible_amount
#
# Thresholds:
#   ≥ 65   → Approve
#   40–64  → Review
#   < 40   → Decline
#
# auto_approved = True when: score ≥ 75 AND within_limit AND no gambling AND competing ratio ≤ 0.30


def run_appraisal_scoring(appraisal_name: str) -> None:
	"""
	Rule-based scoring engine for a Loan Appraisal.

	Reads the linked M-Pesa Statement metrics, computes six sub-scores,
	derives a recommendation, and writes results via LoanAppraisal.set_ai_results().

	If no M-Pesa Statement is linked the appraisal scores only on eligibility
	(request_ratio sub-score) and flags the record for manual review.
	"""
	try:
		appraisal = frappe.get_doc("Loan Appraisal", appraisal_name)
	except frappe.DoesNotExistError:
		frappe.log_error(
			f"Loan Appraisal {appraisal_name} not found",
			"run_appraisal_scoring",
		)
		return

	# ── Pull M-Pesa Statement metrics ────────────────────────────────────
	stmt = None
	if appraisal.mpesa_statement:
		stmt = frappe.db.get_value(
			"M-Pesa Statement",
			appraisal.mpesa_statement,
			[
				"parse_status",
				"monthly_avg_inflow",
				"monthly_avg_outflow",
				"salary_credit_regularity",
				"net_cashflow_trend",
				"loan_repayments_detected",
				"avg_monthly_balance",
				"gambling_transactions_detected",
				"gambling_total",
			],
			as_dict=True,
		)
		# Only use a successfully parsed statement
		if stmt and stmt.parse_status != "Parsed":
			stmt = None

	# ── Sub-score helpers ─────────────────────────────────────────────────

	def _ratio(numerator: float, denominator: float) -> float:
		"""Safe division; returns 0 if denominator is 0."""
		return numerator / denominator if denominator else 0.0

	# 1. Salary regularity (0–25)
	# salary_credit_regularity is 0–100 (% of months with income credit)
	salary_reg = (stmt.salary_credit_regularity if stmt else 50.0) / 100.0
	salary_regularity_score = round(salary_reg * 25.0, 2)

	# 2. Cashflow trend (0–20)
	trend = (stmt.net_cashflow_trend if stmt else "Stable") or "Stable"
	cashflow_trend_score = {"Improving": 20.0, "Stable": 13.0, "Declining": 4.0}.get(trend, 13.0)

	# 3. Competing loan burden (0–20)
	competing_ratio = _ratio(
		stmt.loan_repayments_detected if stmt else 0.0,
		stmt.monthly_avg_inflow if stmt else 1.0,
	)
	if competing_ratio <= 0.10:
		competing_loan_score = 20.0
	elif competing_ratio <= 0.20:
		competing_loan_score = 15.0
	elif competing_ratio <= 0.30:
		competing_loan_score = 10.0
	elif competing_ratio <= 0.40:
		competing_loan_score = 5.0
	else:
		competing_loan_score = 0.0

	# 4. Payday behavior / buffer (0–15)
	# Proxy: avg_monthly_balance / monthly_avg_inflow — borrowers who keep
	# a healthy buffer relative to their inflow manage their cash well.
	buffer_ratio = _ratio(
		stmt.avg_monthly_balance if stmt else 0.0,
		stmt.monthly_avg_inflow if stmt else 1.0,
	)
	if buffer_ratio >= 0.20:
		payday_behavior_score = 15.0
	elif buffer_ratio >= 0.10:
		payday_behavior_score = 10.0
	elif buffer_ratio >= 0.05:
		payday_behavior_score = 5.0
	elif buffer_ratio >= 0.00:
		payday_behavior_score = 2.0
	else:
		payday_behavior_score = 0.0  # negative balance (overdraft)

	# 5. Gambling risk (0–10)
	gambling_ratio = _ratio(
		stmt.gambling_total if stmt else 0.0,
		stmt.monthly_avg_inflow if stmt else 1.0,
	)
	if not stmt or not stmt.gambling_transactions_detected:
		gambling_score = 10.0
	elif gambling_ratio <= 0.02:
		gambling_score = 7.0
	elif gambling_ratio <= 0.05:
		gambling_score = 4.0
	elif gambling_ratio <= 0.10:
		gambling_score = 1.0
	else:
		gambling_score = 0.0

	# 6. Request ratio (0–10)
	max_eligible = appraisal.max_eligible_amount or 0.0
	requested = appraisal.requested_amount or 0.0
	req_ratio = _ratio(requested, max_eligible) if max_eligible > 0 else 999.0
	if req_ratio <= 0.50:
		request_ratio_score = 10.0
	elif req_ratio <= 0.75:
		request_ratio_score = 7.0
	elif req_ratio <= 1.00:
		request_ratio_score = 4.0
	else:
		request_ratio_score = 0.0

	# ── Aggregate ─────────────────────────────────────────────────────────
	appraisal_score = round(
		salary_regularity_score
		+ cashflow_trend_score
		+ competing_loan_score
		+ payday_behavior_score
		+ gambling_score
		+ request_ratio_score,
		2,
	)

	# ── Recommendation ────────────────────────────────────────────────────
	if appraisal_score >= 65:
		recommendation = "Approve"
	elif appraisal_score >= 40:
		recommendation = "Review"
	else:
		recommendation = "Decline"

	# No statement → always require manual review regardless of score
	if not stmt:
		recommendation = "Review"

	# ── Auto-approval gate ────────────────────────────────────────────────
	# Fast-lanes borrower to "Pending Lender Confirm" without officer review.
	auto_approved = bool(
		recommendation == "Approve"
		and appraisal_score >= 75
		and appraisal.within_limit
		and (not stmt or not stmt.gambling_transactions_detected)
		and competing_ratio <= 0.30
	)

	# ── Risk flags and narrative ──────────────────────────────────────────
	risk_flags_parts: list[str] = []

	if not stmt:
		risk_flags_parts.append("No M-Pesa Statement linked — manual review required.")
	if stmt and stmt.gambling_transactions_detected:
		risk_flags_parts.append(
			f"Gambling detected: KES {stmt.gambling_total:,.2f} "
			f"({gambling_ratio * 100:.1f}% of monthly inflow)."
		)
	if competing_ratio > 0.30:
		risk_flags_parts.append(
			f"Competing loan burden: {competing_ratio * 100:.1f}% of monthly inflow."
		)
	if trend == "Declining":
		risk_flags_parts.append("Cashflow trend is Declining.")
	if not appraisal.within_limit:
		risk_flags_parts.append(
			f"Requested amount KES {requested:,.2f} exceeds"
			f" max eligible KES {max_eligible:,.2f}."
		)

	risk_flags = "\n".join(risk_flags_parts)

	# Build a readable narrative for the lender
	stmt_label = "No statement attached." if not stmt else (
		f"Monthly inflow KES {stmt.monthly_avg_inflow:,.2f} | "
		f"outflow KES {stmt.monthly_avg_outflow:,.2f} | "
		f"avg balance KES {stmt.avg_monthly_balance:,.2f}. "
		f"Cashflow trend: {trend}. "
		f"Competing loan repayments: {competing_ratio * 100:.1f}% of income. "
		f"Salary regularity: {(stmt.salary_credit_regularity or 0):.0f}%."
	)

	ai_summary = (
		f"Appraisal score: {appraisal_score:.0f}/100 ({recommendation}).\n\n"
		f"Sub-scores: Salary regularity {salary_regularity_score:.1f}/25 | "
		f"Cashflow trend {cashflow_trend_score:.1f}/20 | "
		f"Competing loans {competing_loan_score:.1f}/20 | "
		f"Payday buffer {payday_behavior_score:.1f}/15 | "
		f"Gambling {gambling_score:.1f}/10 | "
		f"Request ratio {request_ratio_score:.1f}/10.\n\n"
		f"{stmt_label}"
		+ (f"\n\nRisk flags:\n{risk_flags}" if risk_flags else "")
	)

	# ── Write results ─────────────────────────────────────────────────────
	appraisal.set_ai_results(
		appraisal_score=appraisal_score,
		sub_scores={
			"salary_regularity": salary_regularity_score,
			"cashflow_trend": cashflow_trend_score,
			"competing_loan": competing_loan_score,
			"payday_behavior": payday_behavior_score,
			"gambling": gambling_score,
			"request_ratio": request_ratio_score,
		},
		recommendation=recommendation,
		risk_flags=risk_flags,
		ai_summary=ai_summary,
		auto_approved=auto_approved,
	)


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
