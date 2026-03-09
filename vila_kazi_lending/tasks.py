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

		if not doc.statement_file:
			raise ValueError("No file attached to this M-Pesa Statement.")

		from vila_kazi_lending.mpesa_parser import parse

		results = parse(doc.statement_file, doc.period_from, doc.period_to)

		# Guard: reject statements with too few transactions to be meaningful
		import json as _json
		n_tx = len(_json.loads(results.get("parsed_transactions", "[]")))
		if n_tx < 10:
			frappe.db.set_value(
				"M-Pesa Statement",
				doc_name,
				{
					"parse_status": "Failed",
					"parse_error_log": f"Insufficient data: only {n_tx} transactions extracted",
				},
			)
			return

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
# Background job: rule-based appraisal scoring  (Layer 5)
# ---------------------------------------------------------------------------

# Score model — six sub-scores each 0–100, weighted to sum to 100 points:
#   salary_regularity   weight 25   % of months with salary/B2C credit
#   cashflow_trend      weight 20   Improving / Stable / Declining
#   competing_loan      weight 20   loan_repayments ÷ monthly_avg_inflow
#   payday_behavior     weight 15   payday crash rate (0 stored → defaults 0)
#   gambling_activity   weight 10   gambling_total ÷ monthly_avg_inflow
#   request_ratio       weight 10   requested_amount ÷ max_eligible_amount
#
# Hard rules (evaluated first — any trigger → Decline, skip scoring):
#   HR-1: requested_amount > compute_max_eligible()
#   HR-2: gambling_total > monthly_avg_inflow × 0.10
#   HR-3: Borrower credit_category == "Watch"
#
# Thresholds (from VK Lending Settings, defaults):
#   ≥ 70 → Approve | ≥ 50 → Review | < 50 → Decline
#
# Soft rule: competing_loan burden_ratio > 0.30 → escalate Approve → Review


def run_appraisal_scoring(appraisal_name: str) -> None:
	"""
	Rule-based scoring engine for a Loan Appraisal.

	Evaluates three hard rules first (any trigger → immediate Decline).
	Aborts to Review if the M-Pesa Statement is missing or unparsed.
	Otherwise computes six weighted sub-scores, derives a recommendation,
	and writes all results via LoanAppraisal.set_ai_results().

	The on_update event in events/loan_appraisal.py advances the linked
	Loan Application's vk_loan_stage based on the saved recommendation.
	"""
	from vila_kazi_lending.utils import compute_max_eligible

	# ── Load Loan Appraisal ───────────────────────────────────────────────
	try:
		appraisal = frappe.get_doc("Loan Appraisal", appraisal_name)
	except frappe.DoesNotExistError:
		frappe.log_error(
			f"Loan Appraisal {appraisal_name} not found",
			"run_appraisal_scoring",
		)
		return

	# ── Load Loan Application ─────────────────────────────────────────────
	la = None
	if appraisal.loan_application:
		la = frappe.db.get_value(
			"Loan Application",
			appraisal.loan_application,
			["applicant", "vk_net_salary", "vk_existing_liabilities",
			 "vk_is_refinancing", "vk_borrower_bank"],
			as_dict=True,
		)

	# ── Load Borrower Profile ─────────────────────────────────────────────
	bp = None
	if la and la.applicant:
		bp = frappe.db.get_value(
			"Borrower Profile",
			{"customer": la.applicant},
			["credit_category", "on_time_repayment_rate"],
			as_dict=True,
		)

	# ── Load M-Pesa Statement ─────────────────────────────────────────────
	stmt = None
	if appraisal.mpesa_statement:
		stmt = frappe.db.get_value(
			"M-Pesa Statement",
			appraisal.mpesa_statement,
			[
				"parse_status",
				"period_from",
				"period_to",
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
		if stmt and stmt.parse_status != "Parsed":
			stmt = None

	# ── Abort: no usable statement ────────────────────────────────────────
	if stmt is None:
		appraisal.set_ai_results(
			appraisal_score=0.0,
			sub_scores={k: 0.0 for k in (
				"salary_regularity", "cashflow_trend", "competing_loan",
				"payday_behavior", "gambling", "request_ratio",
			)},
			recommendation="Review",
			risk_flags="",
			ai_summary="M-Pesa statement not yet parsed",
			auto_approved=False,
		)
		return

	# ── Load VK Lending Settings ──────────────────────────────────────────
	settings = frappe.get_single("VK Lending Settings")

	# Default weights (VK Lending Settings has no per-weight fields → always defaults)
	_DEFAULT_WEIGHTS = {
		"salary_regularity": 25,
		"cashflow_trend":    20,
		"competing_loan":    20,
		"payday_behavior":   15,
		"gambling":          10,
		"request_ratio":     10,
	}
	weights = {
		k: (getattr(settings, f"{k}_weight", None) or v)
		for k, v in _DEFAULT_WEIGHTS.items()
	}
	if sum(weights.values()) != 100:
		frappe.throw(
			f"Appraisal scoring weights must sum to 100 "
			f"(current: {sum(weights.values())}). "
			"Check VK Lending Settings.",
			frappe.ValidationError,
		)

	# ── Shared derived values ─────────────────────────────────────────────
	requested  = appraisal.requested_amount or 0.0
	net_salary = appraisal.net_salary or 0.0
	existing_liabilities = appraisal.existing_liabilities or 0.0
	max_eligible = compute_max_eligible(net_salary, existing_liabilities)
	inflow = stmt.monthly_avg_inflow or 1.0  # avoid div-by-zero

	def _safe_div(n: float, d: float) -> float:
		return n / d if d else 0.0

	# ── Hard Rules ────────────────────────────────────────────────────────
	hard_flags: list[str] = []

	# HR-1: over-limit request
	if requested > max_eligible:
		hard_flags.append(
			f"Requested amount exceeds eligible limit "
			f"(KES {requested:,.0f} vs KES {max_eligible:,.0f})"
		)

	# HR-2: excessive gambling
	gambling_total = stmt.gambling_total or 0.0
	gambling_threshold = inflow * 0.10
	if gambling_total > gambling_threshold:
		hard_flags.append(
			f"Gambling spend exceeds 10% of avg monthly inflow "
			f"(KES {gambling_total:,.0f} vs threshold KES {gambling_threshold:,.0f})"
		)

	# HR-3: Watch category
	if bp and (bp.credit_category or "") == "Watch":
		hard_flags.append("Borrower is on Watch category — auto-approval blocked")

	if hard_flags:
		zero_sub = {k: 0.0 for k in _DEFAULT_WEIGHTS}
		appraisal.set_ai_results(
			appraisal_score=0.0,
			sub_scores=zero_sub,
			recommendation="Decline",
			risk_flags=", ".join(hard_flags),
			ai_summary=(
				f"Appraisal declined by hard rule(s).\n\n"
				+ "\n".join(f"• {f}" for f in hard_flags)
			),
			auto_approved=False,
		)
		return

	# ── Sub-scores (each 0–100 raw) ───────────────────────────────────────
	risk_flags: list[str] = []
	soft_review = False

	# 1. Salary regularity
	sal_reg_pct = stmt.salary_credit_regularity or 0.0
	sal_reg_frac = sal_reg_pct / 100.0          # normalise 0-100 → 0-1
	salary_raw = sal_reg_frac * 100.0
	if salary_raw < 60.0:
		risk_flags.append(
			f"Irregular salary credits ({sal_reg_frac:.0%} of months)"
		)

	# 2. Cashflow trend
	trend = (stmt.net_cashflow_trend or "Stable")
	trend_raw = {"Improving": 100.0, "Stable": 60.0, "Declining": 20.0}.get(trend, 60.0)
	if trend == "Declining":
		risk_flags.append("Declining net cashflow trend")

	# 3. Competing loan burden
	burden_ratio = _safe_div(stmt.loan_repayments_detected or 0.0, inflow)
	if burden_ratio <= 0.10:
		competing_raw = 100.0
	elif burden_ratio <= 0.20:
		competing_raw = 80.0
	elif burden_ratio <= 0.30:
		competing_raw = 60.0
	else:
		competing_raw = 30.0
		soft_review = True    # soft rule: escalate Approve → Review
	if burden_ratio > 0.20:
		risk_flags.append(
			f"Competing loan repayments are {burden_ratio:.0%} of monthly income"
		)

	# 4. Payday behavior
	# payday_crash_months has no doctype field — defaults to 0 (no crashes observed)
	total_months = _months_in_statement(
		str(stmt.period_from or ""), str(stmt.period_to or "")
	)
	payday_crash_months = 0
	crash_rate = _safe_div(payday_crash_months, total_months)
	if crash_rate == 0.0:
		payday_raw = 100.0
	elif crash_rate <= 0.17:
		payday_raw = 80.0
	elif crash_rate <= 0.33:
		payday_raw = 60.0
	else:
		payday_raw = 30.0
	if crash_rate > 0.17:
		risk_flags.append(
			f"Balance crashes near payday in {payday_crash_months} "
			f"of {total_months} months"
		)

	# 5. Gambling activity
	gambling_ratio = _safe_div(gambling_total, inflow)
	if gambling_ratio == 0.0:
		gambling_raw = 100.0
	elif gambling_ratio <= 0.03:
		gambling_raw = 80.0
	elif gambling_ratio <= 0.07:
		gambling_raw = 50.0
	else:
		gambling_raw = 20.0
	if gambling_total > 0.0:
		risk_flags.append(
			f"Gambling transactions detected (KES {gambling_total:,.0f}/month avg)"
		)

	# 6. Request ratio
	req_ratio = _safe_div(requested, max_eligible) if max_eligible > 0 else 999.0
	if req_ratio <= 0.50:
		req_raw = 100.0
	elif req_ratio <= 0.75:
		req_raw = 80.0
	elif req_ratio <= 0.90:
		req_raw = 60.0
	else:
		req_raw = 40.0

	# ── Weighted sub-score contributions (stored in doctype, range matches labels) ──
	salary_regularity_score  = round(salary_raw  * weights["salary_regularity"]  / 100, 2)
	cashflow_trend_score     = round(trend_raw   * weights["cashflow_trend"]     / 100, 2)
	competing_loan_score     = round(competing_raw * weights["competing_loan"]   / 100, 2)
	payday_behavior_score    = round(payday_raw  * weights["payday_behavior"]    / 100, 2)
	gambling_score           = round(gambling_raw * weights["gambling"]          / 100, 2)
	request_ratio_score      = round(req_raw     * weights["request_ratio"]      / 100, 2)

	appraisal_score = round(
		salary_regularity_score + cashflow_trend_score + competing_loan_score
		+ payday_behavior_score + gambling_score + request_ratio_score,
		1,
	)

	# ── Recommendation ────────────────────────────────────────────────────
	approve_threshold = settings.score_approve_threshold or 70.0
	review_threshold  = settings.score_review_threshold  or 50.0

	if appraisal_score >= approve_threshold:
		recommendation = "Approve"
	elif appraisal_score >= review_threshold:
		recommendation = "Review"
	else:
		recommendation = "Decline"

	# Soft rule: competing loan burden > 30% escalates Approve → Review
	if soft_review and recommendation == "Approve":
		recommendation = "Review"

	# ── Auto-approval gate ────────────────────────────────────────────────
	auto_approved = bool(
		recommendation == "Approve"
		and appraisal_score >= 75.0
		and appraisal.within_limit
		and gambling_total == 0.0
		and burden_ratio <= 0.30
	)

	# ── Build narrative ───────────────────────────────────────────────────
	ai_summary = (
		f"Appraisal score: {appraisal_score:.1f}/100 → {recommendation}.\n\n"
		f"Sub-scores (weighted): "
		f"Salary regularity {salary_regularity_score:.1f}/{weights['salary_regularity']} | "
		f"Cashflow trend {cashflow_trend_score:.1f}/{weights['cashflow_trend']} | "
		f"Competing loans {competing_loan_score:.1f}/{weights['competing_loan']} | "
		f"Payday behavior {payday_behavior_score:.1f}/{weights['payday_behavior']} | "
		f"Gambling {gambling_score:.1f}/{weights['gambling']} | "
		f"Request ratio {request_ratio_score:.1f}/{weights['request_ratio']}.\n\n"
		f"Monthly inflow KES {stmt.monthly_avg_inflow:,.0f} | "
		f"outflow KES {stmt.monthly_avg_outflow:,.0f} | "
		f"avg balance KES {stmt.avg_monthly_balance:,.0f}. "
		f"Cashflow trend: {trend}. "
		f"Competing burden: {burden_ratio:.0%} of income. "
		f"Salary regularity: {sal_reg_pct:.0f}%."
		+ (f"\n\nRisk flags: {', '.join(risk_flags)}" if risk_flags else "")
	)

	# ── Write results ─────────────────────────────────────────────────────
	appraisal.set_ai_results(
		appraisal_score=appraisal_score,
		sub_scores={
			"salary_regularity": salary_regularity_score,
			"cashflow_trend":    cashflow_trend_score,
			"competing_loan":    competing_loan_score,
			"payday_behavior":   payday_behavior_score,
			"gambling":          gambling_score,
			"request_ratio":     request_ratio_score,
		},
		recommendation=recommendation,
		risk_flags=", ".join(risk_flags),
		ai_summary=ai_summary,
		auto_approved=auto_approved,
	)


def _months_in_statement(period_from: str, period_to: str) -> int:
	"""Return the number of calendar months spanned by the statement period.

	Falls back to 3 if either date string is missing or unparseable.
	"""
	try:
		if not period_from or not period_to:
			return 3
		from frappe.utils import getdate
		d1 = getdate(period_from)
		d2 = getdate(period_to)
		return max(1, (d2.year - d1.year) * 12 + d2.month - d1.month + 1)
	except Exception:
		return 3


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
