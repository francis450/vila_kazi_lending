"""
Vila Kazi Lending — shared utility functions
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

import frappe
from frappe import _

__all__ = [
	"get_settings",
	"get_payday_date",
	"compute_max_eligible",
	"check_auto_approval_gate",
	"route_workflow",
	"compute_refinancing_amounts",
]


# ---------------------------------------------------------------------------
# Settings singleton accessor
# ---------------------------------------------------------------------------


def get_settings():
	"""Return the VK Lending Settings singleton document."""
	return frappe.get_single("VK Lending Settings")


# ---------------------------------------------------------------------------
# Payday resolution
# ---------------------------------------------------------------------------


def get_payday_date(bank: str, from_date: date | str) -> date:
	"""Return the NEXT payday date for *bank* strictly after *from_date*.

	If *from_date* falls exactly on the payday day, the FOLLOWING month's
	payday is returned (i.e. the function always looks forward, never
	returning the same day).

	Steps:
	  1. Look up Payday Calendar for the bank.  Raises ValidationError if
	     no record exists.
	  2. Walk month-by-month (current then next) to find the first candidate
	     date where day == payday_day and candidate > from_date.
	  3. Clamp payday_day to the last valid day of the month (e.g. day=31 in
	     February becomes the 28th/29th).
	  4. Apply the weekend_adjustment rule:
	     - Saturday + "Bring Forward"  → Friday (date − 1)
	     - Saturday + "Push to Monday" → Monday (date + 2)
	     - Sunday  + "Bring Forward"   → Friday (date − 2)
	     - Sunday  + "Push to Monday"  → Monday (date + 1)
	  5. If the adjustment crosses a month boundary (e.g. payday = 1st Sunday
	     brought forward to Saturday 31st of prior month) the crossed-back
	     date is returned as-is — the scheduler must NOT push it into the
	     next month's cycle.

	Args:
	    bank: Name of the Bank document (links to Payday Calendar.bank_name).
	    from_date: Start date as datetime.date or ISO string.

	Returns:
	    datetime.date – the resolved next payday.

	Raises:
	    frappe.ValidationError: If no Payday Calendar record exists for *bank*.
	"""
	if isinstance(from_date, str):
		from frappe.utils import getdate
		from_date = getdate(from_date)

	record = frappe.db.get_value(
		"Payday Calendar",
		{"bank_name": bank, "is_active": 1},
		["payday_day", "weekend_adjustment"],
		as_dict=True,
	)
	if not record:
		frappe.throw(
			_("No active Payday Calendar found for bank '{0}'.").format(bank),
			frappe.ValidationError,
		)

	payday_day: int = record.payday_day
	adjustment: str = record.weekend_adjustment  # "Bring Forward" | "Push to Monday"

	# Search the current month and up to two more to find a candidate
	# strictly *after* from_date.  Two months is enough because the worst
	# case is from_date == payday in the current month (skip → next month).
	for month_offset in (0, 1, 2):
		year = from_date.year
		month = from_date.month + month_offset
		if month > 12:
			month -= 12
			year += 1

		# Clamp to last valid calendar day of this month (handles day=31 in Feb, etc.)
		last_day = calendar.monthrange(year, month)[1]
		day = min(payday_day, last_day)
		candidate = date(year, month, day)

		# Strictly greater — if from_date IS the payday, skip to next month
		if candidate > from_date:
			return _apply_weekend_adjustment(candidate, adjustment)

	# Should never reach here for reasonable payday_day values
	frappe.throw(_("Could not resolve payday date for bank '{0}'.").format(bank))


def _apply_weekend_adjustment(payday: date, rule: str) -> date:
	"""Shift *payday* if it falls on a weekend per the bank's adjustment rule.

	The returned date may cross a month boundary (e.g. payday=1st Sunday
	brought forward to Saturday 31st of the prior month).  Callers must
	accept cross-month results — do NOT re-resolve into the next cycle.

	Args:
	    payday: The raw calendar payday (may be a weekend).
	    rule: "Bring Forward" or "Push to Monday".

	Returns:
	    datetime.date – adjusted date (unchanged if payday was a weekday).
	"""
	weekday = payday.weekday()  # 0=Mon … 4=Fri, 5=Sat, 6=Sun

	if weekday == 5:  # Saturday
		if rule == "Bring Forward":
			return payday - timedelta(days=1)   # → Friday
		else:  # "Push to Monday"
			return payday + timedelta(days=2)   # → Monday

	if weekday == 6:  # Sunday
		if rule == "Bring Forward":
			return payday - timedelta(days=2)   # → Friday
		else:  # "Push to Monday"
			return payday + timedelta(days=1)   # → Monday

	return payday  # Weekday — no adjustment needed


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


def compute_max_eligible(net_salary: float, existing_liabilities: float = 0.0) -> float:
	"""Return the maximum loan amount a borrower is eligible for.

	Formula:
	    max_eligible = (net_salary × ratio) − existing_liabilities

	The ratio is read from VK Lending Settings.max_loan_to_salary_ratio
	(default 0.5 if the settings document does not have the field set).

	The result is floored at 0 — it will never be negative.

	Args:
	    net_salary: Borrower's verified net monthly salary (KES).
	    existing_liabilities: Total existing monthly debt obligations (KES).
	                          Defaults to 0.

	Returns:
	    float – maximum eligible loan amount, >= 0.
	"""
	# Read configurable ratio from settings; fall back to 0.5 if unset
	ratio = (
		frappe.db.get_single_value("VK Lending Settings", "max_loan_to_salary_ratio")
		or 0.5
	)
	result = (net_salary or 0.0) * ratio - (existing_liabilities or 0.0)
	return max(0.0, result)


# ---------------------------------------------------------------------------
# Auto-approval gate (fast-lane eligibility)
# ---------------------------------------------------------------------------


def check_auto_approval_gate(loan_application_name: str) -> dict:
	"""Evaluate fast-lane eligibility for a Loan Application.

	All FOUR conditions must pass for fast-lane approval.  The function
	never raises; failures are accumulated in `failed_conditions`.

	Condition 1 — Active Framework Agreement:
	    Loan Application → applicant (Customer) → Borrower Profile →
	    framework_agreement → Loan Framework Agreement.status == "Active".
	    Fail: "No active Framework Agreement on file."

	Condition 2 — Within eligible amount:
	    loan_application.loan_amount <=
	    compute_max_eligible(borrower_profile.net_salary,
	                         borrower_profile.existing_liabilities [default 0]).
	    Fail: "Requested amount KES {x} exceeds eligible limit KES {y}."

	Condition 3 — Repayment history (last 3 closed loans):
	    For each Loan (status in ["Repaid", "Closed"]) belonging to the
	    borrower, the linked Repayment Reconciliation must have
	    received_date <= expected_date.  At least 1 closed loan is required.
	    Fail: "Repayment history check failed: {n} of last {total} loans were late."

	Condition 4 — Watch category block:
	    If Borrower Profile.credit_category == "Watch", always fail.
	    Fail: "Borrower is on Watch category — manual review required."

	Args:
	    loan_application_name: The `name` of the Loan Application document.

	Returns:
	    dict with keys:
	        "passed"            – bool, True only if ALL conditions pass.
	        "failed_conditions" – list[str], empty when passed=True.
	"""
	failed: list[str] = []

	# ── Load Loan Application ──────────────────────────────────────────────
	app = frappe.db.get_value(
		"Loan Application",
		loan_application_name,
		["applicant", "loan_amount"],
		as_dict=True,
	)
	if not app:
		return {
			"passed": False,
			"failed_conditions": [f"Loan Application '{loan_application_name}' not found."],
		}

	# ── Load Borrower Profile (name == customer field value) ───────────────
	bp = frappe.db.get_value(
		"Borrower Profile",
		app.applicant,
		["framework_agreement", "net_salary", "credit_category"],
		as_dict=True,
	)
	if not bp:
		return {
			"passed": False,
			"failed_conditions": [
				f"No Borrower Profile found for customer '{app.applicant}'."
			],
		}

	# ── Condition 4: Watch category block (checked first — hard stop) ──────
	if bp.credit_category == "Watch":
		failed.append("Borrower is on Watch category — manual review required.")

	# ── Condition 1: Active Framework Agreement ────────────────────────────
	fa_active = False
	if bp.framework_agreement:
		fa_status = frappe.db.get_value(
			"Loan Framework Agreement", bp.framework_agreement, "status"
		)
		fa_active = fa_status == "Active"

	if not fa_active:
		failed.append("No active Framework Agreement on file.")

	# ── Condition 2: Requested amount within eligible limit ────────────────
	# Borrower Profile has no existing_liabilities field yet; default to 0
	existing_liabilities = getattr(bp, "existing_liabilities", None) or 0.0
	max_eligible = compute_max_eligible(bp.net_salary or 0.0, existing_liabilities)
	requested = app.loan_amount or 0.0

	if requested > max_eligible:
		failed.append(
			"Requested amount KES {0} exceeds eligible limit KES {1}.".format(
				frappe.utils.fmt_money(requested, currency="KES"),
				frappe.utils.fmt_money(max_eligible, currency="KES"),
			)
		)

	# ── Condition 3: Repayment history on last 3 closed loans ─────────────
	closed_loans = frappe.db.get_all(
		"Loan",
		filters={
			"applicant": app.applicant,
			"status": ["in", ["Repaid", "Closed", "Written Off"]],
		},
		fields=["name"],
		order_by="creation desc",
		limit=3,
	)

	if not closed_loans:
		# No closed loan history — not eligible for fast lane
		failed.append(
			"Repayment history check failed: no closed loans on record."
		)
	else:
		late_count = 0
		total = len(closed_loans)

		for loan_rec in closed_loans:
			rr = frappe.db.get_value(
				"Repayment Reconciliation",
				{"loan": loan_rec.name},
				["received_date", "expected_date"],
				as_dict=True,
			)
			# Missing RR or missing dates are treated as late
			if not rr or not rr.received_date or not rr.expected_date:
				late_count += 1
				continue
			if rr.received_date > rr.expected_date:
				late_count += 1

		if late_count > 0:
			failed.append(
				"Repayment history check failed: {0} of last {1} loans were late.".format(
					late_count, total
				)
			)

	return {"passed": len(failed) == 0, "failed_conditions": failed}


# ---------------------------------------------------------------------------
# Workflow routing helper
# ---------------------------------------------------------------------------


def route_workflow(doc) -> None:
	"""Set the correct initial vk_loan_stage on a Loan Application before submit.

	Called from the `before_submit` hook on Loan Application.  Sets the
	`vk_is_repeat_borrower` flag and determines which workflow entry point
	the application should start at:

	  - Refinancing:     vk_is_refinancing == 1 → stage "Refinancing Requested"
	  - Repeat borrower: verified Borrower Profile exists for applicant
	                     → vk_is_repeat_borrower = 1, stage "Intake"
	  - New borrower:    no Borrower Profile → vk_is_repeat_borrower = 0,
	                     stage "Draft"

	The function mutates *doc* in-place; it does not call doc.save().

	Args:
	    doc: The Loan Application document object (frappe.Document).
	"""
	# --- Refinancing takes precedence over everything else ---
	if doc.get("vk_is_refinancing"):
		doc.vk_loan_stage = "Refinancing Requested"
		return

	# --- Check for an existing verified Borrower Profile ---
	bp_exists = frappe.db.get_value(
		"Borrower Profile",
		{"customer": doc.applicant, "kyc_status": "Verified"},
		"name",
	)

	if bp_exists:
		# Returning borrower with a verified KYC profile — fast track to Intake
		doc.vk_is_repeat_borrower = 1
		doc.vk_loan_stage = "Intake"
	else:
		# First-time or unverified borrower — start at Draft for KYC processing
		doc.vk_is_repeat_borrower = 0
		doc.vk_loan_stage = "Draft"


# ---------------------------------------------------------------------------
# WF-03: Refinancing amount calculation (Amendment 2)
# ---------------------------------------------------------------------------


def compute_refinancing_amounts(loan_application_name: str) -> dict:
	"""Compute refinancing principal and net disbursement amounts.

	WF-03 (Amendment 2 safe sequence):
	  - Derives outstanding_balance, new_loan_principal, and net_disbursement.
	  - Validates the new principal against the eligibility ceiling; throws
	    frappe.ValidationError if exceeded.
	  - Writes loan_amount and vk_loan_stage = "Pending Disbursement" onto
	    the Loan Application via db.set_value.
	  - Does NOT close the original loan — that is handled post-disbursement
	    in events/loan_disbursement_source.py.

	Args:
	    loan_application_name: Name of the Loan Application document.

	Returns:
	    dict with keys: outstanding_balance, new_loan_principal, net_disbursement.

	Raises:
	    frappe.ValidationError: If the loan application or its linked data
	                            cannot be found, or if the new principal
	                            exceeds the eligible ceiling.
	"""
	app = frappe.db.get_value(
		"Loan Application",
		loan_application_name,
		[
			"name",
			"vk_refinancing_of_loan",
			"vk_top_up_amount",
			"vk_max_eligible_amount",
			"vk_net_salary",
			"vk_existing_liabilities",
		],
		as_dict=True,
	)
	if not app:
		frappe.throw(_("Loan Application {0} not found.").format(loan_application_name))

	if not app.vk_refinancing_of_loan:
		frappe.throw(
			_("No original loan linked for refinancing. Set vk_refinancing_of_loan before approving.")
		)

	# Fetch outstanding balance from the original Repayment Reconciliation
	rr = frappe.db.get_value(
		"Repayment Reconciliation",
		{"loan": app.vk_refinancing_of_loan},
		["expected_amount", "received_amount"],
		as_dict=True,
	)
	if not rr:
		frappe.throw(
			_("No Repayment Reconciliation found for loan {0}.").format(app.vk_refinancing_of_loan)
		)

	outstanding_balance = (rr.expected_amount or 0) - (rr.received_amount or 0)
	top_up = app.vk_top_up_amount or 0
	new_loan_principal = outstanding_balance + top_up
	net_disbursement = top_up  # Only the top-up is disbursed as cash

	# Validate against eligibility ceiling
	max_eligible = app.vk_max_eligible_amount or compute_max_eligible(
		app.vk_net_salary or 0, app.vk_existing_liabilities or 0
	)
	if new_loan_principal > max_eligible:
		frappe.throw(
			_(
				"Refinanced principal KES {0} exceeds the maximum eligible amount KES {1}. "
				"Lender must override with a documented reason before proceeding."
			).format(
				frappe.utils.fmt_money(new_loan_principal),
				frappe.utils.fmt_money(max_eligible),
			)
		)

	# Advance the application stage
	frappe.db.set_value(
		"Loan Application",
		loan_application_name,
		{"loan_amount": new_loan_principal, "vk_loan_stage": "Pending Disbursement"},
	)

	frappe.logger("vila_kazi_lending").info(
		f"compute_refinancing_amounts [{loan_application_name}]: "
		f"outstanding={outstanding_balance}, top_up={top_up}, "
		f"new_principal={new_loan_principal}, net_disbursement={net_disbursement}"
	)

	return {
		"outstanding_balance": outstanding_balance,
		"new_loan_principal": new_loan_principal,
		"net_disbursement": net_disbursement,
	}
