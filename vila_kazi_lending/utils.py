"""
Vila Kazi Lending — shared utility functions
"""

# ---------------------------------------------------------------------------
# Settings singleton accessor
# ---------------------------------------------------------------------------


def get_settings():
	"""Return the VK Lending Settings singleton document."""
	import frappe

	return frappe.get_single("VK Lending Settings")

from __future__ import annotations

import calendar
from datetime import date, timedelta

import frappe
from frappe import _


# ---------------------------------------------------------------------------
# Payday resolution
# ---------------------------------------------------------------------------


def get_payday_date(bank: str, from_date: date | str) -> date | None:
	"""
	Return the next payday date for *bank* on or after *from_date*.

	Looks up the Payday Calendar for the bank, computes the canonical
	payday day for the same month as *from_date*, then applies the
	weekend_adjustment rule if it falls on a Saturday or Sunday.

	Returns None if no active Payday Calendar record exists for the bank.
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
		return None

	payday_day: int = record.payday_day
	adjustment: str = record.weekend_adjustment  # "Bring Forward" | "Push to Monday"

	# Try payday in the same month as from_date first; if already past, go next month
	for month_offset in (0, 1):
		year = from_date.year
		month = from_date.month + month_offset
		if month > 12:
			month -= 12
			year += 1

		# Clamp to last valid day of month (e.g. Feb 28/29 for day=31)
		last_day = calendar.monthrange(year, month)[1]
		day = min(payday_day, last_day)
		candidate = date(year, month, day)

		if candidate >= from_date:
			return _apply_weekend_adjustment(candidate, adjustment)

	return None


def _apply_weekend_adjustment(payday: date, rule: str) -> date:
	"""Adjust a payday that falls on a weekend per the bank's rule."""
	weekday = payday.weekday()  # 0=Mon … 5=Sat 6=Sun
	if weekday == 5:  # Saturday
		if rule == "Bring Forward":
			return payday - timedelta(days=1)  # Friday
		else:  # Push to Monday
			return payday + timedelta(days=2)
	if weekday == 6:  # Sunday
		if rule == "Bring Forward":
			return payday - timedelta(days=2)  # Friday
		else:  # Push to Monday
			return payday + timedelta(days=1)
	return payday  # Weekday — no adjustment needed


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


def compute_max_eligible(net_salary: float, existing_liabilities: float = 0.0) -> float:
	"""Return the maximum eligible loan amount: (net_salary × 0.50) − liabilities."""
	return max(0.0, (net_salary or 0.0) * 0.50 - (existing_liabilities or 0.0))


# ---------------------------------------------------------------------------
# Auto-approval gate
# ---------------------------------------------------------------------------


def check_auto_approval_gate(loan_application_name: str) -> bool:
	"""
	Evaluate all three auto-approval gate conditions (spec D2).

	Returns True only if ALL conditions pass.  Never auto-rejects —
	failures route to manual review.

	Condition 1: Borrower's active Framework Agreement status == 'Active'
	Condition 2: Requested amount ≤ max_eligible_amount
	Condition 3: Last 3 closed loans all have Repayment Reconciliation
	             status = 'Received' and received_date ≤ expected_date
	"""
	app = frappe.db.get_value(
		"Loan Application",
		loan_application_name,
		[
			"applicant",
			"loan_amount",
			"vk_max_eligible_amount",
			"vk_framework_agreement",
		],
		as_dict=True,
	)
	if not app:
		return False

	# --- Gate 1: Active Framework Agreement ---
	if not app.vk_framework_agreement:
		return False
	fa_status = frappe.db.get_value(
		"Loan Framework Agreement", app.vk_framework_agreement, "status"
	)
	if fa_status != "Active":
		return False

	# --- Gate 2: Within eligible limit ---
	requested = app.loan_amount or 0.0
	max_eligible = app.vk_max_eligible_amount or 0.0
	if requested > max_eligible:
		return False

	# --- Gate 3: Last 3 closed loans on time ---
	last_three = frappe.db.sql(
		"""
		SELECT rr.status, rr.received_date, rr.expected_date
		FROM `tabRepayment Reconciliation` rr
		JOIN `tabLoan` l ON l.name = rr.loan
		WHERE rr.borrower = %s
		  AND rr.status IN ('Received', 'Partial', 'Overdue')
		ORDER BY rr.expected_date DESC
		LIMIT 3
		""",
		app.applicant,
		as_dict=True,
	)

	if not last_three:
		# No repayment history — not eligible for auto-approval
		return False

	for rec in last_three:
		if rec.status != "Received":
			return False
		if rec.received_date and rec.expected_date and rec.received_date > rec.expected_date:
			return False

	return True


# ---------------------------------------------------------------------------
# WF-03: Refinancing amount calculation (Amendment 2)
# ---------------------------------------------------------------------------


def compute_refinancing_amounts(loan_application_name: str) -> dict:
	"""
	WF-03: Compute refinancing principal and net disbursement amounts.

	Responsibility of this function (Amendment 2 safe sequence):
	  - Compute outstanding_balance, new_loan_principal, net_disbursement
	  - Validate against eligibility ceiling (throws if exceeded)
	  - Update loan_amount and vk_loan_stage on the Loan Application
	  - Does NOT close the original loan — that happens post-disbursement
	    in events/loan_disbursement_source.py

	Returns a dict with: outstanding_balance, new_loan_principal, net_disbursement.
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
	net_disbursement = top_up  # Only the top-up is actual cash; outstanding is a book entry

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

	# Update the Loan Application — advance to Pending Disbursement
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
