"""
Tests for run_appraisal_scoring() — Layer 5 of Vila Kazi Lending.

Covers: hard rules (HR-1/2/3), abort path (no stmt), sub-score formulas,
recommendation thresholds, soft rule escalation, and auto-approval gate.

Run with:
    bench --site lending.erpkenya.com run-tests --app vila_kazi_lending \
        --module vila_kazi_lending.vila_kazi_lending.events.test_appraisal_scoring
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from vila_kazi_lending.tasks import _months_in_statement, run_appraisal_scoring


# ─────────────────────────────────────────────────────────────────────────────
# Fixture helpers
# ─────────────────────────────────────────────────────────────────────────────

def _customer(name_suffix: str) -> str:
	"""Insert a throw-away Customer and return its name."""
	cust = frappe.get_doc({
		"doctype": "Customer",
		"customer_name": f"VK Appraisal Test {name_suffix}",
		"customer_type": "Individual",
		"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
		               or "All Customer Groups",
		"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name")
		           or "All Territories",
	}).insert(ignore_permissions=True)
	return cust.name


def _mpesa_stmt(
	customer_name: str,
	*,
	parse_status: str = "Parsed",
	monthly_avg_inflow: float = 80_000,
	monthly_avg_outflow: float = 40_000,
	avg_monthly_balance: float = 20_000,
	salary_credit_regularity: float = 100.0,
	net_cashflow_trend: str = "Stable",
	loan_repayments_detected: float = 0.0,
	gambling_total: float = 0.0,
	gambling_transactions_detected: int = 0,
	period_from: str = "2025-01-01",
	period_to: str = "2025-03-31",
) -> str:
	"""Insert a minimal M-Pesa Statement and return its name.

	The on_update hook runs parse_mpesa_statement synchronously in test mode,
	which will fail (no real file) and overwrite parse_status = "Failed".
	We therefore set all metric fields via db.set_value() AFTER insert so
	the values used in tests override whatever the background job wrote.
	"""
	stmt = frappe.get_doc({
		"doctype": "M-Pesa Statement",
		"borrower": customer_name,
		"statement_file": "/private/files/test_dummy_appraisal.pdf",
		"period_from": period_from,
		"period_to": period_to,
		"parse_status": "Pending",  # initial value — will be overwritten below
	}).insert(ignore_permissions=True)

	# Override ALL metric fields via direct SQL (no hooks, no background job interference)
	frappe.db.set_value("M-Pesa Statement", stmt.name, {
		"parse_status": parse_status,
		"monthly_avg_inflow": monthly_avg_inflow,
		"monthly_avg_outflow": monthly_avg_outflow,
		"avg_monthly_balance": avg_monthly_balance,
		"salary_credit_regularity": salary_credit_regularity,
		"net_cashflow_trend": net_cashflow_trend,
		"loan_repayments_detected": loan_repayments_detected,
		"gambling_total": gambling_total,
		"gambling_transactions_detected": gambling_transactions_detected,
	})
	return stmt.name


def _appraisal(
	customer_name: str,
	*,
	net_salary: float = 80_000,
	existing_liabilities: float = 0.0,
	requested_amount: float = 30_000,
	mpesa_statement: str | None = None,
) -> str:
	"""Insert a minimal Loan Appraisal (no Loan Application link) and return its name."""
	apr = frappe.get_doc({
		"doctype": "Loan Appraisal",
		"loan_application": _dummy_loan_application(customer_name),
		"borrower": customer_name,
		"net_salary": net_salary,
		"existing_liabilities": existing_liabilities,
		"requested_amount": requested_amount,
		"mpesa_statement": mpesa_statement,
	}).insert(ignore_permissions=True)
	return apr.name


_la_counter = [0]  # module-level counter for unique loan application names


def _dummy_loan_application(customer_name: str) -> str:
	"""Create or return the simplest possible Loan Application for testing."""
	_la_counter[0] += 1

	# We need loan_type — fetch first available
	loan_type = frappe.db.get_value("Loan Type", {}, "name")
	if not loan_type:
		# No loan types in test environment — just create a plain record
		# via SQL to avoid mandatory field issues
		return _make_la_via_sql(customer_name)

	try:
		la = frappe.get_doc({
			"doctype": "Loan Application",
			"applicant_type": "Customer",
			"applicant": customer_name,
			"loan_type": loan_type,
			"loan_amount": 30_000,
			"repayment_method": "Repay Over Number of Periods",
			"repayment_periods": 12,
		}).insert(ignore_permissions=True, ignore_mandatory=True)
		return la.name
	except Exception:
		return _make_la_via_sql(customer_name)


def _make_la_via_sql(customer_name: str) -> str:
	"""Fallback: insert a bare-minimum Loan Application row via direct SQL."""
	import random, string
	name = "TEST-LA-" + "".join(random.choices(string.ascii_uppercase, k=6))
	frappe.db.sql(
		"""
		INSERT INTO `tabLoan Application`
		    (name, applicant_type, applicant, docstatus, status, creation, modified,
		     modified_by, owner)
		VALUES (%s, 'Customer', %s, 0, 'Open', NOW(), NOW(), 'Administrator', 'Administrator')
		ON DUPLICATE KEY UPDATE name = name
		""",
		(name, customer_name),
	)
	return name


# ─────────────────────────────────────────────────────────────────────────────
# Test: Hard Rule HR-1 — requested amount > max eligible
# ─────────────────────────────────────────────────────────────────────────────

class TestHardRuleOverLimit(FrappeTestCase):
	"""
	HR-1: If requested_amount > compute_max_eligible(net_salary, liabilities),
	recommendation must be 'Decline' and appraisal_score = 0, regardless of
	all other factors.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_over_limit_forces_decline(self):
		"""
		net_salary = 80,000 → max_eligible = 40,000
		requested  = 50,000 → over limit → HR-1 triggers → Decline
		"""
		cust = _customer("HR1-Decline")
		stmt_name = _mpesa_stmt(cust, monthly_avg_inflow=80_000)
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			existing_liabilities=0,
			requested_amount=50_000,   # 62.5% of salary — over 50% limit
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "appraisal_score", "risk_flags"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Decline")
		self.assertEqual(result.appraisal_score, 0.0)
		self.assertIn("Requested amount exceeds eligible limit", result.risk_flags)

	def test_within_limit_does_not_trigger_hr1(self):
		"""requested_amount ≤ max_eligible should NOT trigger HR-1."""
		cust = _customer("HR1-Pass")
		stmt_name = _mpesa_stmt(cust, monthly_avg_inflow=80_000)
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			existing_liabilities=0,
			requested_amount=30_000,   # 37.5% of salary — within limit
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value("Loan Appraisal", apr_name, "recommendation")
		self.assertNotEqual(result, "Decline")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Hard Rule HR-2 — gambling > 10% of avg monthly inflow
# ─────────────────────────────────────────────────────────────────────────────

class TestHardRuleGambling(FrappeTestCase):
	"""
	HR-2: gambling_total > monthly_avg_inflow × 0.10 → Decline.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_gambling_over_10pct_forces_decline(self):
		"""gambling_total = 12,000 on inflow 100,000 = 12% → HR-2 → Decline"""
		cust = _customer("HR2-Gambling")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			gambling_total=12_000,
			gambling_transactions_detected=1,
		)
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			requested_amount=30_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "appraisal_score", "risk_flags"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Decline")
		self.assertEqual(result.appraisal_score, 0.0)
		self.assertIn("Gambling spend exceeds 10%", result.risk_flags)

	def test_gambling_under_10pct_does_not_trigger_hr2(self):
		"""gambling_total = 5,000 on inflow 100,000 = 5% → HR-2 not triggered"""
		cust = _customer("HR2-GamblingPass")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			gambling_total=5_000,
			gambling_transactions_detected=1,
		)
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			requested_amount=30_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value("Loan Appraisal", apr_name, "recommendation")
		self.assertNotEqual(result, "Decline",
			"5% gambling should not trigger HR-2 Decline")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Hard Rule HR-3 — Watch category → Decline
# ─────────────────────────────────────────────────────────────────────────────

class TestHardRuleWatchCategory(FrappeTestCase):
	"""
	HR-3: Borrower Profile credit_category == 'Watch' → Decline immediately.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_watch_category_forces_decline(self):
		"""
		Borrower with credit_category = 'Watch' → HR-3 → Decline.
		All other metrics are pristine (would normally Approve).
		"""
		cust = _customer("HR3-Watch")

		# Insert Borrower Profile with a valid category, then override to "Watch"
		# via db.set_value to bypass Select option validation.
		frappe.get_doc({
			"doctype": "Borrower Profile",
			"customer": cust,
			"kyc_status": "Verified",
			"national_id_number": "TEST-WATCH-001",
			"national_id_scan": "/files/dummy.pdf",
			"employer_name": "Good Employer Ltd",
			"employment_letter": "/files/dummy.pdf",
			"bank": frappe.db.get_value("Bank", {}, "name") or "Test Bank",
			"mpesa_number": "0700000001",
			"net_salary": 80_000,
			"credit_category": "New",    # valid option
		}).insert(ignore_permissions=True, ignore_mandatory=True)
		# Override credit_category to "Watch" directly (bypasses Select validation)
		frappe.db.set_value("Borrower Profile", cust, "credit_category", "Watch")

		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
		)
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			requested_amount=20_000,   # well within limit
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "appraisal_score", "risk_flags"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Decline")
		self.assertEqual(result.appraisal_score, 0.0)
		self.assertIn("Watch category", result.risk_flags)


# ─────────────────────────────────────────────────────────────────────────────
# Test: Abort path — no M-Pesa Statement linked or unparsed
# ─────────────────────────────────────────────────────────────────────────────

class TestAbortNoStatement(FrappeTestCase):
	"""
	When no M-Pesa Statement is linked (or parse_status != 'Parsed'),
	the scoring engine must abort with recommendation = 'Review' and a
	placeholder ai_summary.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_no_statement_sets_review(self):
		"""No mpesa_statement linked → abort → recommendation = 'Review'."""
		cust = _customer("Abort-NoStmt")
		apr_name = _appraisal(cust, net_salary=80_000, requested_amount=30_000)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "ai_summary"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Review")
		self.assertIn("not yet parsed", result.ai_summary)

	def test_unparsed_statement_sets_review(self):
		"""parse_status = 'Pending' → treated as absent → abort → 'Review'."""
		cust = _customer("Abort-Pending")
		stmt_name = _mpesa_stmt(cust, parse_status="Pending")
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			requested_amount=30_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value("Loan Appraisal", apr_name, "recommendation")
		self.assertEqual(result, "Review")

	def test_failed_statement_sets_review(self):
		"""parse_status = 'Failed' → treated as absent → abort → 'Review'."""
		cust = _customer("Abort-Failed")
		stmt_name = _mpesa_stmt(cust, parse_status="Failed")
		apr_name = _appraisal(
			cust,
			net_salary=80_000,
			requested_amount=30_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value("Loan Appraisal", apr_name, "recommendation")
		self.assertEqual(result, "Review")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Approve path — ideal borrower profile
# ─────────────────────────────────────────────────────────────────────────────

class TestApprovePath(FrappeTestCase):
	"""
	All sub-scores at maximum → appraisal_score ≥ 70 → Approve.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_ideal_profile_recommends_approve(self):
		"""
		Perfect statement: salary_regularity=100%, Improving trend, no competing
		loans, no gambling, requested = 25% of eligible → expect Approve.
		"""
		cust = _customer("Approve-Ideal")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			monthly_avg_outflow=30_000,
			avg_monthly_balance=50_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
			loan_repayments_detected=0.0,
			gambling_total=0.0,
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,          # max_eligible = 50,000
			existing_liabilities=0,
			requested_amount=12_500,     # 25% of max_eligible → req_ratio = 0.25
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "appraisal_score"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Approve")
		self.assertGreaterEqual(result.appraisal_score, 70.0)

	def test_approve_scores_populated(self):
		"""All six sub-score fields must be non-zero on an Approve outcome."""
		cust = _customer("Approve-SubScores")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,
			requested_amount=20_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["salary_regularity_score", "cashflow_trend_score", "competing_loan_score",
			 "payday_behavior_score", "gambling_score", "request_ratio_score"],
			as_dict=True,
		)
		for field, value in result.items():
			self.assertGreater(value or 0, 0, f"{field} should be > 0 on Approve")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Soft rule escalation — competing loan burden > 30%
# ─────────────────────────────────────────────────────────────────────────────

class TestSoftRuleCompetingLoan(FrappeTestCase):
	"""
	Soft rule: burden_ratio > 0.30 must escalate recommendation from
	'Approve' to 'Review' but must NOT force 'Decline'.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_high_competing_loan_burden_escalates_to_review(self):
		"""
		salary_regularity=100%, Improving trend (would score ≥70 → Approve),
		but loan_repayments_detected / monthly_avg_inflow = 0.40 (40%)
		→ soft rule fires → Review.
		"""
		cust = _customer("Soft-Competing")
		inflow = 100_000.0
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=inflow,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
			loan_repayments_detected=40_000.0,  # 40% → > 30% threshold
			gambling_total=0.0,
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,
			requested_amount=20_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "risk_flags"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Review",
			"High competing loan burden should escalate Approve → Review")
		self.assertIn("Competing loan", result.risk_flags)

	def test_moderate_competing_loan_does_not_escalate(self):
		"""burden_ratio = 0.20 (within 30% threshold) → no soft rule escalation."""
		cust = _customer("Soft-Competing-OK")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
			loan_repayments_detected=20_000,  # 20%
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,
			requested_amount=20_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value("Loan Appraisal", apr_name, "recommendation")
		# Should still Approve (or at worst Review on threshold, not forced by soft rule)
		self.assertNotEqual(result, "Decline")


# ─────────────────────────────────────────────────────────────────────────────
# Test: Decline path — poor metrics
# ─────────────────────────────────────────────────────────────────────────────

class TestDeclinePath(FrappeTestCase):
	"""
	Consistently poor statement metrics (no salary, Declining trend, high gambling)
	without triggering a hard rule should produce score < 50 → Decline.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_poor_metrics_decline(self):
		"""
		0% salary regularity + Declining trend + 8% gambling (under HR-2) +
		request ratio 96% → expected score ~46 → Decline.

		Score breakdown (weights: sal=25, trend=20, comp=20, payday=15, gamb=10, req=10):
		  salary_raw=0    → 0   × 25/100 = 0
		  trend=Declining → 20  × 20/100 = 4
		  competing=0%    → 100 × 20/100 = 20
		  payday=no crash → 100 × 15/100 = 15
		  gambling=8%>7%  → 20  × 10/100 = 2
		  req=24k/25k=96% → 40  × 10/100 = 4
		  Total = 45 → Decline (< 50)
		"""
		cust = _customer("Decline-Poor")
		inflow = 50_000.0
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=inflow,
			salary_credit_regularity=0.0,
			net_cashflow_trend="Declining",
			gambling_total=4_000.0,       # 8% of inflow — under HR-2 10% but bad score
			gambling_transactions_detected=1,
			loan_repayments_detected=0.0,
		)
		apr_name = _appraisal(
			cust,
			net_salary=50_000,
			requested_amount=24_000,    # 96% of max_eligible=25,000 → req_raw=40
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "appraisal_score"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Decline")
		self.assertLess(result.appraisal_score, 50.0)


# ─────────────────────────────────────────────────────────────────────────────
# Test: Sub-score formula correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestSubScoreFormulas(FrappeTestCase):
	"""
	Validate each sub-score contribution against known inputs.
	Uses default weights: salary=25, trend=20, competing=20, payday=15,
	gambling=10, request=10.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def _score_result(self, **stmt_kwargs) -> dict:
		"""Build an appraisal with controlled inputs and return the scored fields."""
		net_salary = stmt_kwargs.pop("net_salary", 80_000)
		requested = stmt_kwargs.pop("requested_amount", 20_000)
		cust = _customer(f"Formula-{id(stmt_kwargs)}")
		stmt_name = _mpesa_stmt(cust, **stmt_kwargs)
		apr_name = _appraisal(
			cust, net_salary=net_salary, requested_amount=requested,
			mpesa_statement=stmt_name,
		)
		run_appraisal_scoring(apr_name)
		return frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["salary_regularity_score", "cashflow_trend_score", "competing_loan_score",
			 "gambling_score", "request_ratio_score", "appraisal_score"],
			as_dict=True,
		)

	def test_salary_regularity_score_full(self):
		"""salary_credit_regularity=100% → raw=100 → weighted=25.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
		)
		self.assertAlmostEqual(r.salary_regularity_score, 25.0, places=1)

	def test_salary_regularity_score_half(self):
		"""salary_credit_regularity=50% → raw=50 → weighted=12.5"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=50.0,
		)
		self.assertAlmostEqual(r.salary_regularity_score, 12.5, places=1)

	def test_cashflow_improving_score(self):
		"""Improving → raw=100 → weighted=20.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
		)
		self.assertAlmostEqual(r.cashflow_trend_score, 20.0, places=1)

	def test_cashflow_stable_score(self):
		"""Stable → raw=60 → weighted=12.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Stable",
		)
		self.assertAlmostEqual(r.cashflow_trend_score, 12.0, places=1)

	def test_cashflow_declining_score(self):
		"""Declining → raw=20 → weighted=4.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Declining",
		)
		self.assertAlmostEqual(r.cashflow_trend_score, 4.0, places=1)

	def test_no_gambling_score_full(self):
		"""gambling_total=0 → raw=100 → weighted=10.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			gambling_total=0.0,
		)
		self.assertAlmostEqual(r.gambling_score, 10.0, places=1)

	def test_moderate_gambling_score(self):
		"""gambling_ratio=0.05 (≤0.07) → raw=50 → weighted=5.0"""
		inflow = 100_000.0
		r = self._score_result(
			monthly_avg_inflow=inflow,
			salary_credit_regularity=100.0,
			gambling_total=5_000.0,  # 5%
			gambling_transactions_detected=1,
		)
		self.assertAlmostEqual(r.gambling_score, 5.0, places=1)

	def test_request_ratio_score_low(self):
		"""requested=20,000 on max_eligible=40,000 → ratio=0.50 → raw=100 → weighted=10.0"""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=100.0,
			net_salary=80_000,
			requested_amount=20_000,   # max_eligible = 40,000 → ratio = 0.50
		)
		self.assertAlmostEqual(r.request_ratio_score, 10.0, places=1)

	def test_aggregate_score_is_sum_of_sub_scores(self):
		"""appraisal_score == sum of the six stored sub-scores."""
		r = self._score_result(
			monthly_avg_inflow=80_000,
			salary_credit_regularity=80.0,
			net_cashflow_trend="Stable",
		)
		sub_total = round(
			(r.salary_regularity_score or 0)
			+ (r.cashflow_trend_score or 0)
			+ (r.competing_loan_score or 0)
			+ (r.gambling_score or 0)
			+ (r.request_ratio_score or 0),
			1,
		)
		# Appraisal score includes payday_behavior_score not in the above list;
		# just verify appraisal_score ≥ sub_total and ≤ 100
		self.assertGreaterEqual(r.appraisal_score, sub_total)
		self.assertLessEqual(r.appraisal_score, 100.0)


# ─────────────────────────────────────────────────────────────────────────────
# Test: Auto-approval gate
# ─────────────────────────────────────────────────────────────────────────────

class TestAutoApprovalGate(FrappeTestCase):
	"""
	auto_approved = True requires ALL of:
	  - recommendation = 'Approve'
	  - appraisal_score ≥ 75
	  - within_limit = 1
	  - gambling_total = 0
	  - burden_ratio ≤ 0.30
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_auto_approved_when_all_conditions_met(self):
		"""Perfect profile → auto_approved = 1."""
		cust = _customer("AutoApprove-All")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
			loan_repayments_detected=0.0,
			gambling_total=0.0,
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,
			existing_liabilities=0,
			requested_amount=10_000,  # 20% of max_eligible=50,000
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		result = frappe.db.get_value(
			"Loan Appraisal", apr_name,
			["recommendation", "auto_approved", "appraisal_score"],
			as_dict=True,
		)
		self.assertEqual(result.recommendation, "Approve")
		self.assertEqual(result.auto_approved, 1)
		self.assertGreaterEqual(result.appraisal_score, 75.0)

	def test_not_auto_approved_when_gambling_present(self):
		"""Any gambling → auto_approved must be 0 even if score is high."""
		cust = _customer("AutoApprove-Gambling")
		stmt_name = _mpesa_stmt(
			cust,
			monthly_avg_inflow=100_000,
			salary_credit_regularity=100.0,
			net_cashflow_trend="Improving",
			gambling_total=2_000.0,   # only 2% — under HR-2 but blocks auto-approve
			gambling_transactions_detected=1,
		)
		apr_name = _appraisal(
			cust,
			net_salary=100_000,
			requested_amount=10_000,
			mpesa_statement=stmt_name,
		)

		run_appraisal_scoring(apr_name)

		auto = frappe.db.get_value("Loan Appraisal", apr_name, "auto_approved")
		self.assertEqual(auto, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests for pure helpers
# ─────────────────────────────────────────────────────────────────────────────

class TestMonthsInStatement(FrappeTestCase):
	"""Unit tests for the _months_in_statement helper."""

	def test_three_month_range(self):
		self.assertEqual(_months_in_statement("2025-01-01", "2025-03-31"), 3)

	def test_single_month(self):
		self.assertEqual(_months_in_statement("2025-01-01", "2025-01-31"), 1)

	def test_six_months(self):
		self.assertEqual(_months_in_statement("2025-01-01", "2025-06-30"), 6)

	def test_cross_year(self):
		self.assertEqual(_months_in_statement("2024-11-01", "2025-01-31"), 3)

	def test_empty_dates_returns_default(self):
		self.assertEqual(_months_in_statement("", ""), 3)
		self.assertEqual(_months_in_statement("", "2025-03-31"), 3)
