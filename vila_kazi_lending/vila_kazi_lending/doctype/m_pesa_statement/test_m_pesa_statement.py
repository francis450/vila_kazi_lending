"""
Tests for the M-Pesa Statement parser — Layer 4 of Vila Kazi Lending.

Run with:
    bench --site lending.erpkenya.com run-tests --app vila_kazi_lending \
        --module vila_kazi_lending.vila_kazi_lending.doctype.m_pesa_statement.test_m_pesa_statement
"""

from __future__ import annotations

import json
import textwrap
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from vila_kazi_lending.mpesa_parser import (
	_DEFAULT_GAMBLING_KEYWORDS,
	_compute_metrics,
	_linear_trend,
	_parse_csv_text,
	categorise,
	parse_csv_content,
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared CSV fixture helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_csv(*rows: dict) -> str:
	"""Build a minimal M-Pesa CSV from a list of row dicts."""
	header = "Receipt No.,Completion Time,Details,Transaction Status,Paid In,Withdrawn,Balance"
	lines = [header]
	for r in rows:
		lines.append(
			"{receipt_no},{dt},{details},{status},{paid_in},{withdrawn},{balance}".format(
				receipt_no=r.get("receipt_no", "ABCD123456"),
				dt=r.get("dt", "2025-01-15 10:00:00"),
				details=r.get("details", "Transfer"),
				status=r.get("status", "COMPLETED"),
				paid_in=r.get("paid_in", "0.00"),
				withdrawn=r.get("withdrawn", "0.00"),
				balance=r.get("balance", "10000.00"),
			)
		)
	return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — CSV happy path: 3-month statement with known values
# ─────────────────────────────────────────────────────────────────────────────

class TestCsvParseHappyPath(FrappeTestCase):
	"""
	Parse a synthetic 3-month CSV and assert computed aggregates are correct.

	Months: Jan, Feb, Mar 2025
	  Jan: salary 60,000 in | expenses 25,000 out | balance 35,000 | gambling 2,000
	  Feb: salary 60,000 in | expenses 30,000 out | balance 30,000 | gambling 1,500
	  Mar: salary 60,000 in | expenses 20,000 out | balance 40,000
	"""

	def setUp(self):
		super().setUp()
		self.gambling_kws = ["sportpesa", "odibets", "betika", "betway", "mozzart"]

	def _build_csv(self) -> str:
		return _make_csv(
			# January — salary credit
			{"receipt_no": "JAN0000001", "dt": "2025-01-25 09:00:00",
			 "details": "Salary Payment from Employer Ltd",
			 "paid_in": "60000.00", "withdrawn": "0.00", "balance": "62000.00"},
			# January — grocery merchant
			{"receipt_no": "JAN0000002", "dt": "2025-01-28 11:00:00",
			 "details": "Merchant Payment - Naivas Supermarket",
			 "paid_in": "0.00", "withdrawn": "25000.00", "balance": "37000.00"},
			# January — SportPesa gambling (out)
			{"receipt_no": "JAN0000003", "dt": "2025-01-29 20:00:00",
			 "details": "Pay Bill - SportPesa deposit",
			 "paid_in": "0.00", "withdrawn": "2000.00", "balance": "35000.00"},
			# February — salary credit
			{"receipt_no": "FEB0000001", "dt": "2025-02-25 09:00:00",
			 "details": "Salary Payment from Employer Ltd",
			 "paid_in": "60000.00", "withdrawn": "0.00", "balance": "65000.00"},
			# February — expenses
			{"receipt_no": "FEB0000002", "dt": "2025-02-26 12:00:00",
			 "details": "Pay Bill - KPLC Prepaid",
			 "paid_in": "0.00", "withdrawn": "28500.00", "balance": "36500.00"},
			# February — gambling
			{"receipt_no": "FEB0000003", "dt": "2025-02-27 19:00:00",
			 "details": "Pay Bill - Betika",
			 "paid_in": "0.00", "withdrawn": "1500.00", "balance": "35000.00"},
			# March — salary credit
			{"receipt_no": "MAR0000001", "dt": "2025-03-25 09:00:00",
			 "details": "Salary Payment from Employer Ltd",
			 "paid_in": "60000.00", "withdrawn": "0.00", "balance": "65000.00"},
			# March — expenses
			{"receipt_no": "MAR0000002", "dt": "2025-03-27 14:00:00",
			 "details": "Merchant Payment - Carrefour",
			 "paid_in": "0.00", "withdrawn": "20000.00", "balance": "45000.00"},
			# March — transfer out
			{"receipt_no": "MAR0000003", "dt": "2025-03-28 16:00:00",
			 "details": "Customer Transfer - John Doe",
			 "paid_in": "0.00", "withdrawn": "5000.00", "balance": "40000.00"},
		)

	def test_monthly_avg_inflow(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		# Each month has exactly 60,000 in salary
		self.assertAlmostEqual(results["monthly_avg_inflow"], 60_000.0, places=0)

	def test_gambling_total(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		# Jan: 2000, Feb: 1500, Mar: 0 → total = 3500
		self.assertAlmostEqual(results["gambling_total"], 3_500.0, places=0)

	def test_gambling_detected_flag(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		self.assertEqual(results["gambling_transactions_detected"], 1)

	def test_salary_credit_regularity(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		# All 3 months have a salary → 100%
		self.assertAlmostEqual(results["salary_credit_regularity"], 100.0, places=0)

	def test_parsed_transactions_json(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		txs = json.loads(results["parsed_transactions"])
		self.assertEqual(len(txs), 9)
		# Every row has the required fields
		required_fields = {"date", "type", "amount", "direction", "balance",
		                   "description", "category", "counterparty"}
		for tx in txs:
			self.assertEqual(required_fields, required_fields & tx.keys())

	def test_salary_transactions_categorised_as_salary_credit(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		txs = json.loads(results["parsed_transactions"])
		salary_txs = [t for t in txs if "Salary Payment" in t["description"]]
		self.assertEqual(len(salary_txs), 3)
		for tx in salary_txs:
			self.assertEqual(tx["category"], "Salary Credit", tx["description"])

	def test_avg_monthly_balance(self):
		results = parse_csv_content(self._build_csv(), self.gambling_kws)
		# Closing balances: Jan=35000, Feb=35000, Mar=40000 → avg = 36666.67
		expected_avg = (35_000 + 35_000 + 40_000) / 3
		self.assertAlmostEqual(results["avg_monthly_balance"], expected_avg, delta=1.0)


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — Gambling must be categorised before Loan Repayment
# ─────────────────────────────────────────────────────────────────────────────

class TestGamblingBeforeLoanRepayment(FrappeTestCase):
	"""
	A transaction with 'SportPesa credit' in the description must be classified
	as 'Gambling', not 'Loan Repayment', because Rule 2 (Gambling) runs before
	Rule 3 (Loan Repayment) and the word 'credit' would otherwise match the
	loan-repayment regex (r'loan|repay|lend|credit ref').

	Note: 'credit ref' is the loan pattern; bare 'credit' by itself does NOT
	match the loan regex, so this test also verifies the 'SportPesa' keyword
	alone is sufficient to classify as Gambling.
	"""

	def _gambling_kws(self) -> list[str]:
		return ["sportpesa", "odibets", "betika", "betway", "mozzart"]

	def test_sportpesa_credit_is_gambling_not_loan(self):
		"""'SportPesa credit' → Gambling, not Loan Repayment."""
		description = "Pay Bill - SportPesa credit"
		direction = "out"
		tx_type = "Pay Bill"
		result = categorise(description, direction, tx_type, self._gambling_kws())
		self.assertEqual(
			result, "Gambling",
			f"Expected 'Gambling' but got {result!r} for description: {description!r}",
		)

	def test_sportpesa_credit_not_loan_repayment(self):
		"""Ensure it is explicitly not classified as Loan Repayment."""
		description = "Pay Bill - SportPesa credit"
		result = categorise(description, "out", "Pay Bill", self._gambling_kws())
		self.assertNotEqual(result, "Loan Repayment")

	def test_odibets_classified_as_gambling(self):
		result = categorise("Pay Bill - Odibets", "out", "Pay Bill", self._gambling_kws())
		self.assertEqual(result, "Gambling")

	def test_full_pipeline_gambling_category_correct(self):
		"""End-to-end CSV parse: SportPesa transaction appears as Gambling in output."""
		csv_text = _make_csv(
			# A valid salary so we have > 1 transaction
			{"receipt_no": "SAL0000001", "dt": "2025-01-25 09:00:00",
			 "details": "Salary Payment from Employer",
			 "paid_in": "50000.00", "withdrawn": "0.00", "balance": "55000.00"},
			# The gambling transaction with 'credit' in description
			{"receipt_no": "GAM0000001", "dt": "2025-01-29 20:00:00",
			 "details": "Pay Bill - SportPesa credit",
			 "paid_in": "0.00", "withdrawn": "3000.00", "balance": "52000.00"},
		)
		# Ensure we have enough rows by adding more padding transactions
		more = [
			{"receipt_no": f"PAD{i:07d}", "dt": f"2025-01-{15+i:02d} 10:00:00",
			 "details": "Customer Transfer - Friend",
			 "paid_in": "0.00", "withdrawn": "200.00", "balance": f"{51000-200*i:.2f}"}
			for i in range(1, 9)
		]
		csv_text = _make_csv(
			{"receipt_no": "SAL0000001", "dt": "2025-01-25 09:00:00",
			 "details": "Salary Payment from Employer",
			 "paid_in": "50000.00", "withdrawn": "0.00", "balance": "55000.00"},
			{"receipt_no": "GAM0000001", "dt": "2025-01-29 20:00:00",
			 "details": "Pay Bill - SportPesa credit",
			 "paid_in": "0.00", "withdrawn": "3000.00", "balance": "52000.00"},
			*more,
		)
		results = parse_csv_content(csv_text, self._gambling_kws())
		txs = json.loads(results["parsed_transactions"])
		gambling_txs = [t for t in txs if t["description"] == "Pay Bill - SportPesa credit"]
		self.assertEqual(len(gambling_txs), 1)
		self.assertEqual(gambling_txs[0]["category"], "Gambling")
		self.assertEqual(results["gambling_transactions_detected"], 1)
		self.assertAlmostEqual(results["gambling_total"], 3000.0, places=0)


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — Insufficient data (< 10 transactions) → parse_status = "Failed"
# ─────────────────────────────────────────────────────────────────────────────

class TestInsufficientDataFails(FrappeTestCase):
	"""
	When an uploaded file contains fewer than 10 transactions the background
	job must set parse_status = 'Failed' and populate parse_error_log.
	"""

	def _create_statement_doc(self, file_url: str) -> str:
		"""Insert a minimal M-Pesa Statement doc and return its name."""
		# We need a Customer to satisfy the borrower link
		customer = frappe.get_doc({
			"doctype": "Customer",
			"customer_name": "VK Test Stmt Insufficient",
			"customer_type": "Individual",
			"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
			               or "All Customer Groups",
			"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name")
			           or "All Territories",
		}).insert(ignore_permissions=True)

		stmt = frappe.get_doc({
			"doctype": "M-Pesa Statement",
			"borrower": customer.name,
			"statement_file": file_url,
			"period_from": "2025-01-01",
			"period_to": "2025-03-31",
			"parse_status": "Pending",
		}).insert(ignore_permissions=True)
		return stmt.name

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def test_fewer_than_10_transactions_sets_failed(self):
		"""
		Provide a CSV with only 5 COMPLETED rows. parse_mpesa_statement() must
		set parse_status = 'Failed' and parse_error_log contains 'Insufficient'.
		"""
		from vila_kazi_lending.tasks import parse_mpesa_statement

		# Build a 5-row CSV
		rows = [
			{"receipt_no": f"TX{i:09d}", "dt": f"2025-01-{10+i:02d} 09:00:00",
			 "details": f"Customer Transfer {i}",
			 "paid_in": "1000.00", "withdrawn": "0.00", "balance": f"{10000+1000*i:.2f}"}
			for i in range(1, 6)
		]
		csv_content = _make_csv(*rows)

		doc_name = self._create_statement_doc("/private/files/test_insufficient.csv")

		# Patch the file open so we don't need real filesystem access
		with patch("vila_kazi_lending.mpesa_parser._parse_csv") as mock_csv, \
		     patch("vila_kazi_lending.mpesa_parser._resolve_path") as mock_path:
			mock_path.return_value = MagicMock(suffix=".csv")

			from pathlib import Path as _Path
			import io, csv as _csv
			from vila_kazi_lending.mpesa_parser import _parse_csv_text
			mock_csv.side_effect = lambda _p: _parse_csv_text(csv_content)

			parse_mpesa_statement(doc_name)

		result = frappe.db.get_value(
			"M-Pesa Statement",
			doc_name,
			["parse_status", "parse_error_log"],
			as_dict=True,
		)
		self.assertEqual(result.parse_status, "Failed")
		self.assertIn("Insufficient", result.parse_error_log)
		self.assertIn("5", result.parse_error_log)


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — File attachment not found → parse_status = "Failed"
# ─────────────────────────────────────────────────────────────────────────────

class TestFileNotFoundFails(FrappeTestCase):
	"""
	When the M-Pesa Statement has no file attached (statement_file is blank or
	the file does not exist on disk), the background job must set
	parse_status = 'Failed' and populate parse_error_log.
	"""

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def _make_customer(self) -> str:
		cust = frappe.get_doc({
			"doctype": "Customer",
			"customer_name": "VK Test Stmt No File",
			"customer_type": "Individual",
			"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
			               or "All Customer Groups",
			"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name")
			           or "All Territories",
		}).insert(ignore_permissions=True)
		return cust.name

	def test_missing_file_attachment_sets_failed(self):
		"""
		A statement_file URL that points to a non-existent file on disk must
		result in parse_status = 'Failed' with a populated parse_error_log.
		"""
		from vila_kazi_lending.tasks import parse_mpesa_statement

		cust_name = self._make_customer()
		stmt = frappe.get_doc({
			"doctype": "M-Pesa Statement",
			"borrower": cust_name,
			"statement_file": "/private/files/nonexistent_file_xyz.csv",
			"period_from": "2025-01-01",
			"period_to": "2025-03-31",
			"parse_status": "Pending",
		}).insert(ignore_permissions=True)
		doc_name = stmt.name

		# Do NOT mock the file — we want the real "file not found" failure path
		parse_mpesa_statement(doc_name)

		result = frappe.db.get_value(
			"M-Pesa Statement",
			doc_name,
			["parse_status", "parse_error_log"],
			as_dict=True,
		)
		self.assertEqual(result.parse_status, "Failed")
		self.assertTrue(
			result.parse_error_log and len(result.parse_error_log) > 0,
			"parse_error_log must be non-empty when file is not found",
		)

	def test_blank_statement_file_sets_failed(self):
		"""
		A statement with statement_file = '' must also fail gracefully.
		parse_error_log is expected to be populated.
		"""
		from vila_kazi_lending.tasks import parse_mpesa_statement

		cust_name = self._make_customer()
		stmt = frappe.get_doc({
			"doctype": "M-Pesa Statement",
			"borrower": cust_name,
			"statement_file": "/private/files/another_nonexistent.pdf",
			"period_from": "2025-01-01",
			"period_to": "2025-03-31",
			"parse_status": "Pending",
		}).insert(ignore_permissions=True)
		doc_name = stmt.name

		# Overwrite statement_file to empty after insert (bypassing reqd validation)
		frappe.db.set_value("M-Pesa Statement", doc_name, "statement_file", "")

		parse_mpesa_statement(doc_name)

		result = frappe.db.get_value(
			"M-Pesa Statement",
			doc_name,
			["parse_status", "parse_error_log"],
			as_dict=True,
		)
		self.assertEqual(result.parse_status, "Failed")
		self.assertTrue(result.parse_error_log and len(result.parse_error_log) > 0)


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests for pure helper functions (no Frappe DB needed)
# ─────────────────────────────────────────────────────────────────────────────

class TestCategorisationRules(FrappeTestCase):
	"""Verify the ordered categorisation rules in isolation."""

	KWS = ["sportpesa", "odibets", "betika", "betway", "mozzart"]

	def test_salary_credit_direction_in(self):
		self.assertEqual(
			categorise("Salary credit from ABC Corp", "in", "B2C Payment", self.KWS),
			"Salary Credit",
		)

	def test_salary_direction_out_not_salary_credit(self):
		# Direction must be "in" for Salary Credit
		result = categorise("Salary deduction", "out", "Other", self.KWS)
		self.assertNotEqual(result, "Salary Credit")

	def test_gambling_wins_over_loan(self):
		# "Betway credit" — gambling keyword present, also matches loan regex (credit ref won't match here)
		result = categorise("Pay Bill - Betway", "out", "Pay Bill", self.KWS)
		self.assertEqual(result, "Gambling")

	def test_loan_repayment_matches(self):
		result = categorise("Loan repayment to Faulu Kenya", "out", "Pay Bill", self.KWS)
		self.assertEqual(result, "Loan Repayment")

	def test_utilities_kplc(self):
		result = categorise("Pay Bill - KPLC Prepaid", "out", "Pay Bill", self.KWS)
		self.assertEqual(result, "Utilities")

	def test_airtime(self):
		result = categorise("Airtime purchase", "out", "Airtime", self.KWS)
		self.assertEqual(result, "Airtime")

	def test_b2c_transfer_in(self):
		result = categorise("B2C payment received from Company", "in", "B2C Payment", self.KWS)
		self.assertEqual(result, "B2C Transfer")

	def test_other_fallthrough(self):
		result = categorise("Random unclassified text", "out", "Other", self.KWS)
		self.assertEqual(result, "Other")


class TestLinearTrend(FrappeTestCase):
	"""Unit tests for the linear trend helper."""

	def test_improving(self):
		# Strongly increasing monthly nets
		values = [10_000, 15_000, 20_000, 25_000, 30_000]
		self.assertEqual(_linear_trend(values), "Improving")

	def test_declining(self):
		values = [30_000, 25_000, 20_000, 15_000, 10_000]
		self.assertEqual(_linear_trend(values), "Declining")

	def test_stable_flat(self):
		values = [20_000, 20_000, 20_000, 20_000]
		self.assertEqual(_linear_trend(values), "Stable")

	def test_single_value(self):
		self.assertEqual(_linear_trend([15_000]), "Stable")

	def test_empty(self):
		self.assertEqual(_linear_trend([]), "Stable")
