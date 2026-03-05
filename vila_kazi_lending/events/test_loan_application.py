"""
Tests for loan_application event handler routing logic (WF-01, WF-02, WF-03).

Run with:
    bench --site lending.erpkenya.com run-tests --module vila_kazi_lending.events.test_loan_application
"""

from __future__ import annotations

from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_days, nowdate

from vila_kazi_lending.events.loan_application import _route_workflow


class _MockDoc:
	"""Minimal stand-in for a Loan Application document for routing unit tests."""

	def __init__(self, **kwargs):
		self.name = "TEST-LOAP-ROUTE-001"
		self.applicant = "TEST-CUST-ROUTE-001"
		self.applicant_name = "Test Borrower"
		self.loan_amount = 10000.0
		self.vk_loan_stage = ""
		self.vk_is_refinancing = 0
		self.vk_is_repeat_borrower = 0
		self.vk_decision_notes = ""
		self.vk_max_eligible_amount = 15000.0
		self.__dict__.update(kwargs)

	def get(self, key, default=None):
		return getattr(self, key, default)


class TestLoanApplicationRouting(FrappeTestCase):
	"""Unit tests for _route_workflow — covers all three entry paths."""

	# ------------------------------------------------------------------
	# WF-01 — New Borrower
	# ------------------------------------------------------------------

	def test_new_borrower_no_profile_sets_draft(self):
		"""No Borrower Profile found → stage = Draft, repeat = 0."""
		doc = _MockDoc()
		with patch("frappe.db.get_value", return_value=None):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Draft")
		self.assertEqual(doc.vk_is_repeat_borrower, 0)

	def test_new_borrower_kyc_pending_sets_draft(self):
		"""Profile exists but KYC not Verified → stage = Draft, repeat = 0."""
		doc = _MockDoc()
		profile = frappe._dict(kyc_status="Pending", framework_agreement=None)
		with patch("frappe.db.get_value", return_value=profile):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Draft")
		self.assertEqual(doc.vk_is_repeat_borrower, 0)

	def test_profile_verified_but_fa_not_active_sets_draft(self):
		"""Verified KYC + inactive FA → abbreviated re-sign path → Draft."""
		doc = _MockDoc()

		def _db_get(doctype, filters=None, fieldname=None, *a, **kw):
			if doctype == "Borrower Profile":
				return frappe._dict(kyc_status="Verified", framework_agreement="FA-0001")
			if doctype == "Loan Framework Agreement":
				return "Expired"
			return None

		with patch("frappe.db.get_value", side_effect=_db_get):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Draft")
		self.assertEqual(doc.vk_is_repeat_borrower, 0)

	# ------------------------------------------------------------------
	# WF-03 — Refinancing
	# ------------------------------------------------------------------

	def test_refinancing_flag_sets_refinancing_requested(self):
		"""vk_is_refinancing=1 → Refinancing Requested, no DB lookup performed."""
		doc = _MockDoc(vk_is_refinancing=1)
		with patch("frappe.db.get_value", side_effect=AssertionError("unexpected DB call")):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Refinancing Requested")

	# ------------------------------------------------------------------
	# WF-02 — Repeat Fast Lane
	# ------------------------------------------------------------------

	def _active_fa_db_mock(self, loan_amount=10000.0, max_eligible=15000.0):
		"""Returns a mock for frappe.db.get_value that simulates an active FA borrower."""

		def _db_get(doctype, filters=None, fieldname=None, *a, **kw):
			if doctype == "Borrower Profile":
				return frappe._dict(kyc_status="Verified", framework_agreement="FA-0001")
			if doctype == "Loan Framework Agreement":
				return "Active"
			if doctype == "Loan Application":
				return frappe._dict(
					applicant="TEST-CUST-ROUTE-001",
					loan_amount=loan_amount,
					vk_max_eligible_amount=max_eligible,
					vk_framework_agreement="FA-0001",
				)
			return None

		return _db_get

	def _on_time_repayments(self, count=3):
		"""Return `count` on-time repayment records."""
		return [
			frappe._dict(
				status="Received",
				received_date=nowdate(),
				expected_date=add_days(nowdate(), 1),
			)
			for _ in range(count)
		]

	def test_repeat_borrower_gate_pass_sets_pending_lender_confirm(self):
		"""Verified KYC + Active FA + all gate conditions pass → Pending Lender Confirm."""
		doc = _MockDoc()
		with (
			patch("frappe.db.get_value", side_effect=self._active_fa_db_mock()),
			patch("frappe.db.sql", return_value=self._on_time_repayments(3)),
		):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Pending Lender Confirm")
		self.assertEqual(doc.vk_is_repeat_borrower, 1)

	def test_repeat_borrower_amount_exceeds_eligible_falls_to_standard_review(self):
		"""Gate fails because loan_amount > max_eligible → Standard Review."""
		doc = _MockDoc(loan_amount=20000.0)
		with (
			patch(
				"frappe.db.get_value",
				side_effect=self._active_fa_db_mock(loan_amount=20000.0, max_eligible=15000.0),
			),
			patch("frappe.db.sql", return_value=self._on_time_repayments(3)),
		):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Standard Review")
		self.assertEqual(doc.vk_is_repeat_borrower, 1)

	def test_repeat_borrower_no_repayment_history_falls_to_standard_review(self):
		"""Gate fails because no prior repayment history → Standard Review."""
		doc = _MockDoc()
		with (
			patch("frappe.db.get_value", side_effect=self._active_fa_db_mock()),
			patch("frappe.db.sql", return_value=[]),
		):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Standard Review")

	def test_repeat_borrower_partial_payment_history_falls_to_standard_review(self):
		"""Gate fails because last repayment was Partial, not Received → Standard Review."""
		doc = _MockDoc()
		partial_records = [
			frappe._dict(status="Partial", received_date=None, expected_date=nowdate()),
		]
		with (
			patch("frappe.db.get_value", side_effect=self._active_fa_db_mock()),
			patch("frappe.db.sql", return_value=partial_records),
		):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Standard Review")

	# ------------------------------------------------------------------
	# Duplicate detection guard
	# ------------------------------------------------------------------

	def test_duplicate_stage_preserved_routing_skipped(self):
		"""If stage is already 'Duplicate - Review', _route_workflow must not overwrite it."""
		doc = _MockDoc(vk_loan_stage="Duplicate - Review")
		with patch("frappe.db.get_value", side_effect=AssertionError("should not query DB")):
			_route_workflow(doc)
		self.assertEqual(doc.vk_loan_stage, "Duplicate - Review")
