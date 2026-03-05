"""
Tests for RepaymentReconciliation controller — collections, write-off,
and overdue category demotion logic.

Run with:
    bench --site lending.erpkenya.com run-tests \
        --module vila_kazi_lending.vila_kazi_lending.doctype.repayment_reconciliation.test_repayment_reconciliation
"""

from __future__ import annotations

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_days, nowdate


class TestRepaymentReconciliationController(FrappeTestCase):
	"""Unit + integration tests for the RepaymentReconciliation controller."""

	# ------------------------------------------------------------------
	# Test fixtures
	# ------------------------------------------------------------------

	def setUp(self):
		super().setUp()
		# Create a test Customer
		self.customer = frappe.get_doc(
			{
				"doctype": "Customer",
				"customer_name": "VK Test Borrower RR",
				"customer_type": "Individual",
				"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
				or "All Customer Groups",
				"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name")
				or "All Territories",
			}
		).insert(ignore_permissions=True)

		# Create a Borrower Profile at Gold tier
		self.profile = frappe.get_doc(
			{
				"doctype": "Borrower Profile",
				"customer": self.customer.name,
				"kyc_status": "Verified",
				"credit_category": "Gold",
				# Supply dummy values for mandatory doctype fields so insert() doesn't fail
				"national_id_number": "TEST123456",
				"national_id_scan": "/files/test-id.pdf",
				"employer_name": "Test Employer Ltd",
				"employment_letter": "/files/test-emp.pdf",
				"mpesa_number": "0712345678",
				"net_salary": 50000,
			}
		).insert(ignore_permissions=True, ignore_mandatory=True)

	def tearDown(self):
		frappe.db.rollback()
		super().tearDown()

	def _new_rr(self, **kwargs) -> object:
		"""
		Return an unsaved RepaymentReconciliation document pre-filled with
		safe defaults. Attributes are set directly so controller methods can
		be called without a DB save.
		"""
		rr = frappe.new_doc("Repayment Reconciliation")
		rr.borrower = self.customer.name
		rr.loan = None
		rr.status = "Expected"
		rr.expected_amount = 10000.0
		rr.received_amount = 0.0
		rr.expected_date = nowdate()
		rr.days_overdue = 0
		rr.vk_collections_stage = ""
		rr.vk_write_off_reason = ""
		rr.vk_contact_log = None
		rr.__dict__.update(kwargs)
		return rr

	def _get_profile_category(self) -> str:
		return (
			frappe.db.get_value("Borrower Profile", self.profile.name, "credit_category")
			or ""
		)

	# ------------------------------------------------------------------
	# Write-off validation
	# ------------------------------------------------------------------

	def test_write_off_raises_without_reason(self):
		"""_validate_write_off must throw when vk_write_off_reason is blank."""
		rr = self._new_rr(vk_collections_stage="Written Off", vk_write_off_reason="")
		with self.assertRaises(Exception):
			rr._validate_write_off()

	def test_write_off_raises_with_whitespace_only_reason(self):
		"""A whitespace-only reason must also be rejected."""
		rr = self._new_rr(vk_collections_stage="Written Off", vk_write_off_reason="   ")
		with self.assertRaises(Exception):
			rr._validate_write_off()

	def test_write_off_passes_with_reason(self):
		"""_validate_write_off must NOT raise when a non-empty reason is supplied."""
		rr = self._new_rr(vk_collections_stage="Written Off", vk_write_off_reason="Borrower unable to repay")
		# Should not raise
		rr._validate_write_off()

	def test_validate_non_write_off_skips_check(self):
		"""_validate_write_off must do nothing when collections stage is not Written Off."""
		rr = self._new_rr(vk_collections_stage="Collections Active", vk_write_off_reason="")
		# Should not raise even with blank reason
		rr._validate_write_off()

	# ------------------------------------------------------------------
	# Category impact — on repayment
	# ------------------------------------------------------------------

	def test_category_unchanged_when_on_time(self):
		"""days_overdue <= 3 → credit_category must stay Gold."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=0)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Gold")

	def test_category_unchanged_at_3_days_overdue(self):
		"""Exactly 3 days overdue → no demotion (boundary test)."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=3)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Gold")

	def test_category_demoted_at_4_days_overdue(self):
		"""4 days overdue → Gold demoted to Silver (boundary test)."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=4)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Silver")

	def test_category_demoted_at_7_days_overdue(self):
		"""7 days overdue → Gold demoted to Silver (boundary test)."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=7)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Silver")

	def test_category_set_to_watch_at_8_days_overdue(self):
		"""8 days overdue → category set to Watch (boundary test)."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=8)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Watch")

	def test_category_set_to_watch_at_14_days_overdue(self):
		"""14 days overdue → category remains Watch."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr(days_overdue=14)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "Watch")

	def test_category_demotion_does_not_go_below_new(self):
		"""A borrower already at New (lowest) cannot be demoted further."""
		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "New")
		rr = self._new_rr(days_overdue=5)
		rr._apply_category_impact()
		self.assertEqual(self._get_profile_category(), "New")

	# ------------------------------------------------------------------
	# Write-off → Borrower Profile effect
	# ------------------------------------------------------------------

	def test_write_off_sets_borrower_to_watch(self):
		"""_handle_write_off must set Borrower Profile credit_category to Watch."""
		from unittest.mock import patch

		frappe.db.set_value("Borrower Profile", self.profile.name, "credit_category", "Gold")
		rr = self._new_rr()

		# Suppress the actual lender notification email during the test
		with patch.object(rr, "_notify_lender_write_off", return_value=None):
			rr._handle_write_off()

		self.assertEqual(self._get_profile_category(), "Watch")

	def test_write_off_with_no_borrower_does_not_raise(self):
		"""_handle_write_off on an RR without a borrower must exit cleanly."""
		from unittest.mock import patch

		rr = self._new_rr()
		rr.borrower = None

		with patch.object(rr, "_notify_lender_write_off", return_value=None):
			rr._handle_write_off()  # must not raise

	# ------------------------------------------------------------------
	# Auto-status logic (before_save)
	# ------------------------------------------------------------------

	def test_auto_status_received_when_fully_paid(self):
		"""received_amount >= expected_amount → status set to Received by _auto_set_status."""
		rr = self._new_rr(expected_amount=10000.0, received_amount=10000.0, status="Expected")
		rr._auto_set_status()
		self.assertEqual(rr.status, "Received")

	def test_auto_status_partial_when_partially_paid(self):
		"""0 < received < expected → status set to Partial by _auto_set_status."""
		rr = self._new_rr(expected_amount=10000.0, received_amount=5000.0, status="Expected")
		rr._auto_set_status()
		self.assertEqual(rr.status, "Partial")

	def test_auto_status_waived_not_overridden(self):
		"""A Waived status must not be changed regardless of amounts."""
		rr = self._new_rr(expected_amount=10000.0, received_amount=10000.0, status="Waived")
		rr._auto_set_status()
		self.assertEqual(rr.status, "Waived")
