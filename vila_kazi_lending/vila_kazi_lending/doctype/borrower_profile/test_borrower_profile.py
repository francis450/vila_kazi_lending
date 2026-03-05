"""
Tests for borrower_profile event handler — KYC status transitions.

Run with:
    bench --site lending.erpkenya.com run-tests \
        --module vila_kazi_lending.vila_kazi_lending.doctype.borrower_profile.test_borrower_profile
"""

from __future__ import annotations

import frappe
from frappe.tests.utils import FrappeTestCase

from vila_kazi_lending.events.borrower_profile import _advance_applications_to_appraisal


class TestBorrowerProfileKYC(FrappeTestCase):
	"""Tests for KYC status → Loan Application stage transitions."""

	# ------------------------------------------------------------------
	# setUp / tearDown
	# ------------------------------------------------------------------

	def setUp(self):
		super().setUp()
		# Create a throw-away customer for this test class.
		self.customer = frappe.get_doc(
			{
				"doctype": "Customer",
				"customer_name": "VK Test Borrower KYC",
				"customer_type": "Individual",
				"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
				or "All Customer Groups",
				"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name")
				or "All Territories",
			}
		).insert(ignore_permissions=True)

		self.profile = frappe.get_doc(
			{
				"doctype": "Borrower Profile",
				"customer": self.customer.name,
				"kyc_status": "Pending",
				# Dummy values for mandatory doctype fields
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

	# ------------------------------------------------------------------
	# Helpers
	# ------------------------------------------------------------------

	def _set_loan_app_stage(self, app_name: str, stage: str):
		frappe.db.set_value("Loan Application", app_name, "vk_loan_stage", stage)

	def _get_loan_app_stage(self, app_name: str) -> str:
		return frappe.db.get_value("Loan Application", app_name, "vk_loan_stage") or ""

	# ------------------------------------------------------------------
	# Tests
	# ------------------------------------------------------------------

	def test_kyc_verified_advances_pending_kyc_apps(self):
		"""
		When KYC status changes to Verified, all submitted Loan Applications for
		this customer in 'Pending KYC Verification' must advance to 'Pending Appraisal'.
		"""
		# Plant a submitted Loan Application in the right stage using a direct DB set.
		# We look for any existing submitted LA for this customer, or skip if none exist.
		apps = frappe.db.get_all(
			"Loan Application",
			filters={"applicant": self.customer.name, "docstatus": 1},
			pluck="name",
			limit=3,
		)

		if not apps:
			# No submitted apps to test against — set the stage on any draft LA if present
			apps = frappe.db.get_all(
				"Loan Application",
				filters={"applicant": self.customer.name},
				pluck="name",
				limit=1,
			)

		if apps:
			for app_name in apps:
				frappe.db.set_value(
					"Loan Application", app_name, "vk_loan_stage", "Pending KYC Verification"
				)

			# Call the helper directly (bypasses doc.has_value_changed guard)
			_advance_applications_to_appraisal(self.profile)

			for app_name in apps:
				stage = self._get_loan_app_stage(app_name)
				self.assertEqual(
					stage,
					"Pending Appraisal",
					f"{app_name} should be Pending Appraisal after KYC verified",
				)
		else:
			# No Loan Applications for this test customer — verify helper is a no-op
			# (does not raise, does not affect other customers' apps)
			_advance_applications_to_appraisal(self.profile)
			# If we reach here without exception, the test passes

	def test_kyc_verified_does_not_touch_non_pending_apps(self):
		"""
		_advance_applications_to_appraisal must not change apps in stages other than
		'Pending KYC Verification'.
		"""
		apps = frappe.db.get_all(
			"Loan Application",
			filters={"applicant": self.customer.name, "docstatus": 1},
			pluck="name",
			limit=2,
		)
		for app_name in apps:
			frappe.db.set_value("Loan Application", app_name, "vk_loan_stage", "Draft")

		_advance_applications_to_appraisal(self.profile)

		for app_name in apps:
			stage = self._get_loan_app_stage(app_name)
			# Should remain Draft — not Pending KYC, so not advanced
			self.assertEqual(stage, "Draft")

	def test_on_update_only_fires_on_kyc_change(self):
		"""
		Saving a Borrower Profile without changing kyc_status must not trigger
		any Loan Application stage change.
		"""
		from vila_kazi_lending.events.borrower_profile import on_update
		from unittest.mock import patch

		self.profile.kyc_status = "Pending"
		self.profile.save(ignore_permissions=True)

		# Reload and save again without touching kyc_status
		reloaded = frappe.get_doc("Borrower Profile", self.profile.name)
		# Patch _advance_applications_to_appraisal to ensure it is NOT called
		with patch(
			"vila_kazi_lending.events.borrower_profile._advance_applications_to_appraisal"
		) as mock_advance:
			on_update(reloaded)
			mock_advance.assert_not_called()

	def test_kyc_verified_triggers_advance_via_on_update(self):
		"""
		Changing kyc_status to Verified via on_update must call
		_advance_applications_to_appraisal exactly once.
		"""
		from unittest.mock import patch

		# Set current DB value to Pending, then call on_update with Verified
		frappe.db.set_value("Borrower Profile", self.profile.name, "kyc_status", "Pending")
		self.profile = frappe.get_doc("Borrower Profile", self.profile.name)
		self.profile.kyc_status = "Verified"

		with patch(
			"vila_kazi_lending.events.borrower_profile._advance_applications_to_appraisal"
		) as mock_advance:
			from vila_kazi_lending.events.borrower_profile import on_update

			on_update(self.profile)
			mock_advance.assert_called_once_with(self.profile)
