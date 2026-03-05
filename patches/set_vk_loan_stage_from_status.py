"""
Migration patch: set vk_loan_stage from existing core status field.

Runs automatically on `bench --site <site> migrate` via patches.txt.
Safe to run multiple times (idempotent — only touches records where
vk_loan_stage is blank or null).

Mapping:
  status=Open      → Draft
  status=Approved  + no linked Loan              → Approved
  status=Approved  + linked Loan, RR Expected/Partial  → Pending Disbursement  (Active)
  status=Approved  + linked Loan, RR Received          → Active / Repaid
  status=Rejected  → Declined
  anything else    → Draft
"""

from __future__ import annotations

import frappe


def execute():
	"""Entry point called by bench migrate."""
	apps_without_stage = frappe.db.sql(
		"""
		SELECT name, status, applicant
		FROM `tabLoan Application`
		WHERE (vk_loan_stage IS NULL OR vk_loan_stage = '')
		  AND docstatus IN (0, 1, 2)
		""",
		as_dict=True,
	)

	if not apps_without_stage:
		frappe.logger("vila_kazi_lending").info(
			"[patch] set_vk_loan_stage_from_status: no records to migrate."
		)
		return

	frappe.logger("vila_kazi_lending").info(
		f"[patch] set_vk_loan_stage_from_status: migrating {len(apps_without_stage)} records."
	)

	for app in apps_without_stage:
		stage = _resolve_stage(app)
		frappe.db.set_value(
			"Loan Application",
			app.name,
			"vk_loan_stage",
			stage,
			update_modified=False,
		)

	frappe.db.commit()
	frappe.logger("vila_kazi_lending").info(
		f"[patch] set_vk_loan_stage_from_status: done."
	)


def _resolve_stage(app: dict) -> str:
	status = (app.get("status") or "").strip()

	if status == "Rejected":
		return "Declined"

	if status != "Approved":
		# Open, or any unexpected value
		return "Draft"

	# Approved — check if a Loan was created
	loan_name = frappe.db.get_value(
		"Loan",
		{"loan_application": app.name, "docstatus": 1},
		"name",
		order_by="creation desc",
	)
	if not loan_name:
		return "Approved"

	# Loan exists — check Repayment Reconciliation status
	rr_status = frappe.db.get_value(
		"Repayment Reconciliation", {"loan": loan_name}, "status"
	)
	if rr_status == "Received":
		return "Repaid"
	if rr_status in ("Expected", "Partial", "Overdue"):
		return "Active"

	# Loan disbursed but no RR yet (edge case)
	return "Pending Disbursement"
