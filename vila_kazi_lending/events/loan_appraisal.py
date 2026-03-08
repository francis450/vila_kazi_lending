"""
Event handlers for the Loan Appraisal doctype.
"""

from __future__ import annotations

import frappe
from frappe import _


def on_update(doc, method=None):
	"""
	Fires on every save of Loan Appraisal.
	When recommendation is set, evaluates hard/soft rules and advances the Loan Application.
	"""
	if not doc.has_value_changed("recommendation") or not doc.recommendation:
		return

	_evaluate_and_route(doc)


# ---------------------------------------------------------------------------


def _evaluate_and_route(doc):
	"""
	Apply hard and soft rules on top of the AI recommendation:
	  Hard rules → force Decline regardless of score
	  Soft rules → escalate to Review Required
	Then advance the linked Loan Application stage.
	"""
	final_recommendation = doc.recommendation  # "Approve" | "Review" | "Decline"

	# --- Hard rules (force Decline) ---
	hard_decline_reasons = []

	if (doc.requested_amount or 0) > (doc.max_eligible_amount or 0):
		hard_decline_reasons.append("Requested amount exceeds 50% net salary eligibility limit.")

	if doc.mpesa_statement:
		stmt = frappe.db.get_value(
			"M-Pesa Statement",
			doc.mpesa_statement,
			["gambling_total", "monthly_avg_inflow"],
			as_dict=True,
		)
		if stmt and (stmt.monthly_avg_inflow or 0) > 0:
			gambling_ratio = (stmt.gambling_total or 0) / stmt.monthly_avg_inflow
			if gambling_ratio > 0.10:
				hard_decline_reasons.append(
					f"Gambling transactions exceed 10% of monthly inflow ({gambling_ratio * 100:.1f}%)."
				)

	if hard_decline_reasons:
		final_recommendation = "Decline"
		risk_note = " | ".join(hard_decline_reasons)
		frappe.db.set_value(
			"Loan Appraisal",
			doc.name,
			"risk_flags",
			(doc.risk_flags or "") + f"\n[HARD RULE] {risk_note}",
		)

	# --- Soft rules (escalate to Review Required) ---
	if final_recommendation == "Approve" and doc.mpesa_statement:
		soft_triggers = []
		stmt = frappe.db.get_value(
			"M-Pesa Statement",
			doc.mpesa_statement,
			["loan_repayments_detected", "monthly_avg_inflow", "net_cashflow_trend"],
			as_dict=True,
		)
		if stmt:
			if (stmt.monthly_avg_inflow or 0) > 0:
				competing_ratio = (stmt.loan_repayments_detected or 0) / stmt.monthly_avg_inflow
				if competing_ratio > 0.30:
					soft_triggers.append(
						f"Competing loan repayments exceed 30% of income ({competing_ratio * 100:.1f}%)."
					)
			if stmt.net_cashflow_trend == "Declining":
				soft_triggers.append("Cashflow trend is Declining.")

		if soft_triggers:
			final_recommendation = "Review"
			risk_note = " | ".join(soft_triggers)
			frappe.db.set_value(
				"Loan Appraisal",
				doc.name,
				"risk_flags",
				(doc.risk_flags or "") + f"\n[SOFT RULE] {risk_note}",
			)

	# --- Advance the Loan Application ---
	if not doc.loan_application:
		return

	if final_recommendation == "Decline":
		frappe.db.set_value("Loan Application", doc.loan_application, "vk_loan_stage", "Declined")
		# N-05 is now sent via VK-N05 Frappe Notification on vk_loan_stage change.

	elif final_recommendation == "Review":
		frappe.db.set_value(
			"Loan Application", doc.loan_application, "vk_loan_stage", "Review Required"
		)

	elif final_recommendation == "Approve":
		if doc.auto_approved:
			# Fast-lane: gate already passed, go straight to Pending Lender Confirm
			frappe.db.set_value(
				"Loan Application",
				doc.loan_application,
				"vk_loan_stage",
				"Pending Lender Confirm",
			)
			# N-16 is now sent via VK-N16 Frappe Notification on vk_loan_stage change.
		else:
			frappe.db.set_value(
				"Loan Application", doc.loan_application, "vk_loan_stage", "Appraisal Complete"
			)


# N-05 and N-16 emails are now sent via Frappe Notification fixtures VK-N05 and VK-N16.
# They trigger on vk_loan_stage value changes on the Loan Application doctype.
