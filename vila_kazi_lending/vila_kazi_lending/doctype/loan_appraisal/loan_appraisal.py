import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime


class LoanAppraisal(Document):
	def before_save(self):
		self._compute_eligibility()

	def _compute_eligibility(self):
		"""
		Compute max_eligible_amount and within_limit.
		Formula: (net_salary × 0.50) − existing_liabilities
		"""
		net_salary = self.net_salary or 0.0
		existing_liabilities = self.existing_liabilities or 0.0
		requested_amount = self.requested_amount or 0.0

		self.max_eligible_amount = max(0.0, (net_salary * 0.50) - existing_liabilities)
		self.within_limit = 1 if requested_amount <= self.max_eligible_amount else 0

	def set_ai_results(
		self,
		appraisal_score,
		sub_scores: dict,
		recommendation,
		risk_flags,
		ai_summary,
		auto_approved,
	):
		"""
		Called by the AI scoring engine (background job) after processing the
		M-Pesa statement. Updates all scoring fields and saves.

		sub_scores keys: salary_regularity, cashflow_trend, competing_loan,
		                 payday_behavior, gambling, request_ratio
		"""
		self.appraisal_score = appraisal_score
		self.salary_regularity_score = sub_scores.get("salary_regularity", 0.0)
		self.cashflow_trend_score = sub_scores.get("cashflow_trend", 0.0)
		self.competing_loan_score = sub_scores.get("competing_loan", 0.0)
		self.payday_behavior_score = sub_scores.get("payday_behavior", 0.0)
		self.gambling_score = sub_scores.get("gambling", 0.0)
		self.request_ratio_score = sub_scores.get("request_ratio", 0.0)
		self.recommendation = recommendation
		self.risk_flags = risk_flags
		self.ai_summary = ai_summary
		self.auto_approved = 1 if auto_approved else 0
		self.appraisal_date = now_datetime()
		self.save(ignore_permissions=True)
		# Note: the on_update event in events/loan_appraisal.py will pick up the
		# recommendation change, apply hard/soft rules, and advance vk_loan_stage
		# on the linked Loan Application. No direct set_value needed here.
