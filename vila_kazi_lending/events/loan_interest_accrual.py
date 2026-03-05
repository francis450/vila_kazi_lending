"""
Event handlers for the Loan Interest Accrual doctype.

Enforces the Kenyan legal accrual cap: total interest accrued for a loan
must not exceed principal × 2 (stored in Loan.vk_accrual_cap).
"""

import frappe
from frappe import _


def before_insert(doc, method=None):
	"""
	Fired before a new Loan Interest Accrual record is inserted.

	Checks whether adding this accrual would breach the vk_accrual_cap
	on the parent Loan.  If it would, the accrual amount is trimmed to
	the remaining headroom and a warning is shown.  If the cap is already
	exhausted, the insert is blocked entirely.
	"""
	if not doc.loan:
		return

	accrual_cap = frappe.db.get_value("Loan", doc.loan, "vk_accrual_cap")
	if not accrual_cap:
		return  # No cap configured — standard lending engine rules apply

	# Sum all previously posted interest accruals for this loan
	already_accrued = frappe.db.sql(
		"""
		SELECT COALESCE(SUM(interest_amount), 0)
		FROM `tabLoan Interest Accrual`
		WHERE loan = %s
		  AND docstatus = 1
		""",
		doc.loan,
	)[0][0] or 0.0

	remaining = accrual_cap - already_accrued

	if remaining <= 0:
		frappe.throw(
			_(
				"Interest accrual blocked for Loan {0}. The legal accrual cap of {1} "
				"(principal × 2) has already been reached."
			).format(doc.loan, frappe.format(accrual_cap, {"fieldtype": "Currency"})),
			title=_("Accrual Cap Reached"),
		)

	if doc.interest_amount > remaining:
		original = doc.interest_amount
		doc.interest_amount = remaining
		frappe.msgprint(
			_(
				"Interest amount trimmed from {0} to {1} for Loan {2} to stay within "
				"the legal accrual cap."
			).format(
				frappe.format(original, {"fieldtype": "Currency"}),
				frappe.format(remaining, {"fieldtype": "Currency"}),
				doc.loan,
			),
			indicator="orange",
			alert=True,
		)
