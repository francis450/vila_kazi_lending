import frappe
from frappe import _
from frappe.model.document import Document


class PaydayCalendar(Document):
	def validate(self):
		if not (1 <= (self.payday_day or 0) <= 31):
			frappe.throw(_("Payday Day must be between 1 and 31."), title=_("Invalid Payday Day"))
