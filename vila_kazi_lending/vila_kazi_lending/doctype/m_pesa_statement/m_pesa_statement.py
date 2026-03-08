import frappe
from frappe.model.document import Document


GAMBLING_MERCHANTS = frozenset(
	["sportpesa", "odibets", "betika", "betway", "mozzart"]
)


class MPesaStatement(Document):
	def on_update(self):
		if self.has_value_changed("statement_file") and self.statement_file:
			# Reset parse status before queuing so the background job starts clean
			if self.parse_status != "Pending":
				self.db_set("parse_status", "Pending")
			frappe.enqueue(
				"vila_kazi_lending.tasks.parse_mpesa_statement",
				doc_name=self.name,
				queue="long",
				# enqueue_after_commit=True prevents the race condition where the
				# background worker picks up the job before this transaction commits
				# and frappe.get_doc() fails with "not found".
				enqueue_after_commit=True,
				now=frappe.flags.in_test,
			)
