frappe.ui.form.on("Payday Calendar", {
	refresh(frm) {
		if (!frm.is_new()) {
			frm.set_intro(
				__("This record is used by the loan rules engine to resolve payday dates for Loan Applications."),
				"blue"
			);
		}
	},
});
