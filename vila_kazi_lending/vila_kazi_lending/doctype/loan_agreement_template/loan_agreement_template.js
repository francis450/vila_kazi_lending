frappe.ui.form.on("Loan Agreement Template", {
	refresh(frm) {
		if (frm.doc.is_current) {
			frm.set_intro(__("This is the current active template. All new Framework Agreements will use this version."), "green");
		}
		if (!frm.is_new()) {
			frm.add_custom_button(__("View Agreements"), () => {
				frappe.set_route("List", "Loan Framework Agreement", {
					agreement_template: frm.doc.name,
				});
			});
		}
	},

	is_current(frm) {
		if (frm.doc.is_current) {
			frappe.show_alert({
				message: __("This template will be set as current. All other templates will be unmarked on save."),
				indicator: "orange",
			});
		}
	},
});
