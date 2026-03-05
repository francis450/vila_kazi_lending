frappe.ui.form.on("M-Pesa Statement", {
	refresh(frm) {
		const status_color = { Pending: "orange", Parsed: "green", Failed: "red" };
		if (frm.doc.parse_status) {
			frm.set_intro(
				__("Parse Status: {0}", [frm.doc.parse_status]),
				status_color[frm.doc.parse_status] || "blue"
			);
		}

		if (frm.doc.parse_status === "Failed") {
			frm.set_df_property("parse_error_log", "hidden", 0);
		}

		if (!frm.is_new() && frm.doc.parse_status === "Parsed") {
			frm.add_custom_button(__("View Appraisals"), () => {
				frappe.set_route("List", "Loan Appraisal", {
					mpesa_statement: frm.doc.name,
				});
			});
		}
	},

	statement_file(frm) {
		if (frm.doc.statement_file && !frm.is_new()) {
			frappe.show_alert({
				message: __("Statement file uploaded. Parse job will run automatically on save."),
				indicator: "blue",
			});
		}
	},
});
