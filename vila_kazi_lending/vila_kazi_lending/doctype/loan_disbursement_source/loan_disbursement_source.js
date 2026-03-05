frappe.ui.form.on("Loan Disbursement Source", {
	refresh(frm) {
		const status_color = { Sent: "orange", Confirmed: "green", Failed: "red" };
		if (frm.doc.status) {
			frm.set_intro(__("Disbursement Status: {0}", [frm.doc.status]), status_color[frm.doc.status]);
		}

		if (!frm.is_new() && frm.doc.loan) {
			frm.add_custom_button(__("View Loan"), () => {
				frappe.set_route("Form", "Loan", frm.doc.loan);
			});

			frm.add_custom_button(__("All Disbursements for this Loan"), () => {
				frappe.set_route("List", "Loan Disbursement Source", { loan: frm.doc.loan });
			});
		}
	},
});
