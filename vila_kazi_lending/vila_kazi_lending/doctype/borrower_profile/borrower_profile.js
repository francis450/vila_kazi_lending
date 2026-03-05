frappe.ui.form.on("Borrower Profile", {
	refresh(frm) {
		if (frm.doc.kyc_status === "Verified") {
			frm.set_df_property("kyc_status", "description", "✓ KYC verified — borrower is eligible to apply for loans.");
		}
		if (!frm.is_new()) {
			frm.add_custom_button(__("View Loans"), () => {
				frappe.set_route("List", "Loan", { applicant: frm.doc.customer });
			});
		}
	},

	customer(frm) {
		if (frm.doc.customer) {
			frappe.db.get_value("Customer", frm.doc.customer, "customer_name", (r) => {
				if (r && r.customer_name) {
					frm.set_value("employer_name", r.customer_name);
				}
			});
		}
	},
});
