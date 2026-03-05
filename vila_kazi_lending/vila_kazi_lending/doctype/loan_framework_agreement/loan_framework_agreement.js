frappe.ui.form.on("Loan Framework Agreement", {
	refresh(frm) {
		const status_color = {
			"Pending Signature": "orange",
			"Active": "green",
			"Expired": "grey",
			"Revoked": "red",
		};
		frm.set_indicator_formatter("status", (doc) => status_color[doc.status] || "grey");

		if (!frm.is_new() && frm.doc.status === "Active") {
			frm.add_custom_button(__("Revoke Agreement"), () => {
				frappe.prompt(
					{ fieldname: "reason", fieldtype: "Small Text", label: "Reason", reqd: 1 },
					(values) => {
						frm.set_value("status", "Revoked");
						frm.set_value("revoked_by", frappe.session.user);
						frm.set_value("revocation_reason", values.reason);
						frm.save();
					},
					__("Revoke Agreement"),
					__("Revoke")
				);
			}, __("Actions"));
		}

		// Indicate that uploading signed_document will auto-activate
		if (frm.doc.status === "Pending Signature") {
			frm.set_df_property(
				"signed_document",
				"description",
				"Upload the signed document here. The agreement will be activated automatically upon save."
			);
		}
	},
});
