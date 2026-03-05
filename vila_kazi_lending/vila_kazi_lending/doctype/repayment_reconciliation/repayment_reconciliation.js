frappe.ui.form.on("Repayment Reconciliation", {
	refresh(frm) {
		const status_color = {
			Expected: "blue",
			Received: "green",
			Partial: "orange",
			Overdue: "red",
			Waived: "grey",
		};
		if (frm.doc.status) {
			frm.set_indicator_formatter("status", () => status_color[frm.doc.status] || "grey");
		}

		if (!frm.is_new() && frm.doc.loan) {
			frm.add_custom_button(__("View Loan"), () => {
				frappe.set_route("Form", "Loan", frm.doc.loan);
			});
		}

		frm.trigger("toggle_collections_fields");
		frm.trigger("show_collections_actions");
	},

	status(frm) {
		frm.trigger("toggle_collections_fields");
		frm.trigger("show_collections_actions");
	},

	received_amount(frm) {
		_recompute(frm);
	},

	expected_amount(frm) {
		_recompute(frm);
	},

	// ---------------------------------------------------------------------------
	// Field visibility
	// ---------------------------------------------------------------------------

	toggle_collections_fields(frm) {
		const isCollections = ["Overdue", "Collections Active"].includes(frm.doc.status)
			|| frm.doc.vk_collections_stage === "Written Off";
		frm.toggle_display("vk_write_off_reason", isCollections);
		frm.toggle_display("vk_contact_log", isCollections);
	},

	// ---------------------------------------------------------------------------
	// Collections action buttons
	// ---------------------------------------------------------------------------

	show_collections_actions(frm) {
		if (!["Overdue", "Collections Active"].includes(frm.doc.status)) return;

		const isManager = frappe.user.has_role("Lender Manager");
		const isStaff = frappe.user.has_role("Lender Staff");
		if (!isManager && !isStaff) return;

		// --- Log Contact Attempt ---
		frm.add_custom_button(__("Log Contact Attempt"), () => {
			frappe.prompt(
				[
					{
						fieldname: "contact_date",
						fieldtype: "Date",
						label: __("Date"),
						reqd: 1,
						default: frappe.datetime.get_today(),
					},
					{
						fieldname: "channel",
						fieldtype: "Select",
						label: __("Channel"),
						options: "Call\nWhatsApp\nEmail",
						reqd: 1,
					},
					{
						fieldname: "outcome",
						fieldtype: "Small Text",
						label: __("Outcome / Notes"),
						reqd: 1,
					},
					{
						fieldname: "next_followup",
						fieldtype: "Date",
						label: __("Next Follow-Up Date"),
					},
				],
				(v) => frm.call({
					method: "log_contact_attempt",
					args: {
						contact_date: v.contact_date,
						channel: v.channel,
						outcome: v.outcome,
						next_followup: v.next_followup || "",
					},
					callback() {
						frappe.show_alert({ message: __("Contact attempt logged."), indicator: "green" });
						frm.reload_doc();
					},
				}),
				__("Log Contact Attempt")
			);
		}, __("Collections"));

		// --- Set Collections Active (Overdue only, Lender Manager) ---
		if (frm.doc.status === "Overdue" && isManager) {
			frm.add_custom_button(__("Set Collections Active"), () => {
				frappe.confirm(
					__("Mark this loan as actively in collections?"),
					() => frm.call({
						method: "set_collections_active",
						callback() { frm.reload_doc(); },
					})
				);
			}, __("Collections"));
		}

		// --- Initiate Refinancing ---
		frm.add_custom_button(__("Initiate Refinancing"), () => {
			if (!frm.doc.loan) {
				frappe.msgprint(__("No linked loan found on this record."));
				return;
			}
			frappe.new_doc("Loan Application", {
				applicant: frm.doc.borrower,
				vk_is_refinancing: 1,
				vk_refinancing_of_loan: frm.doc.loan,
			});
		}, __("Collections"));

		// --- Write Off (Lender Manager only) ---
		if (isManager) {
			const outstanding = (frm.doc.expected_amount || 0) - (frm.doc.received_amount || 0);
			frm.add_custom_button(__("Write Off Balance"), () => {
				frappe.prompt(
					{
						fieldname: "reason",
						fieldtype: "Small Text",
						label: __("Write-Off Reason"),
						reqd: 1,
					},
					(v) => {
						frappe.confirm(
							__("Write off KES ") + _vkRrFmt(outstanding) +
							__(" outstanding balance? This cannot be undone."),
							() => frm.call({
								method: "write_off_loan",
								args: { reason: v.reason },
								callback() { frm.reload_doc(); },
							})
						);
					},
					__("Write Off Balance")
				);
			}, __("Collections"));
		}
	},
});

function _recompute(frm) {
	const received = frm.doc.received_amount || 0;
	const expected = frm.doc.expected_amount || 0;
	frm.set_value("variance", received - expected);

	if (received >= expected && expected > 0) {
		frm.set_value("status", "Received");
	} else if (received > 0 && received < expected) {
		frm.set_value("status", "Partial");
	}
}

function _vkRrFmt(val) {
	if (!val) return "0.00";
	return parseFloat(val).toLocaleString("en-KE", { minimumFractionDigits: 2 });
}

