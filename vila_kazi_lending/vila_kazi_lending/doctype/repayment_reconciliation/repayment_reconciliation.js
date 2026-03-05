frappe.ui.form.on("Repayment Reconciliation", {
	refresh(frm) {
		const status_color = {
			Expected:  "blue",
			Received:  "green",
			Partial:   "orange",
			Overdue:   "red",
			Waived:    "grey",
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
		frm.trigger("render_collections_buttons");
	},

	status(frm) {
		frm.trigger("toggle_collections_fields");
	},

	vk_collections_stage(frm) {
		frm.trigger("toggle_collections_fields");
		frm.trigger("render_collections_buttons");
	},

	received_amount(frm) {
		_recompute(frm);
	},

	expected_amount(frm) {
		_recompute(frm);
	},

	// ---------------------------------------------------------------------------
	// Field visibility — show collections fields only when relevant
	// ---------------------------------------------------------------------------

	toggle_collections_fields(frm) {
		const stage = frm.doc.vk_collections_stage || "";
		const activeStages = new Set([
			"Pending Review", "Collections Active", "Partially Paid",
			"Promise to Pay", "Escalated",
		]);
		const inCollections = activeStages.has(stage) || stage === "Written Off";

		frm.toggle_display("vk_promise_date",       ["Collections Active", "Partially Paid", "Promise to Pay"].includes(stage));
		frm.toggle_display("vk_promise_amount",     ["Collections Active", "Partially Paid", "Promise to Pay"].includes(stage));
		frm.toggle_display("vk_escalation_reason",  ["Escalated", "Collections Active"].includes(stage));
		frm.toggle_display("vk_recovery_date",      stage === "Recovered");
		frm.toggle_display("vk_recovery_amount",    stage === "Recovered");
		frm.toggle_display("vk_recovery_notes",     stage === "Recovered");
		frm.toggle_display("vk_write_off_reason",   stage === "Written Off");
		frm.toggle_display("vk_contact_log",        inCollections);
	},

	// ---------------------------------------------------------------------------
	// Collections action buttons — stage-aware
	// ---------------------------------------------------------------------------

	render_collections_buttons(frm) {
		if (frm.is_new()) return;

		const stage = frm.doc.vk_collections_stage || "";
		const isManager = frappe.user.has_role("Lender Manager");
		const isStaff   = frappe.user.has_role("Lender Staff");
		const canAct    = isManager || isStaff;

		const TERMINAL = new Set(["Paid", "Recovered", "Written Off"]);
		if (TERMINAL.has(stage)) return; // No actions on terminal records

		const grp = __("Collections");

		// --- Log Contact Attempt: available in all active stages ---
		const ACTIVE = new Set([
			"Pending Review", "Collections Active", "Partially Paid",
			"Promise to Pay", "Escalated",
		]);
		if (ACTIVE.has(stage) && canAct) {
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
							options: "Call\nWhatsApp\nEmail\nSMS\nIn Person",
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
							contact_date:  v.contact_date,
							channel:       v.channel,
							outcome:       v.outcome,
							next_followup: v.next_followup || "",
						},
						callback() {
							frappe.show_alert({ message: __("Contact attempt logged."), indicator: "green" });
							frm.reload_doc();
						},
					}),
					__("Log Contact Attempt")
				);
			}, grp);
		}

		// --- Pending Review ---
		if (stage === "Pending Review" && canAct) {
			frm.add_custom_button(__("Activate Collections"), () => {
				frappe.confirm(
					__("Move this record into active collections?"),
					() => frm.call({
						method: "activate_collections",
						callback() { frm.reload_doc(); },
					})
				);
			}, grp);
		}

		// --- Collections Active ---
		if (stage === "Collections Active" && canAct) {
			// Log Partial Payment
			frm.add_custom_button(__("Log Partial Payment"), () => {
				frappe.prompt(
					[
						{
							fieldname: "received_amount",
							fieldtype: "Currency",
							label: __("Received Amount"),
							reqd: 1,
						},
						{
							fieldname: "received_date",
							fieldtype: "Date",
							label: __("Received Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "payment_reference",
							fieldtype: "Data",
							label: __("Payment Reference (M-Pesa / Bank)"),
						},
					],
					(v) => frm.call({
						method: "mark_partial_payment",
						args: {
							received_amount:    v.received_amount,
							received_date:      v.received_date,
							payment_reference:  v.payment_reference || "",
						},
						callback() { frm.reload_doc(); },
					}),
					__("Log Partial Payment")
				);
			}, grp);

			// Log Promise to Pay
			frm.add_custom_button(__("Log Promise to Pay"), () => {
				frappe.prompt(
					[
						{
							fieldname: "promise_date",
							fieldtype: "Date",
							label: __("Promise-to-Pay Date"),
							reqd: 1,
						},
						{
							fieldname: "promise_amount",
							fieldtype: "Currency",
							label: __("Promised Amount"),
							reqd: 1,
							default: (frm.doc.expected_amount || 0) - (frm.doc.received_amount || 0),
						},
						{
							fieldname: "notes",
							fieldtype: "Small Text",
							label: __("Notes"),
						},
					],
					(v) => frm.call({
						method: "log_promise_to_pay",
						args: {
							promise_date:   v.promise_date,
							promise_amount: v.promise_amount,
							notes:          v.notes || "",
						},
						callback() { frm.reload_doc(); },
					}),
					__("Log Promise to Pay")
				);
			}, grp);

			// Mark Paid
			frm.add_custom_button(__("Mark Paid"), () => {
				frappe.prompt(
					[
						{
							fieldname: "received_amount",
							fieldtype: "Currency",
							label: __("Received Amount"),
							reqd: 1,
							default: frm.doc.expected_amount || 0,
						},
						{
							fieldname: "received_date",
							fieldtype: "Date",
							label: __("Received Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "payment_reference",
							fieldtype: "Data",
							label: __("Payment Reference"),
						},
					],
					(v) => frappe.confirm(
						__("Mark this loan as fully paid?"),
						() => frm.call({
							method: "mark_paid",
							args: {
								received_amount:   v.received_amount,
								received_date:     v.received_date,
								payment_reference: v.payment_reference || "",
							},
							callback() { frm.reload_doc(); },
						})
					),
					__("Mark Paid")
				);
			}, grp);

			// Initiate Refinancing
			frm.add_custom_button(__("Initiate Refinancing"), () => {
				if (!frm.doc.loan) {
					frappe.msgprint(__("No linked loan found on this record."));
					return;
				}
				frappe.new_doc("Loan Application", {
					applicant:              frm.doc.borrower,
					vk_is_refinancing:      1,
					vk_refinancing_of_loan: frm.doc.loan,
				});
			}, grp);

			// Escalate — Lender Manager only
			if (isManager) {
				frm.add_custom_button(__("Escalate"), () => {
					frappe.prompt(
						{
							fieldname: "reason",
							fieldtype: "Small Text",
							label: __("Escalation Reason"),
							reqd: 1,
						},
						(v) => frappe.confirm(
							__("Escalate this loan to formal recovery? This will notify the lender team."),
							() => frm.call({
								method: "escalate",
								args: { reason: v.reason },
								callback() { frm.reload_doc(); },
							})
						),
						__("Escalate Loan")
					);
				}, grp);

				// Write Off
				_add_write_off_button(frm, grp);
			}
		}

		// --- Partially Paid ---
		if (stage === "Partially Paid" && canAct) {
			// Resume Collections
			frm.add_custom_button(__("Resume Collections"), () => {
				frappe.confirm(
					__("Return this record to active collections?"),
					() => frm.call({
						method: "resume_collections",
						callback() { frm.reload_doc(); },
					})
				);
			}, grp);

			// Log Promise to Pay
			frm.add_custom_button(__("Log Promise to Pay"), () => {
				frappe.prompt(
					[
						{
							fieldname: "promise_date",
							fieldtype: "Date",
							label: __("Promise-to-Pay Date"),
							reqd: 1,
						},
						{
							fieldname: "promise_amount",
							fieldtype: "Currency",
							label: __("Promised Amount"),
							reqd: 1,
							default: (frm.doc.expected_amount || 0) - (frm.doc.received_amount || 0),
						},
						{
							fieldname: "notes",
							fieldtype: "Small Text",
							label: __("Notes"),
						},
					],
					(v) => frm.call({
						method: "log_promise_to_pay",
						args: {
							promise_date:   v.promise_date,
							promise_amount: v.promise_amount,
							notes:          v.notes || "",
						},
						callback() { frm.reload_doc(); },
					}),
					__("Log Promise to Pay")
				);
			}, grp);

			// Mark Paid
			frm.add_custom_button(__("Mark Paid"), () => {
				frappe.prompt(
					[
						{
							fieldname: "received_amount",
							fieldtype: "Currency",
							label: __("Received Amount"),
							reqd: 1,
							default: frm.doc.expected_amount || 0,
						},
						{
							fieldname: "received_date",
							fieldtype: "Date",
							label: __("Received Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "payment_reference",
							fieldtype: "Data",
							label: __("Payment Reference"),
						},
					],
					(v) => frappe.confirm(
						__("Mark this loan as fully paid?"),
						() => frm.call({
							method: "mark_paid",
							args: {
								received_amount:   v.received_amount,
								received_date:     v.received_date,
								payment_reference: v.payment_reference || "",
							},
							callback() { frm.reload_doc(); },
						})
					),
					__("Mark Paid")
				);
			}, grp);

			if (isManager) {
				_add_write_off_button(frm, grp);
			}
		}

		// --- Promise to Pay ---
		if (stage === "Promise to Pay" && canAct) {
			// Log Partial Payment (borrower paid less than promised)
			frm.add_custom_button(__("Log Partial Payment"), () => {
				frappe.prompt(
					[
						{
							fieldname: "received_amount",
							fieldtype: "Currency",
							label: __("Received Amount"),
							reqd: 1,
						},
						{
							fieldname: "received_date",
							fieldtype: "Date",
							label: __("Received Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "payment_reference",
							fieldtype: "Data",
							label: __("Payment Reference (M-Pesa / Bank)"),
						},
					],
					(v) => frm.call({
						method: "mark_partial_payment",
						args: {
							received_amount:   v.received_amount,
							received_date:     v.received_date,
							payment_reference: v.payment_reference || "",
						},
						callback() { frm.reload_doc(); },
					}),
					__("Log Partial Payment")
				);
			}, grp);

			// Promise Kept
			frm.add_custom_button(__("Promise Kept"), () => {
				frappe.prompt(
					[
						{
							fieldname: "received_amount",
							fieldtype: "Currency",
							label: __("Received Amount"),
							reqd: 1,
							default: frm.doc.vk_promise_amount || frm.doc.expected_amount || 0,
						},
						{
							fieldname: "received_date",
							fieldtype: "Date",
							label: __("Received Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "payment_reference",
							fieldtype: "Data",
							label: __("Payment Reference"),
						},
					],
					(v) => frm.call({
						method: "promise_kept",
						args: {
							received_amount:   v.received_amount,
							received_date:     v.received_date,
							payment_reference: v.payment_reference || "",
						},
						callback() { frm.reload_doc(); },
					}),
					__("Record Payment — Promise Kept")
				);
			}, grp);

			// Promise Broken
			frm.add_custom_button(__("Promise Broken"), () => {
				frappe.prompt(
					{
						fieldname: "notes",
						fieldtype: "Small Text",
						label: __("Notes"),
					},
					(v) => frappe.confirm(
						__("Mark the promise as broken and return to active collections?"),
						() => frm.call({
							method: "promise_broken",
							args: { notes: v.notes || "" },
							callback() { frm.reload_doc(); },
						})
					),
					__("Promise Broken")
				);
			}, grp);

			if (isManager) {
				// Escalate
				frm.add_custom_button(__("Escalate"), () => {
					frappe.prompt(
						{
							fieldname: "reason",
							fieldtype: "Small Text",
							label: __("Escalation Reason"),
							reqd: 1,
						},
						(v) => frappe.confirm(
							__("Escalate to formal recovery?"),
							() => frm.call({
								method: "escalate",
								args: { reason: v.reason },
								callback() { frm.reload_doc(); },
							})
						),
						__("Escalate Loan")
					);
				}, grp);

				_add_write_off_button(frm, grp);
			}
		}

		// --- Escalated ---
		if (stage === "Escalated" && isManager) {
			// Mark Recovered
			frm.add_custom_button(__("Mark Recovered"), () => {
				frappe.prompt(
					[
						{
							fieldname: "recovery_date",
							fieldtype: "Date",
							label: __("Recovery Date"),
							reqd: 1,
							default: frappe.datetime.get_today(),
						},
						{
							fieldname: "recovery_amount",
							fieldtype: "Currency",
							label: __("Recovered Amount"),
							reqd: 1,
							default: (frm.doc.expected_amount || 0) - (frm.doc.received_amount || 0),
						},
						{
							fieldname: "notes",
							fieldtype: "Small Text",
							label: __("Recovery Notes"),
						},
					],
					(v) => frappe.confirm(
						__("Mark this loan as recovered? This will close the collections case."),
						() => frm.call({
							method: "mark_recovered",
							args: {
								recovery_date:   v.recovery_date,
								recovery_amount: v.recovery_amount,
								notes:           v.notes || "",
							},
							callback() { frm.reload_doc(); },
						})
					),
					__("Mark Recovered")
				);
			}, grp);

			_add_write_off_button(frm, grp);
		}
	},
});

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

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

function _add_write_off_button(frm, grp) {
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
					__("Write off KES ") + _vkFmt(outstanding) +
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
	}, grp);
}

function _vkFmt(val) {
	if (!val) return "0.00";
	return parseFloat(val).toLocaleString("en-KE", { minimumFractionDigits: 2 });
}
