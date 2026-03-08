// Vila Kazi Lending — Loan Application client-side form hooks
// Loaded via doctype_js in hooks.py

frappe.ui.form.on("Loan Application", {
	refresh(frm) {
		frm.trigger("toggle_refinancing_fields");
		frm.trigger("show_workflow_actions");
		frm.trigger("show_confirm_card");
	},

	// ---------------------------------------------------------------------------
	// Live preview — max eligible amount
	// ---------------------------------------------------------------------------

	vk_net_salary(frm) {
		frm.trigger("update_max_eligible_preview");
	},

	vk_existing_liabilities(frm) {
		frm.trigger("update_max_eligible_preview");
	},

	update_max_eligible_preview(frm) {
		const salary = frm.doc.vk_net_salary || 0;
		if (salary <= 0) return;
		frappe.call({
			method: "vila_kazi_lending.api.compute_max_eligible_preview",
			args: {
				net_salary: salary,
				existing_liabilities: frm.doc.vk_existing_liabilities || 0,
			},
			callback(r) {
				if (r.message != null) {
					frm.set_value("vk_max_eligible_amount", r.message);
				}
			},
		});
	},

	// ---------------------------------------------------------------------------
	// Refinancing field visibility
	// ---------------------------------------------------------------------------

	vk_is_refinancing(frm) {
		frm.trigger("toggle_refinancing_fields");
	},

	toggle_refinancing_fields(frm) {
		const show = !!frm.doc.vk_is_refinancing;
		frm.toggle_display("vk_refinancing_of_loan", show);
		frm.toggle_display("vk_top_up_amount", show);
	},

	// ---------------------------------------------------------------------------
	// Workflow action buttons — shown based on vk_loan_stage + user role
	// ---------------------------------------------------------------------------

	show_workflow_actions(frm) {
		frm.clear_custom_buttons();
		const stage = frm.doc.vk_loan_stage;
		const isSysAdmin = frappe.user.has_role("System Manager");
		const isManager = frappe.user.has_role("Lender Manager") || isSysAdmin;
		const isStaff = frappe.user.has_role("Lender Staff") || isSysAdmin;

		// --- Unsaved / draft document (docstatus=0): show Submit Application ---
		if (frm.doc.docstatus === 0 && !frm.doc.__islocal) {
			frm.add_custom_button(__("Submit Application"), () => {
				frappe.confirm(
					__("Submit this loan application and start the review workflow?"),
					() => frm.savesubmit()
				);
			}).css({ "background-color": "#1565C0", "color": "white", "font-weight": "bold" });
			return;
		}

		if (!stage) return;

		// --- WF-01: KYC step ---
		if (stage === "Draft" && (isManager || isStaff)) {
			frm.add_custom_button(__("Submit for KYC"), () => {
				frappe.call({
					method: "vila_kazi_lending.api.set_loan_stage",
					args: { docname: frm.doc.name, stage: "Pending KYC Verification" },
					callback() { frm.reload_doc(); },
				});
			}, __("Workflow"));
		}

		// --- Intake: repeat-borrower holding state (shown when gate check pends/failed) ---
		if (stage === "Intake" && (isManager || isStaff)) {
			frm.add_custom_button(__("Submit for KYC"), () => {
				frappe.confirm(__("Submit this application for KYC verification?"), () =>
					frappe.call({
						method: "vila_kazi_lending.api.set_loan_stage",
						args: { docname: frm.doc.name, stage: "Pending KYC Verification" },
						callback() { frm.reload_doc(); },
					})
				);
			}).css({ "background-color": "#1565C0", "color": "white", "font-weight": "bold" });
		}

		if (stage === "Pending KYC Verification" && (isManager || isStaff)) {
			frm.add_custom_button(__("Mark KYC Verified"), () => {
				frappe.confirm(__("Confirm KYC documents have been verified?"), () => {
					frappe.call({
						method: "vila_kazi_lending.api.set_loan_stage",
						args: { docname: frm.doc.name, stage: "Pending Appraisal" },
						callback() {
							// Also update Borrower Profile KYC status
							frappe.db.set_value(
								"Borrower Profile",
								{ customer: frm.doc.applicant },
								"kyc_status",
								"Verified"
							).then(() => frm.reload_doc());
						},
					});
				});
			}, __("KYC"));

			frm.add_custom_button(__("Reject KYC"), () => {
				frappe.prompt(
					{ fieldname: "reason", fieldtype: "Small Text", label: __("Rejection Reason"), reqd: 1 },
					(v) => frappe.call({
						method: "vila_kazi_lending.api.reject_kyc",
						args: { docname: frm.doc.name, reason: v.reason },
						callback() { frm.reload_doc(); },
					}),
					__("KYC Rejection Reason")
				);
			}, __("KYC"));
		}

		// --- Appraisal Complete or Standard Review: lender decision ---
		if ((stage === "Appraisal Complete" || stage === "Standard Review") && isManager) {
			frm.add_custom_button(__("Approve"), () => {
				frappe.confirm(__("Approve this loan application?"), () =>
					frappe.call({
						method: "vila_kazi_lending.api.lender_approve",
						args: { docname: frm.doc.name },
						callback() { frm.reload_doc(); },
					})
				);
			}, __("Decision")).css({ "background-color": "#4CAF50", "color": "white" });

			frm.add_custom_button(__("Decline"), () => {
				frappe.prompt(
					{ fieldname: "reason", fieldtype: "Small Text", label: __("Decline Reason"), reqd: 1 },
					(v) => frappe.call({
						method: "vila_kazi_lending.api.lender_decline",
						args: { docname: frm.doc.name, reason: v.reason },
						callback() { frm.reload_doc(); },
					}),
					__("Decline Reason")
				);
			}, __("Decision")).css({ "background-color": "#f44336", "color": "white" });
		}

		// --- Review Required: override path ---
		if (stage === "Review Required" && isManager) {
			frm.add_custom_button(__("Override & Approve"), () => {
				frappe.prompt(
					{
						fieldname: "notes",
						fieldtype: "Small Text",
						label: __("Override Reason (required)"),
						reqd: 1,
					},
					(v) => frappe.call({
						method: "vila_kazi_lending.api.lender_override_approve",
						args: { docname: frm.doc.name, notes: v.notes },
						callback() { frm.reload_doc(); },
					}),
					__("Override Approval")
				);
			}, __("Decision")).css({ "background-color": "#FF9800", "color": "white" });

			frm.add_custom_button(__("Decline"), () => {
				frappe.prompt(
					{ fieldname: "reason", fieldtype: "Small Text", label: __("Decline Reason"), reqd: 1 },
					(v) => frappe.call({
						method: "vila_kazi_lending.api.lender_decline",
						args: { docname: frm.doc.name, reason: v.reason },
						callback() { frm.reload_doc(); },
					}),
					__("Decline Reason")
				);
			}, __("Decision")).css({ "background-color": "#f44336", "color": "white" });
		}

		// --- WF-03: Lender Review for refinancing ---
		if (stage === "Lender Review" && isManager) {
			frm.add_custom_button(__("Approve Refinancing"), () => {
				frappe.confirm(__("Approve refinancing for this borrower?"), () =>
					frappe.call({
						method: "vila_kazi_lending.api.approve_refinancing",
						args: { docname: frm.doc.name },
						callback() { frm.reload_doc(); },
					})
				);
			}, __("Refinancing")).css({ "background-color": "#4CAF50", "color": "white" });

			frm.add_custom_button(__("Decline Refinancing"), () => {
				frappe.prompt(
					{ fieldname: "reason", fieldtype: "Small Text", label: __("Decline Reason"), reqd: 1 },
					(v) => frappe.call({
						method: "vila_kazi_lending.api.lender_decline",
						args: { docname: frm.doc.name, reason: v.reason },
						callback() { frm.reload_doc(); },
					}),
					__("Decline Reason")
				);
			}, __("Refinancing")).css({ "background-color": "#f44336", "color": "white" });
		}
	},

	// ---------------------------------------------------------------------------
	// WF-02 Fast-Lane Confirm Card
	// ---------------------------------------------------------------------------

	show_confirm_card(frm) {
		if (frm.doc.vk_loan_stage !== "Pending Lender Confirm") return;
		if (!frappe.user.has_role("Lender Manager") && !frappe.user.has_role("System Manager")) return;

		// Load borrower profile data for the card
		frappe.call({
			method: "vila_kazi_lending.api.get_confirm_card_data",
			args: { docname: frm.doc.name },
			callback(r) {
				const bp = r.message || {};
				const card = `
					<div style="border:2px solid #4CAF50;border-radius:8px;padding:16px;
					            margin-bottom:12px;background:#f9fff9;">
						<h4 style="color:#2d6a2d;margin-top:0;">⚡ Fast-Lane Confirmation Required</h4>
						<table style="width:100%;border-collapse:collapse;">
							<tr><td style="padding:4px 8px;width:45%;"><b>Borrower</b></td>
							    <td>${frm.doc.applicant_name || frm.doc.applicant}</td></tr>
							<tr><td style="padding:4px 8px;"><b>Credit Category</b></td>
							    <td>${bp.credit_category || "—"}</td></tr>
							<tr><td style="padding:4px 8px;"><b>On-Time Repayment Rate</b></td>
							    <td>${bp.on_time_rate != null ? bp.on_time_rate.toFixed(1) + "%" : "—"}</td></tr>
							<tr><td style="padding:4px 8px;"><b>Requested Amount</b></td>
							    <td>KES ${_vkFmt(frm.doc.loan_amount)}</td></tr>
							<tr><td style="padding:4px 8px;"><b>Max Eligible</b></td>
							    <td>KES ${_vkFmt(frm.doc.vk_max_eligible_amount)}</td></tr>
							<tr><td style="padding:4px 8px;"><b>M-Pesa Number</b></td>
							    <td>${bp.mpesa_number || "—"}</td></tr>
							<tr><td style="padding:4px 8px;"><b>Due Date</b></td>
							    <td>${frm.doc.vk_payday_date || "—"}</td></tr>
						</table>
						<p style="color:#555;font-size:0.9em;margin-top:10px;">
							Review the details above, then Confirm to authorise the transfer
							or Decline to reject this application.
						</p>
					</div>`;
				frm.set_intro(card, false);

				frm.add_custom_button(__("✓ Confirm & Proceed to Disbursement"), () => {
					frappe.confirm(
						__("Confirm disbursement authorisation for this loan?"),
						() => frappe.call({
							method: "vila_kazi_lending.api.lender_confirm_fast_lane",
							args: { docname: frm.doc.name },
							callback() { frm.reload_doc(); },
						})
					);
				}).css({ "background-color": "#4CAF50", "color": "white", "font-weight": "bold" });

				frm.add_custom_button(__("✗ Decline"), () => {
					frappe.prompt(
						{
							fieldname: "reason",
							fieldtype: "Small Text",
							label: __("Decline Reason"),
							reqd: 1,
						},
						(v) => frappe.call({
							method: "vila_kazi_lending.api.lender_decline",
							args: { docname: frm.doc.name, reason: v.reason },
							callback() { frm.reload_doc(); },
						}),
						__("Decline Reason")
					);
				}).css({ "background-color": "#f44336", "color": "white" });
			},
		});
	},
});

function _vkFmt(val) {
	if (!val) return "0.00";
	return parseFloat(val).toLocaleString("en-KE", { minimumFractionDigits: 2 });
}
