frappe.ui.form.on("Loan Appraisal", {
	refresh(frm) {
		const rec_color = { Approve: "green", Review: "orange", Decline: "red" };
		if (frm.doc.recommendation) {
			frm.set_intro(
				__("Recommendation: {0}", [frm.doc.recommendation]),
				rec_color[frm.doc.recommendation] || "blue"
			);
		}

		// Show score breakdown as a progress bar hint
		if (frm.doc.appraisal_score) {
			const pct = Math.min(100, frm.doc.appraisal_score);
			frm.set_df_property(
				"appraisal_score",
				"description",
				`Score: ${pct}/100 — ${pct >= 70 ? "✓ Approve threshold met" : "✗ Below approve threshold (70)"}`
			);
		}

		if (!frm.is_new() && frm.doc.loan_application) {
			frm.add_custom_button(__("View Loan Application"), () => {
				frappe.set_route("Form", "Loan Application", frm.doc.loan_application);
			});
		}
	},

	net_salary(frm) {
		_recompute_eligibility(frm);
	},

	existing_liabilities(frm) {
		_recompute_eligibility(frm);
	},

	requested_amount(frm) {
		_recompute_eligibility(frm);
	},
});

function _recompute_eligibility(frm) {
	const net = frm.doc.net_salary || 0;
	const liabilities = frm.doc.existing_liabilities || 0;
	const requested = frm.doc.requested_amount || 0;
	const max_eligible = Math.max(0, net * 0.5 - liabilities);
	frm.set_value("max_eligible_amount", max_eligible);
	frm.set_value("within_limit", requested <= max_eligible ? 1 : 0);
}
