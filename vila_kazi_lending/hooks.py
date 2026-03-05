app_name = "vila_kazi_lending"
app_title = "Vila Kazi Lending"
app_publisher = "ERP Kenya"
app_description = "All customizations for Vila Kazi lending"
app_email = "erpsolutionskenya@gmail.com"
app_license = "mit"

# ---------------------------------------------------------------------------
# Fixtures — version-controlled Custom Fields on Loan Application and Loan
# ---------------------------------------------------------------------------

fixtures = [
	{
		"dt": "Custom Field",
		"filters": [["name", "like", "Loan Application-vk_%"]],
	},
	{
		"dt": "Custom Field",
		"filters": [["name", "like", "Loan-vk_%"]],
	},
	{
		"dt": "Custom Field",
		"filters": [["name", "like", "Repayment Reconciliation-vk_%"]],
	},
	{
		"dt": "Workflow State",
		"filters": [["name", "in", [
			"None", "Collections Active", "Written Off",
			"Draft", "Intake", "Refinancing Requested", "Pending KYC Verification",
			"Gate Check", "Lender Review", "Pending Appraisal", "Standard Review",
			"Appraisal Complete", "Review Required", "Pending Lender Confirm",
			"Refinancing Approved", "New Loan Calculation", "Approved", "Declined",
			"Refinancing Declined", "Duplicate - Review", "Pending Agreement Signing",
			"Agreement Signed", "Confirmed for Disbursement", "Pending Disbursement",
			"Disbursed", "Active", "Repaid"
		]]],
	},
	{
		"dt": "Workflow Action Master",
		"filters": [["name", "in", [
			"Submit for KYC", "Mark KYC Verified", "Mark KYC Rejected",
			"Approve", "Decline", "Override Approve", "Confirm",
			"Approve Refinancing", "Mark Agreement Signed",
			"Confirm Disbursement", "Set Collections Active", "Write Off",
			"Complete Appraisal", "Send Agreement", "Request Disbursement",
			"Set Active", "Mark Repaid",
			"Initiate Disbursement", "Route to Standard Review", "Flag for Review",
			"Send for Lender Confirmation", "Request Refinancing",
			"Submit for Lender Review", "Calculate New Loan", "Submit Calculation",
			"Submit Application", "Pass Gate Check", "Flag as Duplicate", "Resolve Duplicate"
		]]],
	},
	{
		"dt": "Workflow",
		"filters": [["name", "in", ["VK Loan Application", "VK Collections"]]],
	},
	{
		"dt": "Notification",
		"filters": [["name", "like", "VK-%"]],
	},
	{
		"dt": "Role",
		"filters": [["name", "in", ["Lender Manager", "Lender Staff"]]],
	},
]

# ---------------------------------------------------------------------------
# Document Events
# ---------------------------------------------------------------------------

doc_events = {
	# ------------------------------------------------------------------
	# Loan Application — on_submit + on_update (stage-change notifications)
	# ------------------------------------------------------------------
	"Loan Application": {
		"on_submit": "vila_kazi_lending.events.loan_application.on_submit",
		"on_update": "vila_kazi_lending.events.loan_application.on_update",
	},
	# ------------------------------------------------------------------
	# Borrower Profile — on_update
	# KYC status transitions → advance linked Loan Applications
	# ------------------------------------------------------------------
	"Borrower Profile": {
		"on_update": "vila_kazi_lending.events.borrower_profile.on_update",
	},
	# ------------------------------------------------------------------
	# Loan Appraisal — on_update
	# Evaluates hard/soft rules and advances Loan Application stage
	# ------------------------------------------------------------------
	"Loan Appraisal": {
		"on_update": "vila_kazi_lending.events.loan_appraisal.on_update",
	},
	# ------------------------------------------------------------------
	# Loan Disbursement Source — on_update
	# Post-disbursement: activate loan, close original (refinancing), send N-08
	# ------------------------------------------------------------------
	"Loan Disbursement Source": {
		"on_update": "vila_kazi_lending.events.loan_disbursement_source.on_update",
	},
	# ------------------------------------------------------------------
	# Loan — on_submit
	# Sets vk_accrual_cap = loan_amount × 2 and creates Repayment Reconciliation
	# ------------------------------------------------------------------
	"Loan": {
		"on_submit": "vila_kazi_lending.events.loan.on_submit",
	},
	# ------------------------------------------------------------------
	# Loan Interest Accrual — before_insert
	# Enforces the Kenyan legal accrual cap (principal × 2)
	# ------------------------------------------------------------------
	"Loan Interest Accrual": {
		"before_insert": "vila_kazi_lending.events.loan_interest_accrual.before_insert",
	},
}

# ---------------------------------------------------------------------------
# Scheduled Tasks
# ---------------------------------------------------------------------------

scheduler_events = {
	"daily": [
		"vila_kazi_lending.tasks.mark_overdue_repayments",
		"vila_kazi_lending.tasks.send_pre_due_reminders",
	],
}

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "vila_kazi_lending",
# 		"logo": "/assets/vila_kazi_lending/logo.png",
# 		"title": "Vila Kazi Lending",
# 		"route": "/vila_kazi_lending",
# 		"has_permission": "vila_kazi_lending.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/vila_kazi_lending/css/vila_kazi_lending.css"
# app_include_js = "/assets/vila_kazi_lending/js/vila_kazi_lending.js"

# include js, css files in header of web template
# web_include_css = "/assets/vila_kazi_lending/css/vila_kazi_lending.css"
# web_include_js = "/assets/vila_kazi_lending/js/vila_kazi_lending.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "vila_kazi_lending/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
doctype_js = {"Loan Application": "public/js/loan_application.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "vila_kazi_lending/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "vila_kazi_lending.utils.jinja_methods",
# 	"filters": "vila_kazi_lending.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "vila_kazi_lending.install.before_install"
# after_install = "vila_kazi_lending.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "vila_kazi_lending.uninstall.before_uninstall"
# after_uninstall = "vila_kazi_lending.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "vila_kazi_lending.utils.before_app_install"
# after_app_install = "vila_kazi_lending.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "vila_kazi_lending.utils.before_app_uninstall"
# after_app_uninstall = "vila_kazi_lending.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "vila_kazi_lending.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"vila_kazi_lending.tasks.all"
# 	],
# 	"daily": [
# 		"vila_kazi_lending.tasks.daily"
# 	],
# 	"hourly": [
# 		"vila_kazi_lending.tasks.hourly"
# 	],
# 	"weekly": [
# 		"vila_kazi_lending.tasks.weekly"
# 	],
# 	"monthly": [
# 		"vila_kazi_lending.tasks.monthly"
# 	],
# }

# Testing
# -------

# before_tests = "vila_kazi_lending.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "vila_kazi_lending.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "vila_kazi_lending.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["vila_kazi_lending.utils.before_request"]
# after_request = ["vila_kazi_lending.utils.after_request"]

# Job Events
# ----------
# before_job = ["vila_kazi_lending.utils.before_job"]
# after_job = ["vila_kazi_lending.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"vila_kazi_lending.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

# Translation
# ------------
# List of apps whose translatable strings should be excluded from this app's translations.
# ignore_translatable_strings_from = []

