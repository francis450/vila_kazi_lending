"""vila_kazi_lending.mpesa_parser
────────────────────────────────
Parses a Safaricom M-Pesa Full Statement PDF and returns financial metrics
for credit appraisal.

Supported format: the "MPESA FULL STATEMENT" PDF delivered by Safaricom.
Column layout per page:
  Receipt No | Completion Time | Details | Transaction Status | Paid in | Withdrawn | Balance

Returns a dict whose keys map directly to MPesaStatement doctype fields:
  parsed_transactions          – JSON array of transaction dicts
  monthly_avg_inflow           – float (KES)
  monthly_avg_outflow          – float (KES)
  avg_monthly_balance          – float (KES)
  salary_credit_regularity     – float 0–100 (% of months with a salary-like credit)
  loan_repayments_detected     – float (total KES repaid to mobile loans)
  net_cashflow_trend           – "Improving" | "Stable" | "Declining"
  gambling_transactions_detected – 0 | 1
  gambling_total               – float (KES spent on gambling merchants)
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import frappe

try:
	import pypdf
except ImportError:  # pragma: no cover
	pypdf = None  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Keyword sets for classification
# ─────────────────────────────────────────────────────────────────────────────

GAMBLING_KEYWORDS: frozenset[str] = frozenset(
	[
		"sportpesa",
		"odibets",
		"betika",
		"betway",
		"mozzart",
		"betin",
		"betpawa",
		"elitebet",
		"shabiki",
		"supabet",
	]
)

LOAN_REPAYMENT_KEYWORDS: list[str] = [
	"od loan repayment",
	"loan repayment",
	"m-shwari deposit",  # M-Shwari Deposit = savings/credit repayment
	"overdraw repayment",
	"fuliza repayment",
	"okoa repayment",
	"stawi repayment",
	"kopa karo repayment",
]

# Keywords that indicate a regular income / salary credit
SALARY_KEYWORDS: list[str] = [
	"b2c payment",
	"salary",
	"payroll",
	"wages",
	"pay from",
	"fsi withdraw",    # FSI = Mobile savings drawdown (treated as income inflow)
	"reversal",
	"merchant customer payment",
]

# ─────────────────────────────────────────────────────────────────────────────
# Regex patterns
# ─────────────────────────────────────────────────────────────────────────────

# Repeating page-header line (stripped before parsing)
_HEADER_RE = re.compile(
	r"Receipt\s+No\s+Completion\s+Time\s+Details\s+"
	r"Transaction\s+Status\s+Paid\s+in\s+Withdraw\s*n?\s+Balance",
	re.IGNORECASE,
)

# Summary TOTAL line on page 1 (marks end of summary block)
_SUMMARY_TOTAL_RE = re.compile(
	r"TOTAL\s*:\s*[\d,]+\.\d{2}\s+[\d,]+\.\d{2}",
	re.IGNORECASE,
)

# Start of a transaction row: 10-char alphanumeric receipt no. + ISO datetime
_TX_START_RE = re.compile(
	r"(?<!\w)([A-Z0-9]{10})\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
)

# End of a transaction row: STATUS + paid_in + withdrawn + balance
_TX_TAIL_RE = re.compile(
	r"(COMPLETED|FAILED)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$",
	re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────


def parse(
	file_url: str,
	period_from: str,
	period_to: str,
	password: str | None = None,
) -> dict[str, Any]:
	"""
	Parse a Safaricom M-Pesa Full Statement PDF.

	Args:
		file_url:    Frappe file URL, e.g. ``/private/files/Statement.pdf``.
		period_from: Statement start date (ISO string, for reference only).
		period_to:   Statement end date (ISO string, for reference only).
		password:    Optional PDF decryption password.

	Returns:
		Dict of field values ready to be merged into the MPesaStatement document.

	Raises:
		frappe.ValidationError: if pypdf is missing or the PDF is encrypted
		                        but no password was supplied.
	"""
	if pypdf is None:  # pragma: no cover
		frappe.throw("pypdf is not installed. Run: pip install pypdf")

	abs_path = _resolve_path(file_url)
	text = _extract_text(abs_path, password)
	transactions = _parse_transactions(text)
	return _compute_metrics(transactions)


# ─────────────────────────────────────────────────────────────────────────────
# PDF extraction
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_path(file_url: str) -> Path:
	"""Translate a Frappe ``/private/files/…`` or ``/files/…`` URL to an
	absolute filesystem path for the current site."""
	site_path = frappe.get_site_path()
	if file_url.startswith("/private/files/"):
		return Path(site_path) / file_url.lstrip("/")
	if file_url.startswith("/files/"):
		return Path(site_path) / "public" / file_url.lstrip("/")
	raise ValueError(f"Unsupported Frappe file URL: {file_url!r}")


def _extract_text(path: Path, password: str | None = None) -> str:
	"""Return concatenated plain text from all pages of the PDF file."""
	reader = pypdf.PdfReader(str(path))
	if reader.is_encrypted:
		if not password:
			frappe.throw(
				"The PDF is password-protected. "
				"Supply the statement password (usually your M-Pesa PIN or ID number) "
				"to parse this statement."
			)
		result = reader.decrypt(password)
		if result == pypdf.PasswordType.NOT_DECRYPTED:
			frappe.throw("Incorrect PDF password for this M-Pesa statement.")

	pages: list[str] = []
	for page in reader.pages:
		pages.append(page.extract_text() or "")
	return "\n".join(pages)


# ─────────────────────────────────────────────────────────────────────────────
# Transaction parsing
# ─────────────────────────────────────────────────────────────────────────────


def _parse_transactions(text: str) -> list[dict]:
	"""
	Extract all transaction rows from the combined PDF text.

	Strategy:
	  1. Remove repeating page-header lines.
	  2. Fast-forward past the summary block on page 1.
	  3. Split remaining text into one chunk per receipt number.
	  4. Within each chunk extract: receipt, datetime, details, status,
	     paid_in, withdrawn, balance.
	"""
	# 1. Strip table headers that repeat on every page
	text = _HEADER_RE.sub("", text)

	# 2. Skip the summary block (everything before the TOTAL: line on page 1)
	m = _SUMMARY_TOTAL_RE.search(text)
	if m:
		text = text[m.end():]

	# 3. Find all receipt-number positions
	starts = list(_TX_START_RE.finditer(text))
	if not starts:
		return []

	transactions: list[dict] = []

	for i, match in enumerate(starts):
		chunk_start = match.start()
		chunk_end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
		chunk = text[chunk_start:chunk_end]

		receipt = match.group(1)
		dt_str = match.group(2).strip()

		# Everything after the datetime within this chunk
		body = chunk[match.end() - chunk_start:]

		# 4. Find the tail (STATUS paid_in withdrawn balance) at the end of the chunk
		tail = _TX_TAIL_RE.search(body)
		if not tail:
			continue  # malformed row — skip

		status = tail.group(1).upper()
		paid_in = _to_float(tail.group(2))
		withdrawn = _to_float(tail.group(3))
		balance = _to_float(tail.group(4))

		# Details = text between datetime and STATUS, normalised to single spaces
		raw_details = body[: tail.start()].strip()
		details = " ".join(raw_details.split())

		tx_date = dt_str.split()[0]  # YYYY-MM-DD

		transactions.append(
			{
				"receipt_no": receipt,
				"date": tx_date,
				"datetime": dt_str,
				"description": details,
				"type": _classify_type(details),
				"amount": round(paid_in - withdrawn, 2),  # positive = inflow
				"paid_in": paid_in,
				"withdrawn": withdrawn,
				"balance": balance,
				"category": _classify_category(details, paid_in, withdrawn),
				"counterparty": _extract_counterparty(details),
				"status": status,
			}
		)

	return transactions


# ─────────────────────────────────────────────────────────────────────────────
# Metrics computation
# ─────────────────────────────────────────────────────────────────────────────


def _compute_metrics(transactions: list[dict]) -> dict[str, Any]:
	"""Aggregate parsed transactions into the MPesaStatement summary fields."""

	_empty: dict[str, Any] = {
		"parsed_transactions": "[]",
		"monthly_avg_inflow": 0,
		"monthly_avg_outflow": 0,
		"avg_monthly_balance": 0,
		"salary_credit_regularity": 0,
		"loan_repayments_detected": 0,
		"net_cashflow_trend": "Stable",
		"gambling_transactions_detected": 0,
		"gambling_total": 0,
	}
	if not transactions:
		return _empty

	# Per-month accumulators
	by_month: dict[str, dict] = defaultdict(
		lambda: {
			"inflow": 0.0,
			"outflow": 0.0,
			"last_balance": 0.0,
			"has_salary": False,
		}
	)

	gambling_total = 0.0
	has_gambling = False
	loan_repayments_total = 0.0

	for tx in transactions:
		if tx["status"] != "COMPLETED":
			continue

		month = tx["date"][:7]  # YYYY-MM
		paid_in = tx["paid_in"]
		withdrawn = tx["withdrawn"]
		dl = tx["description"].lower()

		by_month[month]["inflow"] += paid_in
		by_month[month]["outflow"] += withdrawn
		# last_balance will end up as the FIRST tx of the month since the PDF is
		# newest-first; we'll correct the ordering in the sort below
		by_month[month]["last_balance"] = tx["balance"]

		# Salary / regular income detection
		if not by_month[month]["has_salary"]:
			if any(kw in dl for kw in SALARY_KEYWORDS):
				by_month[month]["has_salary"] = True

		# Loan repayment detection (only outflows)
		if withdrawn > 0 and any(kw in dl for kw in LOAN_REPAYMENT_KEYWORDS):
			loan_repayments_total += withdrawn

		# Gambling detection (only outflows)
		if withdrawn > 0 and any(kw in dl for kw in GAMBLING_KEYWORDS):
			has_gambling = True
			gambling_total += withdrawn

	months = sorted(by_month.keys())  # ascending YYYY-MM
	n_months = len(months) or 1

	monthly_avg_inflow = sum(by_month[m]["inflow"] for m in months) / n_months
	monthly_avg_outflow = sum(by_month[m]["outflow"] for m in months) / n_months
	avg_monthly_balance = sum(by_month[m]["last_balance"] for m in months) / n_months
	salary_regularity = (
		sum(1 for m in months if by_month[m]["has_salary"]) / n_months
	) * 100

	# Net cashflow trend: compare avg closing balance in first half vs second half
	mid = max(n_months // 2, 1)
	first_half = months[:mid]
	second_half = months[mid:]

	def _half_avg(half: list[str]) -> float:
		if not half:
			return 0.0
		return sum(by_month[m]["last_balance"] for m in half) / len(half)

	first_avg = _half_avg(first_half)
	second_avg = _half_avg(second_half)

	if first_avg == 0:
		trend = "Stable"
	elif (second_avg - first_avg) / first_avg > 0.10:
		trend = "Improving"
	elif (second_avg - first_avg) / first_avg < -0.10:
		trend = "Declining"
	else:
		trend = "Stable"

	# Build lean output list (drop internal parsing fields)
	output_tx = [
		{
			"date": t["date"],
			"type": t["type"],
			"amount": t["amount"],
			"balance": t["balance"],
			"description": t["description"],
			"category": t["category"],
			"counterparty": t["counterparty"],
		}
		for t in transactions
	]

	return {
		"parsed_transactions": json.dumps(output_tx, ensure_ascii=False),
		"monthly_avg_inflow": round(monthly_avg_inflow, 2),
		"monthly_avg_outflow": round(monthly_avg_outflow, 2),
		"avg_monthly_balance": round(avg_monthly_balance, 2),
		"salary_credit_regularity": round(salary_regularity, 2),
		"loan_repayments_detected": round(loan_repayments_total, 2),
		"net_cashflow_trend": trend,
		"gambling_transactions_detected": 1 if has_gambling else 0,
		"gambling_total": round(gambling_total, 2),
	}


# ─────────────────────────────────────────────────────────────────────────────
# Classification helpers
# ─────────────────────────────────────────────────────────────────────────────


def _classify_type(details: str) -> str:
	"""Return a normalised transaction type derived from the Details text."""
	d = details.lower()
	if "merchant payment" in d:
		return "Merchant Payment"
	if "pay bill" in d:
		return "Pay Bill"
	if "airtime" in d:
		return "Airtime"
	if "customer transfer" in d or "send money" in d:
		return "Send Money"
	if "b2c payment" in d:
		return "B2C Payment"
	if "deposit" in d and "agent" in d:
		return "Cash In"
	if "cash out" in d:
		return "Cash Out"
	if "m-shwari" in d or "mshwari" in d:
		return "M-Shwari"
	if "od loan repayment" in d or "overdraw" in d:
		return "OD Repayment"
	if "overdraft" in d or "fuliza" in d:
		return "Fuliza"
	if "fsi withdraw" in d or "fsi deposit" in d:
		return "FSI"
	if "reversal" in d:
		return "Reversal"
	if "customer payment to small business" in d:
		return "Send Money"
	return "Other"


def _classify_category(details: str, paid_in: float, withdrawn: float) -> str:
	"""High-level category for credit-risk analysis."""
	d = details.lower()

	if any(kw in d for kw in GAMBLING_KEYWORDS):
		return "Gambling"
	if any(kw in d for kw in LOAN_REPAYMENT_KEYWORDS):
		return "Loan Repayment"
	if any(kw in d for kw in SALARY_KEYWORDS) and paid_in > 0:
		return "Income"
	if "deposit" in d and "agent" in d and paid_in > 0:
		return "Cash In"
	if "merchant payment" in d and withdrawn > 0:
		return "Expenditure"
	if "pay bill" in d and withdrawn > 0:
		return "Expenditure"
	if "airtime" in d and withdrawn > 0:
		return "Airtime"
	if "customer transfer" in d or "customer payment to small business" in d:
		return "Transfer Out" if withdrawn > 0 else "Transfer In"
	if "m-shwari withdraw" in d and paid_in > 0:
		return "Mobile Loan Drawdown"
	if "fuliza" in d or "overdraft" in d:
		return "Mobile Loan Drawdown" if paid_in > 0 else "Mobile Loan Repayment"
	return "Other"


def _extract_counterparty(details: str) -> str:
	"""
	Try to extract a counterparty name from a Details string.

	M-Pesa details typically end with ``- COUNTERPARTY NAME`` or
	``to PAYBILL_NO - MERCHANT NAME``.
	"""
	# "to 247247 - Equity Paybill Account Acc. 0741***469"
	m = re.search(r"\bto\s+\d+\s*-\s*(.+?)(?:\s+Acc\.\s+\S+)?$", details, re.IGNORECASE)
	if m:
		return m.group(1).strip()

	# Generic trailing "- NAME" pattern
	m = re.search(r"-\s*([A-Z][A-Z0-9 &'./()\-]+)$", details.strip(), re.IGNORECASE)
	if m:
		return m.group(1).strip()

	return ""


def _to_float(s: str) -> float:
	return float(s.replace(",", ""))
