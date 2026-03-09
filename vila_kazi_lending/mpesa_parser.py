"""vila_kazi_lending.mpesa_parser
────────────────────────────────
Parses a Safaricom M-Pesa Full Statement (PDF or CSV) and returns financial
metrics for credit appraisal.

Supported formats:
  PDF — Safaricom "MPESA FULL STATEMENT" PDF (pdfplumber preferred, pypdf fallback)
  CSV — Safaricom CSV export with columns:
        Receipt No.,Completion Time,Details,Transaction Status,Paid In,Withdrawn,Balance

Returns a dict whose keys map directly to MPesaStatement doctype fields:
  parsed_transactions          – JSON array of transaction dicts
  monthly_avg_inflow           – float (KES)
  monthly_avg_outflow          – float (KES)
  avg_monthly_balance          – float (KES)
  salary_credit_regularity     – float 0–100 (% of months with a salary-like credit)
  loan_repayments_detected     – float (total KES repaid to other lenders)
  net_cashflow_trend           – "Improving" | "Stable" | "Declining"
  gambling_transactions_detected – 0 | 1
  gambling_total               – float (KES spent on gambling merchants)
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import frappe

try:
	import pdfplumber
except ImportError:  # pragma: no cover
	pdfplumber = None  # type: ignore

try:
	import pypdf
except ImportError:  # pragma: no cover
	pypdf = None  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Default keyword sets (overridden at runtime from VK Lending Settings)
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_GAMBLING_KEYWORDS: list[str] = [
	"sportpesa",
	"odibets",
	"betika",
	"betway",
	"mozzartbet",
	"mozzart",
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

# Categorisation regexes — applied in order per spec
_RE_SALARY = re.compile(r"salary|payroll|employer", re.IGNORECASE)
_RE_LOAN = re.compile(r"loan|repay|lend|credit ref", re.IGNORECASE)
_RE_UTILITIES = re.compile(
	r"kplc|nairobi water|water|electricity|safaricom home|zuku|faiba",
	re.IGNORECASE,
)
_RE_AIRTIME = re.compile(r"airtime|bundles", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────


def parse(
	file_url: str,
	period_from: str,
	period_to: str,
	password: str | None = None,
	gambling_keywords: list[str] | None = None,
) -> dict[str, Any]:
	"""
	Parse a Safaricom M-Pesa Full Statement (PDF or CSV).

	Args:
		file_url:          Frappe file URL, e.g. ``/private/files/Statement.pdf``.
		period_from:       Statement start date (ISO string, for reference only).
		period_to:         Statement end date (ISO string, for reference only).
		password:          Optional PDF decryption password.
		gambling_keywords: List of lowercase keyword strings to flag as gambling.
		                   Defaults to VK Lending Settings value, then built-in list.

	Returns:
		Dict of field values ready to be merged into the MPesaStatement document.

	Raises:
		ValueError: if the file extension is unrecognised.
		frappe.ValidationError: if PDF is encrypted but no password was supplied.
	"""
	if gambling_keywords is None:
		gambling_keywords = _load_gambling_keywords()

	abs_path = _resolve_path(file_url)
	ext = abs_path.suffix.lower()

	if ext == ".csv":
		transactions = _parse_csv(abs_path)
	else:
		# Default: treat as PDF
		text = _extract_text_pdfplumber(abs_path, password)
		transactions = _parse_transactions_from_text(text)

	return _compute_metrics(transactions, gambling_keywords)


# ─────────────────────────────────────────────────────────────────────────────
# Public low-level helpers (used by tests)
# ─────────────────────────────────────────────────────────────────────────────


def parse_csv_content(content: str, gambling_keywords: list[str] | None = None) -> dict[str, Any]:
	"""Parse raw CSV text and return metrics dict. Useful for unit tests."""
	if gambling_keywords is None:
		gambling_keywords = _load_gambling_keywords()
	transactions = _parse_csv_text(content)
	return _compute_metrics(transactions, gambling_keywords)


def categorise(description: str, direction: str, tx_type: str, gambling_keywords: list[str]) -> str:
	"""
	Categorise a single transaction per the spec's ordered rules.

	Args:
		description:       Raw description text from the statement.
		direction:         "in" or "out".
		tx_type:           Normalised transaction type string.
		gambling_keywords: Lowercase gambling platform keywords.

	Returns:
		One of: Salary Credit, Gambling, Loan Repayment, Utilities, Airtime,
		        B2C Transfer, Other.
	"""
	d_lower = description.lower()

	# Rule 1: Salary Credit
	if direction == "in" and _RE_SALARY.search(description):
		return "Salary Credit"

	# Rule 2: Gambling (MUST run before Loan Repayment)
	if any(kw in d_lower for kw in gambling_keywords):
		return "Gambling"

	# Rule 3: Loan Repayment
	if _RE_LOAN.search(description):
		return "Loan Repayment"

	# Rule 4: Utilities
	if _RE_UTILITIES.search(description):
		return "Utilities"

	# Rule 5: Airtime
	if _RE_AIRTIME.search(description):
		return "Airtime"

	# Rule 6: B2C Transfer
	if direction == "in" and "b2c" in d_lower:
		return "B2C Transfer"

	return "Other"


# ─────────────────────────────────────────────────────────────────────────────
# Settings helpers
# ─────────────────────────────────────────────────────────────────────────────


def _load_gambling_keywords() -> list[str]:
	"""Return lowercase gambling keywords from VK Lending Settings, with fallback."""
	try:
		raw = frappe.db.get_single_value("VK Lending Settings", "gambling_keywords") or ""
		if raw.strip():
			return [k.strip().lower() for k in raw.split(",") if k.strip()]
	except Exception:
		pass
	return _DEFAULT_GAMBLING_KEYWORDS


# ─────────────────────────────────────────────────────────────────────────────
# File resolution
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_path(file_url: str) -> Path:
	"""Translate a Frappe file URL to an absolute filesystem path."""
	site_path = frappe.get_site_path()
	if file_url.startswith("/private/files/"):
		return Path(site_path) / file_url.lstrip("/")
	if file_url.startswith("/files/"):
		return Path(site_path) / "public" / file_url.lstrip("/")
	raise ValueError(f"Unsupported Frappe file URL: {file_url!r}")


# ─────────────────────────────────────────────────────────────────────────────
# PDF extraction — pdfplumber preferred, pypdf fallback
# ─────────────────────────────────────────────────────────────────────────────


def _extract_text_pdfplumber(path: Path, password: str | None = None) -> str:
	"""Extract plain text from all PDF pages using pdfplumber, falling back to pypdf."""
	if pdfplumber is not None:
		try:
			open_kwargs: dict[str, Any] = {}
			if password:
				open_kwargs["password"] = password
			with pdfplumber.open(str(path), **open_kwargs) as pdf:
				# Try table extraction first — M-Pesa PDFs often have structured tables
				rows: list[str] = []
				for page in pdf.pages:
					tables = page.extract_tables()
					if tables:
						for table in tables:
							for row in table:
								if row:
									rows.append("\t".join(str(c or "") for c in row))
					else:
						# Fall back to text extraction for this page
						rows.append(page.extract_text() or "")
			return "\n".join(rows)
		except Exception:
			# pdfplumber failed — try pypdf below
			pass

	# pypdf fallback
	if pypdf is None:  # pragma: no cover
		frappe.throw(
			"Neither pdfplumber nor pypdf is installed. "
			"Run: pip install pdfplumber"
		)

	reader = pypdf.PdfReader(str(path))
	if reader.is_encrypted:
		if not password:
			frappe.throw(
				"The PDF is password-protected. "
				"Supply the statement password (your M-Pesa PIN or ID number)."
			)
		result = reader.decrypt(password)
		if result == pypdf.PasswordType.NOT_DECRYPTED:
			frappe.throw("Incorrect PDF password for this M-Pesa statement.")

	pages: list[str] = []
	for page in reader.pages:
		pages.append(page.extract_text() or "")
	return "\n".join(pages)


# ─────────────────────────────────────────────────────────────────────────────
# CSV parsing
# ─────────────────────────────────────────────────────────────────────────────

# Expected CSV column names (case-insensitive match)
_CSV_COL_MAP = {
	"receipt no.": "receipt_no",
	"receipt no": "receipt_no",
	"completion time": "datetime",
	"details": "description",
	"transaction status": "status",
	"paid in": "paid_in",
	"withdrawn": "withdrawn",
	"balance": "balance",
}


def _parse_csv(path: Path) -> list[dict]:
	"""Read a Safaricom M-Pesa CSV statement from disk."""
	with open(str(path), encoding="utf-8-sig") as fh:
		content = fh.read()
	return _parse_csv_text(content)


def _parse_csv_text(content: str) -> list[dict]:
	"""Parse raw CSV text into transaction dicts."""
	reader = csv.DictReader(io.StringIO(content))

	# Build a normalised column name → field name mapping
	fieldnames = reader.fieldnames or []
	col_map: dict[str, str] = {}
	for original in fieldnames:
		normalised = (original or "").strip().lower()
		if normalised in _CSV_COL_MAP:
			col_map[original] = _CSV_COL_MAP[normalised]

	transactions: list[dict] = []
	for raw_row in reader:
		# Map to standard field names
		row: dict[str, str] = {}
		for orig, field in col_map.items():
			row[field] = (raw_row.get(orig) or "").strip()

		status = row.get("status", "").upper()
		if status != "COMPLETED":
			continue

		dt_str = row.get("datetime", "")
		tx_date = dt_str.split()[0] if dt_str else ""
		if not tx_date or len(tx_date) < 10:
			continue

		paid_in = _to_float(row.get("paid_in", "0") or "0")
		withdrawn = _to_float(row.get("withdrawn", "0") or "0")
		balance = _to_float(row.get("balance", "0") or "0")
		description = row.get("description", "")
		tx_type = _classify_type(description)
		direction = "in" if paid_in > 0 else "out"
		amount = paid_in if direction == "in" else withdrawn

		transactions.append(
			{
				"receipt_no": row.get("receipt_no", ""),
				"date": tx_date,
				"datetime": dt_str,
				"description": description,
				"type": tx_type,
				"amount": round(amount, 2),
				"direction": direction,
				"paid_in": paid_in,
				"withdrawn": withdrawn,
				"balance": balance,
				"status": status,
				"counterparty": _extract_counterparty(description),
			}
		)

	return transactions


# ─────────────────────────────────────────────────────────────────────────────
# PDF text-mode parsing
# ─────────────────────────────────────────────────────────────────────────────


def _parse_transactions_from_text(text: str) -> list[dict]:
	"""
	Extract transaction rows from plain PDF text (regex-based).

	Handles both raw text-layer output and pdfplumber tab-delimited table rows.
	If the text contains tab-delimited rows that look like a statement table,
	those are preferred; otherwise the classic regex parser is used.
	"""
	# If pdfplumber extracted tab-delimited rows, try structured parse first
	if "\t" in text:
		transactions = _parse_tabular_text(text)
		if transactions:
			return transactions

	# Classic text-layer regex parser
	return _parse_transactions_regex(text)


def _parse_tabular_text(text: str) -> list[dict]:
	"""
	Parse tab-delimited rows produced by pdfplumber table extraction.

	Expected columns (by position):
	  0: Receipt No | 1: Completion Time | 2: Details | 3: Status |
	  4: Paid In    | 5: Withdrawn       | 6: Balance
	"""
	transactions: list[dict] = []
	for line in text.splitlines():
		parts = [p.strip() for p in line.split("\t")]
		if len(parts) < 7:
			continue

		# Validate receipt no. heuristic: 10 alphanumeric chars
		receipt = parts[0]
		if not re.match(r"^[A-Z0-9]{10}$", receipt):
			continue

		status = parts[3].upper() if len(parts) > 3 else ""
		if status != "COMPLETED":
			continue

		dt_str = parts[1]
		tx_date = dt_str.split()[0] if dt_str else ""
		if not tx_date or len(tx_date) < 10:
			continue

		description = parts[2]
		paid_in = _to_float(parts[4] if len(parts) > 4 else "0")
		withdrawn = _to_float(parts[5] if len(parts) > 5 else "0")
		balance = _to_float(parts[6] if len(parts) > 6 else "0")
		tx_type = _classify_type(description)
		direction = "in" if paid_in > 0 else "out"
		amount = paid_in if direction == "in" else withdrawn

		transactions.append(
			{
				"receipt_no": receipt,
				"date": tx_date,
				"datetime": dt_str,
				"description": description,
				"type": tx_type,
				"amount": round(amount, 2),
				"direction": direction,
				"paid_in": paid_in,
				"withdrawn": withdrawn,
				"balance": balance,
				"status": status,
				"counterparty": _extract_counterparty(description),
			}
		)

	return transactions


def _parse_transactions_regex(text: str) -> list[dict]:
	"""Classic regex-based parser for raw PDF text."""
	# Strip table headers that repeat on every page
	text = _HEADER_RE.sub("", text)

	# Skip the summary block (everything before the TOTAL: line on page 1)
	m = _SUMMARY_TOTAL_RE.search(text)
	if m:
		text = text[m.end():]

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

		body = chunk[match.end() - chunk_start:]
		tail = _TX_TAIL_RE.search(body)
		if not tail:
			continue  # malformed row

		status = tail.group(1).upper()
		if status != "COMPLETED":
			continue

		paid_in = _to_float(tail.group(2))
		withdrawn = _to_float(tail.group(3))
		balance = _to_float(tail.group(4))

		raw_details = body[: tail.start()].strip()
		description = " ".join(raw_details.split())

		tx_date = dt_str.split()[0]
		tx_type = _classify_type(description)
		direction = "in" if paid_in > 0 else "out"
		amount = paid_in if direction == "in" else withdrawn

		transactions.append(
			{
				"receipt_no": receipt,
				"date": tx_date,
				"datetime": dt_str,
				"description": description,
				"type": tx_type,
				"amount": round(amount, 2),
				"direction": direction,
				"paid_in": paid_in,
				"withdrawn": withdrawn,
				"balance": balance,
				"status": status,
				"counterparty": _extract_counterparty(description),
			}
		)

	return transactions


# ─────────────────────────────────────────────────────────────────────────────
# Metrics computation
# ─────────────────────────────────────────────────────────────────────────────


def _compute_metrics(transactions: list[dict], gambling_keywords: list[str]) -> dict[str, Any]:
	"""Aggregate parsed transactions into MPesaStatement summary fields."""

	_empty: dict[str, Any] = {
		"parsed_transactions": "[]",
		"monthly_avg_inflow": 0.0,
		"monthly_avg_outflow": 0.0,
		"avg_monthly_balance": 0.0,
		"salary_credit_regularity": 0.0,
		"loan_repayments_detected": 0.0,
		"net_cashflow_trend": "Stable",
		"gambling_transactions_detected": 0,
		"gambling_total": 0.0,
	}
	if not transactions:
		return _empty

	# Add category to each transaction (using spec-ordered rules)
	for tx in transactions:
		tx["category"] = categorise(
			tx["description"],
			tx["direction"],
			tx["type"],
			gambling_keywords,
		)

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
		month = tx["date"][:7]  # YYYY-MM
		paid_in = tx["paid_in"]
		withdrawn = tx["withdrawn"]

		by_month[month]["inflow"] += paid_in
		by_month[month]["outflow"] += withdrawn
		# last_balance tracks the final known balance per month
		by_month[month]["last_balance"] = tx["balance"]

		cat = tx["category"]

		if cat == "Salary Credit":
			by_month[month]["has_salary"] = True

		if cat == "Loan Repayment":
			loan_repayments_total += withdrawn

		if cat == "Gambling":
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

	# Net cashflow trend — simple linear slope on monthly net cashflow
	monthly_nets = [by_month[m]["inflow"] - by_month[m]["outflow"] for m in months]
	trend = _linear_trend(monthly_nets)

	# Build lean output list (omit internal parsing fields)
	output_tx = [
		{
			"date": t["date"],
			"type": t["type"],
			"amount": t["amount"],
			"direction": t["direction"],
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


def _linear_trend(values: list[float]) -> str:
	"""
	Fit a simple linear slope to a sequence of monthly values.

	Slope > +500  → "Improving"
	Slope < -500  → "Declining"
	Otherwise     → "Stable"
	"""
	n = len(values)
	if n < 2:
		return "Stable"

	# Least-squares slope: sum((x - x_mean)(y - y_mean)) / sum((x - x_mean)^2)
	x_mean = (n - 1) / 2.0
	y_mean = sum(values) / n
	numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
	denominator = sum((i - x_mean) ** 2 for i in range(n))

	if denominator == 0:
		return "Stable"

	slope = numerator / denominator

	if slope > 500:
		return "Improving"
	if slope < -500:
		return "Declining"
	return "Stable"


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


def _extract_counterparty(details: str) -> str:
	"""Try to extract a counterparty name from a Details string."""
	m = re.search(r"\bto\s+\d+\s*-\s*(.+?)(?:\s+Acc\.\s+\S+)?$", details, re.IGNORECASE)
	if m:
		return m.group(1).strip()

	m = re.search(r"-\s*([A-Z][A-Z0-9 &'./()\-]+)$", details.strip(), re.IGNORECASE)
	if m:
		return m.group(1).strip()

	return ""


def _to_float(s: str) -> float:
	try:
		return float(str(s).replace(",", "").strip() or "0")
	except ValueError:
		return 0.0
