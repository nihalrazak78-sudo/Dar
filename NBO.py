import pandas as pd
from collections import defaultdict
import io
from flask import Flask, request, send_file, jsonify
import os
app = Flask(__name__)
def safe_read_excel_from_bytes(file_bytes, **kwargs):
"""Reads an Excel file from bytes, handling potential errors."""
try:
return pd.read_excel(io.BytesIO(file_bytes), **kwargs)
except Exception as e:
print(f"❌ Error reading Excel from bytes: {e}")
# Re-raise as ValueError to be caught by the Flask route for a 400
response
raise ValueError(f"Failed to read Excel file: {e}")
def extract_nbo_receipt_breakdown_rows_web(nbo_file_bytes, nbo_sheet,
bank_file_bytes, bank_sheet):
"""Core logic adapted for web service, processing Excel bytes and returning
processed Excel bytes."""
# Load NBO Data
nbo_df = safe_read_excel_from_bytes(nbo_file_bytes, sheet_name=nbo_sheet,
skiprows=16, header=None)
# Original columns: [2, 3, 5, 7, 16] -> Date, Bank Reference No,
Description, Credit Amount, ORACLE STATUS
nbo_df = nbo_df[[2, 3, 5, 7, 16]]
nbo_df.columns = [None, None, None, None, None] # Reset columns to avoid
issues with potential duplicate names
nbo_df.columns = ["Date", "Bank Reference No", "Description", "Credit
Amount", "ORACLE STATUS"]
# Filter out rows with missing 'Credit Amount' or 'Bank Reference No'
nbo_df = nbo_df[nbo_df["Credit Amount"].notna() & nbo_df["Bank Reference
No"].notna()]
# Data type conversions and cleaning
nbo_df["Date"] = pd.to_datetime(nbo_df["Date"], errors="coerce")
nbo_df["Bank Reference No"] = nbo_df["Bank Reference
No"].astype(str).str.strip()
nbo_df["Description"] =
nbo_df["Description"].astype(str).str.strip().str.replace("\"", "",
regex=False)
# Load Bank Report
bank_df = safe_read_excel_from_bytes(bank_file_bytes,
sheet_name=bank_sheet, header=8)
bank_df.columns = [str(c).strip() for c in bank_df.columns] # Clean column
names
bank_df["Description"] =
bank_df["Description"].astype(str).str.strip().str.replace("\"", "",
regex=False)
bank_df["Oracle Receipt Number (Recon)"] = bank_df["Oracle Receipt Number
(Recon)"].astype(str).str.strip()
# Build reference map for bank data
bank_lookup = {
str(row["Oracle Receipt Number (Recon)"]).strip(): row
for _, row in bank_df.iterrows()
if pd.notna(row["Oracle Receipt Number (Recon)"])
}
# Group bank rows by Description for efficient lookup
bank_groups = defaultdict(list)
for idx, row in bank_df.iterrows():
desc = row["Description"]
if pd.notna(desc):
bank_groups[desc].append(idx)
# Containers for processed data
output_rows = []
used_oracle_numbers = set() # Track used Oracle numbers to avoid duplicates
used_bank_indices = set() # Track used bank indices to avoid re-matching
# Process NBO rows
for _, nbo_row in nbo_df.iterrows():
ref = str(nbo_row["Bank Reference No"]).strip()
desc = nbo_row["Description"]
credit_amt = float(nbo_row["Credit Amount"])
txn_date = nbo_row["Date"]
matched_receipts = []
cumulative_sum = 0.0
# Attempt to match NBO row with bank receipts based on description and
amount
for bank_idx in bank_groups.get(desc, []):
if bank_idx in used_bank_indices:
continue # Skip if this bank entry has already been used
bank_row = bank_df.loc[bank_idx]
oracle_raw = bank_row["Oracle Receipt Number (Recon)"]
try:
oracle_num = int(float(oracle_raw)) if pd.notna(oracle_raw)
else None
except ValueError:
oracle_num = None # Handle cases where conversion to int/float
fails
if oracle_num in used_oracle_numbers or oracle_num is None:
continue # Skip if Oracle number already used or is invalid
try:
receipt_amt = float(bank_row["Receipt Amount"])
except ValueError:
receipt_amt = 0.0 # Default to 0 if receipt amount is not a
valid number
matched_receipts.append((
receipt_amt, oracle_num,
bank_row.get("Currency"),
bank_row.get("Account Number.")
))
used_oracle_numbers.add(oracle_num)
used_bank_indices.add(bank_idx)
cumulative_sum += receipt_amt
# If cumulative sum is close to credit amount, break (assuming full
match or close enough)
if cumulative_sum >= credit_amt - 0.01:
break
# Helper to build a standardized output row dictionary
def build_output_row(overrides={}):
row_template = {
"Line No.": None,
"Type": "Receipt",
"Code": "",
"ORACLE NUMBER": None,
"Transaction Date": txn_date,
"Account Currency Cleared Date": txn_date,
"Currency": None,
"Exchange Rate Date": txn_date,
"Rate Type": "Corporate",
"Account Number": None,
"Customer Name": None,
"Account Currency Amount": None,
"Account Currency Amount Cleared": None,
"Cleared Date": txn_date,
"Value Date": txn_date,
"GL Date": txn_date,
"Bank Reference No": ref,
"Description": desc,
"Credit Amount": credit_amt,
"Receipt Amount": None,
"Tally": None
}
row_template.update(overrides)
return row_template
if not matched_receipts:
# If no matches, add the NBO row as is (or with default values)
output_rows.append(build_output_row())
else:
total_receipts = sum(r[0] for r in matched_receipts)
for idx, (receipt_amt, oracle_num, currency, account_number) in
enumerate(matched_receipts):
is_last = idx == len(matched_receipts) - 1
row_data = {
"ORACLE NUMBER": oracle_num,
"Currency": currency,
"Account Number": account_number,
"Credit Amount": credit_amt if idx == 0 else "", # Only
show credit amount on first line of a group
"Receipt Amount": receipt_amt,
"Tally": round(credit_amt - total_receipts, 2) if is_last
else "" # Tally only on the last line
}
output_rows.append(build_output_row(row_data))
# Create final DataFrame from processed rows
final_df = pd.DataFrame(output_rows)
# Drop duplicates based on 'Bank Reference No' and 'ORACLE NUMBER', keeping
the first occurrence
final_df.drop_duplicates(subset=["Bank Reference No", "ORACLE NUMBER"],
keep="first", inplace=True)
# Add 'Line No.' column if not present, and ensure it's the first column
if "Line No." in final_df.columns:
final_df.drop(columns=["Line No."], inplace=True)
final_df.insert(0, "Line No.", range(1, len(final_df) + 1))
# Define desired column order for the final output
desired_order = [
"Line No.", "Type", "Code", "ORACLE NUMBER",
"Transaction Date", "Account Currency Cleared Date",
"Currency", "Exchange Rate Date", "Rate Type",
"Account Number", "Customer Name",
"Account Currency Amount", "Account Currency Amount Cleared",
"Cleared Date", "Value Date", "GL Date",
"Bank Reference No", "Description",
"Credit Amount", "Receipt Amount", "Tally"
]
# Filter and reorder columns, keeping only those that exist in final_df
final_df = final_df[[col for col in desired_order if col in
final_df.columns]]
# Write the final DataFrame to an in-memory Excel file
output_buffer = io.BytesIO()
with pd.ExcelWriter(output_buffer, engine="xlsxwriter",
datetime_format="dd-mmm-yy") as writer:
final_df.to_excel(writer, index=False, sheet_name="Formatted")
workbook = writer.book
worksheet = writer.sheets["Formatted"]
date_fmt = workbook.add_format({"num_format": "dd-mmm-yy"})
# Adjust column widths and apply date format
for i, column in enumerate(final_df.columns):
series = final_df[column].astype(str)
max_len = max(series.map(len).max(), len(column)) + 2
fmt = date_fmt if column in [
"Transaction Date", "Account Currency Cleared Date",
"Exchange Rate Date", "Cleared Date", "Value Date", "GL Date"]
else None
worksheet.set_column(i, i, max_len, fmt)
output_buffer.seek(0) # Rewind buffer to the beginning
return output_buffer.getvalue() # Return the binary content of the Excel
file
@app.route("/process_excel", methods=["POST"])
def process_excel():
"""Flask endpoint to receive Excel files, process them, and return the
result."""
# Check if files are present in the request
if "nbo_file" not in request.files or "bank_file" not in request.files:
return jsonify({"error": "Missing one or both files (nbo_file,
bank_file)"}), 400
nbo_file = request.files["nbo_file"]
bank_file = request.files["bank_file"]
# Get sheet names from form data, with default values
nbo_sheet = request.form.get("nbo_sheet", "Jul-25")
bank_sheet = request.form.get("bank_sheet", "All Receipts Report")
try:
# Read file contents into bytes
nbo_file_bytes = nbo_file.read()
bank_file_bytes = bank_file.read()
# Call the core processing function
processed_excel_bytes = extract_nbo_receipt_breakdown_rows_web(
nbo_file_bytes, nbo_sheet, bank_file_bytes, bank_sheet
)
# Return the processed Excel file as a downloadable attachment
return send_file(
io.BytesIO(processed_excel_bytes),
mimetype="application/vnd.openxmlformatsofficedocument.spreadsheetml.sheet",
as_attachment=True,
download_name="NBO_Matched_Exploded_Processed.xlsx"
)
except ValueError as e:
# Catch specific ValueErrors from safe_read_excel_from_bytes or other
data issues
return jsonify({"error": str(e)}), 400
except Exception as e:
# Catch any other unexpected errors
return jsonify({"error": f"An internal server error occurred: {e}"}),
500
@app.route("/health", methods=["GET"])
def health_check():
"""Simple health check endpoint for Render."""
return "OK", 200
if __name__ == "__main__":
# This block is for local development/testing only
# Render will use Gunicorn to run the app in production
app.run(debug=True, host="0.0.0.0", port=os.environ.get("PORT", 5000))