#!/usr/bin/env python3
"""
Test script to verify the external operator streaming functionality with transaction data.
"""

import duckdb
import os
import tempfile
import csv

EXT_PATH = os.path.abspath(
    "build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension"
)

# Connect to DuckDB with unsigned extension loading
con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)

# Load the external extension
con.execute(f"LOAD '{EXT_PATH}'")

# Create CSV file with transaction data
csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
csv_path = csv_file.name

csv_writer = csv.writer(csv_file)
csv_writer.writerow(['transaction_type', 'amount', 'customer_id', 'status', 'is_active'])  # Header
csv_writer.writerow(['food', 0.0, 101, 'pending', True])      # Violating: food
csv_writer.writerow(['food', 50.5, 102, 'completed', False])    # Violating: food
csv_writer.writerow(['clothing', 100.25, 103, 'pending', True])  # Non-violating: not food
csv_writer.writerow(['food', 25.75, 104, 'completed', True])    # Violating: food
csv_writer.writerow(['electronics', 200.99, 105, 'pending', False])  # Non-violating: not food
csv_writer.writerow(['food', 100.0, 106, 'completed', True])   # Violating: food
csv_file.close()

# Create a temporary file for the stream (initially empty)
stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
stream_path = stream_file.name
stream_file.close()

# Load CSV data into DuckDB table
con.execute(f"""
    CREATE TABLE T AS
    SELECT transaction_type, amount, customer_id, status, is_active
    FROM read_csv_auto('{csv_path}');
""")

# Define & register the Python UDF
def address_violating_rows(transaction_type: str, amount: float, customer_id: int, status: str, is_active: bool, stream_endpoint: str) -> bool:
    """Handle violating rows where transaction_type=="food". Writes row to stream file with amount=0.0."""
    # Write the violating row to the stream file with amount=0.0 and all other columns
    with open(stream_endpoint, 'a') as f:
        f.write(f"{transaction_type}\t0.0\t{customer_id}\t{status}\t{is_active}\n")
        f.flush()
    return False  # Filter out the violating row

con.create_function("address_violating_rows", address_violating_rows)

# Query with violation condition
query = f"""
SELECT *
FROM T
WHERE CASE
        WHEN transaction_type != 'food' THEN TRUE
        ELSE address_violating_rows(transaction_type, amount, customer_id, status, is_active, '{stream_path}')
      END;
"""

# Execute the query
df = con.execute(query).df()

# Expected: 2 table rows (non-food) + 4 stream rows (all food with amount=0) = 6 rows
expected_rows = 6
passed = len(df) == expected_rows

# Verify stream data is unionized
# Count actual rows (not unique rows, since we can have duplicates)
stream_row_count = sum(1 for _, row in df.iterrows() 
                       if row['transaction_type'] == 'food' and row['amount'] == 0.0)
table_row_count = sum(1 for _, row in df.iterrows() 
                       if not (row['transaction_type'] == 'food' and row['amount'] == 0.0))

stream_correct = stream_row_count == 4
table_correct = table_row_count == 2

# Output results
print("Results:")
print(df)
print(f"\nTotal rows: {len(df)} (expected: {expected_rows})")
print(f"Stream rows: {stream_row_count} (expected: 4)")
print(f"Table rows: {table_row_count} (expected: 2)")

if passed and stream_correct and table_correct:
    print("\n✓ Test PASSED")
else:
    print("\n✗ Test FAILED")

# Cleanup
try:
    os.unlink(csv_path)
    os.unlink(stream_path)
except:
    pass
