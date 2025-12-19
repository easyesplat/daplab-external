#!/usr/bin/env python3
"""
Test script to verify the external operator streaming functionality.
This script tests:
1. Setting stream endpoint via pragma
2. Writing data to a stream file
3. Querying with the external operator (via UNION)
4. Verifying data is read from the stream using streaming approach
"""

import duckdb
import os
import tempfile

# ----------------------------------------------------
# Hard-coded extension path
# ----------------------------------------------------
EXT_PATH = os.path.abspath(
    "build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension"
)

print("=" * 60)
print("External Operator Streaming Test")
print("=" * 60)

print(f"\nLoading extension from: {EXT_PATH}")
print(f"Exists? {os.path.exists(EXT_PATH)}")

# ----------------------------------------------------
# Connect to DuckDB with unsigned extension loading
# ----------------------------------------------------
con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)

# Load the external extension
con.execute(f"LOAD '{EXT_PATH}'")

# ----------------------------------------------------
# Create a temporary file for the stream and write data
# ----------------------------------------------------
stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
stream_path = stream_file.name

# Write tab-separated values matching the table schema (id, status, value)
# Format: INTEGER, BOOLEAN (use lowercase true/false or 1/0), INTEGER
print(f"\nStream file: {stream_path}")
print("Writing stream data to file...")

# FIX 1: Use lowercase true/false instead of uppercase TRUE/FALSE
stream_file.write("100\ttrue\t1000\n")
stream_file.write("101\tfalse\t2000\n")
stream_file.write("102\ttrue\t3000\n")
stream_file.close()  # Close file - this simulates EOF

print("Stream data written (3 rows)")

# Verify file contents
print("\n--- Verifying file contents ---")
with open(stream_path, 'r') as f:
    content = f.read()
    print("File contents:")
    print(content)
    print(f"File size: {len(content)} bytes")

# ----------------------------------------------------
# Define & register the Python UDF
# UDF now accepts stream_endpoint as the last argument
# ----------------------------------------------------
def address_violating_rows(status: bool, id: int, value: int, stream_endpoint: str) -> bool:
    """
    Dummy function to 'handle' violating rows.
    Returning False means the row is considered violating and filtered out.
    """
    print(f"[UDF] Violating row encountered -> id={id}, status={status}, value={value}")
    return False  # Always reject violating rows for now

con.create_function("address_violating_rows", address_violating_rows)

con.execute(
    """
    CREATE TABLE T AS
    SELECT *
    FROM (
        VALUES
            (1, TRUE,  10),
            (2, FALSE, 20),
            (3, FALSE, 30),
            (4, TRUE,  40)
    ) AS t(id, status, value);
    """
)

query = f"""
SELECT *
FROM T
WHERE CASE
        WHEN status = TRUE THEN TRUE
        ELSE address_violating_rows(status, id, value, '{stream_path}')
      END;
"""

# FIX 2: Remove EXPLAIN queries before the actual query to avoid consuming the stream
print("\n--- Running query (should union with external stream data) ---")
print("Stream data is already written to file, executing query...")

# Execute the query (streaming will read from file)
print("\n--- EXECUTING QUERY NOW ---")
df = con.execute(query).df()

print("\n--- Results (should include both table data and stream data) ---")
print(df)
print(f"\nTotal rows: {len(df)}")

# Expected: 
# - 2 rows from table T (where status = TRUE): (1, TRUE, 10) and (4, TRUE, 40)
# - 3 rows from stream file: (100, true, 1000), (101, false, 2000), (102, true, 3000)
# Total: 5 rows

expected_rows = 5
if len(df) == expected_rows:
    print(f"✓ Test PASSED: Got expected {expected_rows} rows")
else:
    print(f"✗ Test FAILED: Expected {expected_rows} rows, got {len(df)}")

# Verify stream data is actually in results
print("\n--- Verifying stream data is unionized ---")
stream_ids = {100, 101, 102}  # IDs from stream file
table_ids = {1, 4}  # IDs from table (status=TRUE)

result_ids = set(df['id'].tolist())

stream_found = stream_ids.intersection(result_ids)
table_found = table_ids.intersection(result_ids)

print(f"Stream IDs in results: {sorted(stream_found)} (expected: {sorted(stream_ids)})")
print(f"Table IDs in results: {sorted(table_found)} (expected: {sorted(table_ids)})")

if stream_found == stream_ids:
    print("✓ All stream data (100, 101, 102) found in results - UNION working!")
else:
    print(f"✗ Missing stream data. Found: {sorted(stream_found)}, Expected: {sorted(stream_ids)}")

if table_found == table_ids:
    print("✓ All table data (1, 4) found in results")
else:
    print(f"✗ Missing table data. Found: {sorted(table_found)}, Expected: {sorted(table_ids)}")

# Show which rows came from where
print("\n--- Row breakdown ---")
for _, row in df.iterrows():
    if row['id'] in stream_ids:
        print(f"  Stream row: id={row['id']}, status={row['status']}, value={row['value']}")
    elif row['id'] in table_ids:
        print(f"  Table row:  id={row['id']}, status={row['status']}, value={row['value']}")
    else:
        print(f"  Unknown row: id={row['id']}, status={row['status']}, value={row['value']}")

# ----------------------------------------------------
# Cleanup
# ----------------------------------------------------
try:
    os.unlink(stream_path)
    print(f"\nCleaned up stream file: {stream_path}")
except:
    pass

print("\n" + "=" * 60)
print("Test completed")
print("=" * 60)