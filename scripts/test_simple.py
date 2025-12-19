#!/usr/bin/env python3
"""
Simple test to verify the external operator works correctly.
This test:
1. Creates a table with some data
2. Writes stream data to a file
3. Sets up the external operator via pragma
4. Runs a query that triggers the UNION with external operator
5. Verifies the results contain both table and stream data
"""

import duckdb
import os
import tempfile

# Extension path
EXT_PATH = os.path.abspath(
    "build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension"
)

print("=" * 60)
print("Simple External Operator Test")
print("=" * 60)

# Connect and load extension
con = duckdb.connect(
    database=":memory:",
    config={"allow_unsigned_extensions": "true"},
)
con.execute(f"LOAD '{EXT_PATH}'")

# Create stream file with test data
stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
stream_path = stream_file.name

# Write tab-separated data: id (INTEGER), status (BOOLEAN), value (INTEGER)
stream_file.write("100\tTRUE\t1000\n")
stream_file.write("101\tFALSE\t2000\n")
stream_file.write("102\tTRUE\t3000\n")
stream_file.flush()
stream_file.close()

print(f"Created stream file: {stream_path}")
print("Stream data: 3 rows (100, 101, 102)")

# Register UDF that will trigger the external operator
# UDF now accepts stream_endpoint as the last argument
def address_violating_rows(status: bool, id: int, value: int, stream_endpoint: str) -> bool:
    return False  # Always reject violating rows

con.create_function("address_violating_rows", address_violating_rows)

# Create test table
con.execute("""
    CREATE TABLE T AS
    SELECT *
    FROM (
        VALUES
            (1, TRUE,  10),
            (2, FALSE, 20),
            (3, FALSE, 30),
            (4, TRUE,  40)
    ) AS t(id, status, value);
""")
print("Created table T with 4 rows")

# Query that will trigger the external operator
# This filter uses address_violating_rows, which should trigger the UNION rewrite
# Pass stream_endpoint as the last argument to the UDF
query = f"""
SELECT *
FROM T
WHERE CASE
        WHEN status = TRUE THEN TRUE
        ELSE address_violating_rows(status, id, value, '{stream_path}')
      END;
"""

print("\nRunning query (should union table data with stream data)...")
result = con.execute(query).df()

print("\nResults:")
print(result)
print(f"\nTotal rows: {len(result)}")

# Verify results
expected_table_rows = {(1, True, 10), (4, True, 40)}  # Rows where status = TRUE
expected_stream_rows = {(100, True, 1000), (101, False, 2000), (102, True, 3000)}

result_rows = set()
for _, row in result.iterrows():
    result_rows.add((row['id'], row['status'], row['value']))

print("\nVerification:")
print(f"Expected table rows: {expected_table_rows}")
print(f"Expected stream rows: {expected_stream_rows}")
print(f"Actual result rows: {result_rows}")

# Check if we got the expected rows
table_found = expected_table_rows.intersection(result_rows)
stream_found = expected_stream_rows.intersection(result_rows)

print(f"\nTable rows found: {len(table_found)}/{len(expected_table_rows)}")
print(f"Stream rows found: {len(stream_found)}/{len(expected_stream_rows)}")

if len(table_found) == len(expected_table_rows) and len(stream_found) == len(expected_stream_rows):
    print("\n✓ TEST PASSED: All expected rows found!")
else:
    print("\n✗ TEST FAILED: Missing some expected rows")
    if len(table_found) < len(expected_table_rows):
        missing = expected_table_rows - table_found
        print(f"  Missing table rows: {missing}")
    if len(stream_found) < len(expected_stream_rows):
        missing = expected_stream_rows - stream_found
        print(f"  Missing stream rows: {missing}")

# Cleanup
try:
    os.unlink(stream_path)
    print(f"\nCleaned up: {stream_path}")
except:
    pass

print("\n" + "=" * 60)
print("Test completed")
print("=" * 60)

