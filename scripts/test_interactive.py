#!/usr/bin/env python3

import duckdb
import os
import tempfile
import sys
import threading
import time

# ----------------------------------------------------
# Hard-coded extension path
# ----------------------------------------------------
EXT_PATH = os.path.abspath(
    "build/release/repository/v1.4.1/osx_arm64/external.duckdb_extension"
)

print("=" * 60)
print("Interactive External Operator Test")
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

# Pending file: stores all violating rows that need user review
pending_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
pending_path = pending_file.name
pending_file.close()

# Stream file: stores passed rows that will be read by external operator
stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
stream_path = stream_file.name
stream_file.close()

print(f"\nPending file (violating rows): {pending_path}")
print(f"Stream file (passed rows): {stream_path}")

# Threading synchronization
violating_rows_count = 0
all_violating_rows_collected = threading.Event()  # Set when all violating rows are written
user_decisions_complete = threading.Event()  # Set when user has finished making decisions
query_result = None
query_exception = None
query_lock = threading.Lock()

def address_violating_rows(status: bool, id: int, value: int, stream_endpoint: str) -> bool:
    global violating_rows_count
    
    # Write violating row to pending file
    # Format: id (INTEGER), status (BOOLEAN), value (INTEGER)
    with open(pending_path, 'a') as f:
        f.write(f"{id}\t{str(status).lower()}\t{value}\n")
        f.flush()
    
    violating_rows_count += 1
    print(f"[UDF] Violating row #{violating_rows_count} written to pending file: id={id}, status={status}, value={value}")
    
    # After writing, check if we should wait for user decisions
    # We'll detect when all rows are collected by checking if we've processed all expected rows
    # For now, we'll use a simple approach: wait a bit, then check if decisions are complete
    
    # Return False to filter out from original query (it will come from stream if user passes it)
    return False

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
            (4, TRUE,  40),
            (5, FALSE, 50),
            (6, TRUE,  60)
    ) AS t(id, status, value);
    """
)

print("\nCreated table T with 6 rows")
print("Table rows:")
print("  (1, TRUE,  10) - will pass (status=TRUE)")
print("  (2, FALSE, 20) - violating row")
print("  (3, FALSE, 30) - violating row")
print("  (4, TRUE,  40) - will pass (status=TRUE)")
print("  (5, FALSE, 50) - violating row")
print("  (6, TRUE,  60) - will pass (status=TRUE)")

# Function to run the query in a separate thread
def run_query():
    global query_result, query_exception
    
    try:
        query = f"""
        SELECT *
        FROM T
        WHERE CASE
                WHEN status = TRUE THEN TRUE
                ELSE address_violating_rows(status, id, value, '{stream_path}')
              END;
        """
        result = con.execute(query).df()
        
        with query_lock:
            query_result = result
            all_violating_rows_collected.set()
        
    except Exception as e:
        with query_lock:
            query_exception = e
            all_violating_rows_collected.set()

print("\n" + "=" * 60)
print("Starting query execution...")
print("=" * 60)
print("Query will collect violating rows, then wait for your decisions\n")

# Start query in a separate thread
query_thread = threading.Thread(target=run_query, daemon=False)
query_thread.start()

expected_violating_count = 3
max_wait_time = 10  # seconds
wait_interval = 0.1  # seconds
waited = 0
last_file_size = 0
stable_count = 0

print("[Waiting for query to collect all violating rows...]")
while waited < max_wait_time:
    if os.path.exists(pending_path):
        current_size = os.path.getsize(pending_path)
        if current_size == last_file_size and current_size > 0:
            stable_count += 1
            # File size stable for 0.3 seconds, assume all rows collected
            if stable_count >= 3:
                print(f"[Detected {violating_rows_count} violating rows collected]")
                break
        else:
            stable_count = 0
            last_file_size = current_size
    time.sleep(wait_interval)
    waited += wait_interval

# Give a bit more time for any final file writes
time.sleep(0.2)

# Verify pending file contents
print("\n--- Pending file contents (violating rows) ---")
if os.path.exists(pending_path):
    with open(pending_path, 'r') as f:
        content = f.read()
        if content:
            print("Contents:")
            print(content)
            print(f"File size: {len(content)} bytes")
            print(f"Found {violating_rows_count} violating rows")
        else:
            with open(stream_path, 'w') as f:
                f.write("")
            # Signal that decisions are complete (no decisions needed)
            user_decisions_complete.set()
            # Wait for query to finish
            query_thread.join()
            
            # Cleanup and exit early
            try:
                os.unlink(pending_path)
                os.unlink(stream_path)
            except:
                pass
            if query_exception:
                print(f"\n[Query error: {query_exception}]")
                sys.exit(1)
            if query_result is not None:
                print("\n--- Final Results ---")
                print(query_result)
            sys.exit(0)
else:
    print("(file does not exist)")
    user_decisions_complete.set()
    query_thread.join()
    sys.exit(1)

# Now user makes decisions while query is waiting
print("\n" + "=" * 60)
print("REVIEW VIOLATING ROWS AND MAKE DECISIONS")
print("=" * 60)
print("Query is waiting for your decisions...")
print("For each violating row, type:")
print("  'y' or 'Y' - Pass this row (will be streamed back)")
print("  'n' or 'N' - Reject this row (will not be streamed)")
print("  'END' - Stop processing and finish")
print("=" * 60)

# Read pending rows
pending_rows = []
with open(pending_path, 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            parts = line.split('\t')
            if len(parts) == 3:
                pending_rows.append({
                    'id': int(parts[0]),
                    'status': parts[1].lower() == 'true',
                    'value': int(parts[2])
                })

print(f"\nFound {len(pending_rows)} violating rows to review\n")

# Open stream file for writing passed rows
stream_file_handle = open(stream_path, 'w')

rows_processed = 0
rows_passed = 0

for row in pending_rows:
    row_id = row['id']
    row_status = row['status']
    row_value = row['value']
    
    print(f"\n--- Row {rows_processed + 1}/{len(pending_rows)} ---")
    print(f"ID: {row_id}, Status: {row_status}, Value: {row_value}")
    
    while True:
        try:
            user_input = input("Pass this row? (y/n/END): ").strip().upper()
            
            if user_input == 'END':
                print("\n[User typed END] Stopping processing...")
                stream_file_handle.close()
                print("[Stream file closed. External operator will read until EOF.]")
                
                # Signal that decisions are complete
                user_decisions_complete.set()
                
                # Wait for query to finish
                print("\n[Waiting for query to complete...]")
                query_thread.join()
                
                # Show results
                print("\n" + "=" * 60)
                print("QUERY RESULTS")
                print("=" * 60)
                
                if query_exception:
                    print(f"\n[Query error: {query_exception}]")
                    try:
                        os.unlink(pending_path)
                        os.unlink(stream_path)
                    except:
                        pass
                    sys.exit(1)
                
                if query_result is not None:
                    print("\n--- Final Results ---")
                    print(query_result)
                    print(f"\nTotal rows: {len(query_result)}")
                
                # Cleanup and exit
                try:
                    os.unlink(pending_path)
                    os.unlink(stream_path)
                    print(f"\nCleaned up files")
                except:
                    pass
                
                print("\n" + "=" * 60)
                print("Test completed (ended by user)")
                print("=" * 60)
                sys.exit(0)
            
            elif user_input in ['Y', 'YES']:
                # Pass this row - write to stream file
                # Format: id (INTEGER), status (BOOLEAN), value (INTEGER)
                # Use lowercase true/false for boolean
                stream_file_handle.write(f"{row_id}\t{str(row_status).lower()}\t{row_value}\n")
                stream_file_handle.flush()  # Ensure data is written immediately
                print(f"✓ Row {row_id} PASSED - written to stream")
                rows_passed += 1
                rows_processed += 1
                break
            
            elif user_input in ['N', 'NO']:
                # Reject this row - don't write to stream
                print(f"✗ Row {row_id} REJECTED - not written to stream")
                rows_processed += 1
                break
            
            else:
                print("Invalid input. Please type 'y', 'n', or 'END'")
        
        except KeyboardInterrupt:
            print("\n\n[Interrupted by user]")
            stream_file_handle.close()
            print("[Stream file closed. External operator will read until EOF.]")
            user_decisions_complete.set()
            try:
                os.unlink(pending_path)
                os.unlink(stream_path)
            except:
                pass
            sys.exit(0)
        except EOFError:
            print("\n\n[EOF reached]")
            stream_file_handle.close()
            print("[Stream file closed. External operator will read until EOF.]")
            user_decisions_complete.set()
            try:
                os.unlink(pending_path)
                os.unlink(stream_path)
            except:
                pass
            sys.exit(0)

# Close stream file after processing all rows
stream_file_handle.close()

print(f"\n\n[All violating rows processed]")
print(f"  Total processed: {rows_processed}")
print(f"  Passed (streamed): {rows_passed}")
print(f"  Rejected: {rows_processed - rows_passed}")

# Verify what was written to stream file
print("\n--- Stream file contents (passed rows) ---")
with open(stream_path, 'r') as f:
    content = f.read()
    if content:
        print("Contents:")
        print(content)
    else:
        print("(empty - no rows were passed)")
    print(f"File size: {len(content)} bytes")

# If no rows were passed, inform user
if rows_passed == 0:
    print("\n⚠ Warning: No rows were passed. Stream file is empty.")
    print("The query will still run but will only return table rows (status=TRUE).")

# Close stream file - external operator will read until EOF
print(f"\n[Stream file closed]")
print("[External operator will read from stream file until EOF]")

# Signal that user decisions are complete - query can now finish
print("\n[Signaling query to continue...]")
user_decisions_complete.set()

# Wait for query to finish
print("[Waiting for query to complete...]")
query_thread.join()

# Show results
print("\n" + "=" * 60)
print("QUERY RESULTS")
print("=" * 60)

if query_exception:
    print(f"\n[Query error: {query_exception}]")
    try:
        os.unlink(pending_path)
        os.unlink(stream_path)
    except:
        pass
    sys.exit(1)

if query_result is None:
    print("\n[Error: Query did not return results]")
    try:
        os.unlink(pending_path)
        os.unlink(stream_path)
    except:
        pass
    sys.exit(1)

print("\n--- Results (should include both table data and stream data) ---")
print(query_result)
print(f"\nTotal rows: {len(query_result)}")

expected_table_ids = {1, 4, 6}  # IDs from table where status=TRUE
result_ids = set(query_result['id'].tolist())

table_found = expected_table_ids.intersection(result_ids)
stream_ids = result_ids - expected_table_ids

print("\n--- Verification ---")
print(f"Table IDs in results: {sorted(table_found)} (expected: {sorted(expected_table_ids)})")
print(f"Stream IDs in results: {sorted(stream_ids)} (from user decisions)")

if table_found == expected_table_ids:
    print("✓ All table data (status=TRUE) found in results")
else:
    print(f"✗ Missing table data. Found: {sorted(table_found)}, Expected: {sorted(expected_table_ids)}")

if stream_ids:
    print(f"✓ Stream data found: {sorted(stream_ids)} rows from user decisions")
else:
    print("ℹ No stream data (user rejected all violating rows)")

# Show which rows came from where
print("\n--- Row breakdown ---")
for _, row in query_result.iterrows():
    if row['id'] in expected_table_ids:
        print(f"  Table row:  id={row['id']}, status={row['status']}, value={row['value']}")
    else:
        print(f"  Stream row: id={row['id']}, status={row['status']}, value={row['value']}")

# ----------------------------------------------------
# Cleanup
# ----------------------------------------------------
try:
    os.unlink(pending_path)
    os.unlink(stream_path)
    print(f"\nCleaned up files")
except:
    pass

print("\n" + "=" * 60)
print("Test completed")
print("=" * 60)
