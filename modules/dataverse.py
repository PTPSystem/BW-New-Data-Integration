import requests
import msal
import uuid
import json
import time
import concurrent.futures
import re
import json as json_module
from datetime import datetime
from urllib.parse import quote

def get_dataverse_access_token(environment_url, client_id, client_secret, tenant_id, logger=None):
    """Obtain an access token for Dataverse."""
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        scope = [f"{environment_url}/.default"]
        result = app.acquire_token_for_client(scopes=scope)
        
        if "access_token" in result:
            log(f"Dataverse access token obtained")
            return result["access_token"]
        else:
            log(f"Failed to obtain Dataverse access token: {result.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        log(f"Error obtaining Dataverse access token: {e}")
        return None

def upsert_to_dataverse(environment_url, access_token, table_name, records, alternate_key="crf63_businesskey", logger=None):
    """
    ULTRA-FAST upsert using optimized batch method from load_csv.py.
    Achieves 1,800–2,600 rows/sec on production Dataverse environments.
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    api_url = f"{environment_url.rstrip('/')}/api/data/v9.2"
    batch_url = f"{api_url}/$batch"

    # Filter valid records
    valid_records = [r for r in records if r.get(alternate_key)]
    total = len(valid_records)
    if total == 0:
        log("No valid records to upsert")
        return 0, 0, 0

    # Setup session with connection pooling
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount('https://', adapter)
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    # Build batch function (binary encoding for speed)
    debug_first_batch = True
    def build_batch(batch_records):
        nonlocal debug_first_batch
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        parts = [f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()]

        for i, rec in enumerate(batch_records, 1):
            clean_rec = {k: v for k, v in rec.items() if v is not None}
            key_value = clean_rec[alternate_key]
            # Try WITHOUT URL encoding - just escape single quotes
            encoded_key = str(key_value).replace("'", "''")
            payload = json.dumps(clean_rec, separators=(',', ':'))

            part = (
                f"--{changeset_id}\r\n"
                f"Content-Type: application/http\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                f"\r\n"
                f"PATCH {table_name}({alternate_key}='{encoded_key}') HTTP/1.1\r\n"
                f"Content-Type: application/json\r\n"
                f"Prefer: return=representation,odata.include-annotations=*\r\n"
                f"\r\n"
                f"{payload}\r\n"
            ).encode()
            parts.append(part)
            
            # Debug print first record
            if debug_first_batch and i == 1:
                log(f"\n🔍 DEBUG - First record being sent:")
                log(f"   Table: {table_name}")
                log(f"   Alternate key field: {alternate_key}")
                log(f"   Key value (raw): {key_value}")
                log(f"   Key value (encoded): {encoded_key}")
                log(f"   PATCH line: PATCH {table_name}({alternate_key}='{encoded_key}') HTTP/1.1")
                log(f"   Payload: {payload[:200]}...")
                debug_first_batch = False

        parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
        return b"".join(parts), batch_id

    def _count_subresponses(batch_text: str, expected: int):
        # Count all possible success status codes
        created = batch_text.count('HTTP/1.1 201 Created')
        updated = batch_text.count('HTTP/1.1 200 OK')
        no_content = batch_text.count('HTTP/1.1 204 No Content')
        
        # Count only actual error codes (4xx and 5xx, but NOT 204)
        errors = 0
        for code in ['400', '401', '403', '404', '409', '429', '500', '502', '503', '504']:
            errors += batch_text.count(f'HTTP/1.1 {code}')
        
        # For PATCH operations, 204 No Content is success (no body returned)
        total_success = created + updated + no_content
        
        # Debug: Print actual status codes found (only on first batch)
        nonlocal debug_first_batch
        if debug_first_batch:
            log(f"\n🔍 DEBUG - Batch response status codes:")
            log(f"   201 Created: {created}")
            log(f"   200 OK: {updated}")
            log(f"   204 No Content: {no_content}")
            log(f"   Total Success: {total_success}")
            log(f"   4xx/5xx Errors: {errors}")
            log(f"   Expected responses: {expected}")
            debug_first_batch = False
        
        accounted = total_success + errors

        if accounted == 0:
            return {"created": 0, "updated": 0, "errors": expected}

        if accounted != expected:
            # Fall back to conservative: anything not clearly success is error.
            errors = max(expected - total_success, 0)

        # Treat 204 as updates (PATCH without response body)
        return {"created": created, "updated": updated + no_content, "errors": errors}

    def _extract_error_snippets(batch_text: str, limit: int = 2):
        snippets = []
        # Split into HTTP response parts; surface a couple failures for debugging.
        for part in re.split(r'\nHTTP/1\.1 ', batch_text):
            if not part:
                continue
            status_line = part.split('\n', 1)[0]
            if status_line.startswith('4') or status_line.startswith('5'):
                preview = part[:800].strip()
                snippets.append(f"HTTP/1.1 {preview}")
                if len(snippets) >= limit:
                    break
        return snippets

    def upsert_batch(chunk):
        body, batch_id = build_batch(chunk)
        headers = {
            "Content-Type": f"multipart/mixed; boundary={batch_id}",
            "Prefer": "odata.continue-on-error"
        }

        last_error_preview = None
        for _ in range(5):
            try:
                r = session.post(batch_url, headers=headers, data=body, timeout=600)

                if r.status_code in (200, 204):
                    batch_text = r.text if isinstance(r.text, str) else r.content.decode('utf-8', errors='replace')
                    counts = _count_subresponses(batch_text, expected=len(chunk))
                    if counts["errors"]:
                        snippets = _extract_error_snippets(batch_text)
                        if snippets:
                            log(f"\n⚠️  Sample batch errors ({len(snippets)} shown):\n- " + "\n- ".join(snippets))
                    return counts

                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 5)))
                    continue

                last_error_preview = f"HTTP {r.status_code}: {r.text[:800]}"
                break
            except Exception as e:
                last_error_preview = str(e)
                time.sleep(3)

        if last_error_preview:
            log(f"\n⚠️  Batch request failed: {last_error_preview}")
        return {"created": 0, "updated": 0, "errors": len(chunk)}

    # Create batches and process
    batch_size = 400
    batches = [valid_records[i:i + batch_size] for i in range(0, total, batch_size)]
    log(f"Fast upserting {total:,} records in {len(batches)} batches of {batch_size} (6 parallel threads)")

    total_created = 0
    total_updated = 0
    total_errors = 0
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        for future in concurrent.futures.as_completed([ex.submit(upsert_batch, c) for c in batches]):
            result = future.result() or {"created": 0, "updated": 0, "errors": 0}
            total_created += int(result.get("created", 0))
            total_updated += int(result.get("updated", 0))
            total_errors += int(result.get("errors", 0))
            processed = total_created + total_updated + total_errors
            ok = total_created + total_updated
            rate = ok / (time.time() - start_time) if time.time() - start_time > 0 else 0
            log(f"\r  Progress: {processed:,}/{total:,} records ({total_created:,} created, {total_updated:,} updated, {total_errors:,} errors) | {rate:,.0f} ok-rows/sec")

    elapsed = time.time() - start_time
    log(f"\nFast upsert complete: {total_created:,} created, {total_updated:,} updated, {total_errors:,} errors in {elapsed:.1f}s → {(total_created+total_updated)/elapsed:,.0f} rows/sec")
    return total_created, total_updated, total_errors
