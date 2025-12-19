import requests
import msal
import uuid
import json
import time
import concurrent.futures
import re
import json as json_module
from datetime import datetime

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

def upsert_to_dataverse(environment_url, access_token, table_name, records, logger=None):
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
    valid_records = [r for r in records if r.get("crf63_businesskey")]
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
    def build_batch(batch_records):
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        parts = [f"--{batch_id}\r\nContent-Type: multipart/mixed;boundary={changeset_id}\r\n\r\n".encode()]

        for i, rec in enumerate(batch_records, 1):
            clean_rec = {k: v for k, v in rec.items() if v is not None}
            key = clean_rec["crf63_businesskey"].replace("'", "''")
            payload = json.dumps(clean_rec, separators=(',', ':'))

            part = (
                f"--{changeset_id}\r\n"
                f"Content-Type: application/http\r\n"
                f"Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                f"\r\n"
                f"PATCH {table_name}(crf63_businesskey='{key}') HTTP/1.1\r\n"
                f"Content-Type: application/json\r\n"
                f"Prefer: return=representation,odata.include-annotations=*\r\n"
                f"\r\n"
                f"{payload}\r\n"
            ).encode()
            parts.append(part)

        parts.append(f"--{changeset_id}--\r\n--{batch_id}--\r\n".encode())
        return b"".join(parts), batch_id

    def _count_subresponses(batch_text: str, expected: int):
        created = batch_text.count('HTTP/1.1 201 Created')
        updated = batch_text.count('HTTP/1.1 200 OK')
        errors = batch_text.count('HTTP/1.1 4') + batch_text.count('HTTP/1.1 5')
        accounted = created + updated + errors

        if accounted == 0:
            return {"created": 0, "updated": 0, "errors": expected}

        if accounted != expected:
            # Some responses may be 204 No Content; treat as updated for PATCH.
            no_content = batch_text.count('HTTP/1.1 204 No Content')
            updated += no_content
            accounted = created + updated + errors

        if accounted != expected:
            # Fall back to conservative: anything not clearly success is error.
            errors = max(expected - (created + updated), 0)

        return {"created": created, "updated": updated, "errors": errors}

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
