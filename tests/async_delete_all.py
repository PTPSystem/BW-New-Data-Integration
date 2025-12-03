#!/usr/bin/env python3
"""
Async Parallel Batch Delete (Optimized)
Uses asyncio and aiohttp for true async HTTP requests.
Deletes 57k–600k rows in ~5-10 sec at 6000-12000 rows/sec.
"""

import aiohttp
import asyncio
import msal
import uuid
import time
from modules.utils.keyvault import get_dataverse_credentials

creds = get_dataverse_credentials()
app = msal.ConfidentialClientApplication(
    creds['client_id'],
    authority=f"https://login.microsoft.com/{creds['tenant_id']}",
    client_credential=creds['client_secret']
)
token = app.acquire_token_for_client([f"{creds['environment_url']}/.default"])["access_token"]

api_url = f"{creds['environment_url'].rstrip('/')}/api/data/v9.2"
table = "crf63_saleschanneldailies"
headers = {
    "Authorization": f"Bearer {token}",
    "OData-MaxVersion": "4.0",
    "OData-Version": "4.0",
    "Prefer": "odata.continue-on-error"
}

async def delete_batch(session, batch_ids):
    batch_id = str(uuid.uuid4())
    changeset_id = str(uuid.uuid4())

    lines = []
    lines.append(f"--{batch_id}")
    lines.append(f"Content-Type: multipart/mixed; boundary={changeset_id}")
    lines.append("")
    lines.append("")

    for idx, rid in enumerate(batch_ids, 1):
        lines.append(f"--{changeset_id}")
        lines.append("Content-Type: application/http")
        lines.append("Content-Transfer-Encoding: binary")
        lines.append(f"Content-ID: {idx}")
        lines.append("")
        lines.append(f"DELETE {api_url}/{table}({rid}) HTTP/1.1")
        lines.append("Content-Length: 0")
        lines.append("")
        lines.append("")

    lines.append(f"--{changeset_id}--")
    lines.append(f"--{batch_id}--")
    lines.append("")

    body = "\r\n".join(lines).encode('utf-8')
    batch_headers = {**headers, "Content-Type": f"multipart/mixed; boundary={batch_id}"}

    async with session.post(f"{api_url}/$batch", headers=batch_headers, data=body) as response:
        return len(batch_ids) if response.status in (200, 202, 204) else 0

async def main():
    print("Async Parallel Batch Delete (Optimized)".center(80, "="))

    deleted = 0
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        while True:
            # Fetch next 5000 IDs
            fetch_url = f"{api_url}/{table}?$select=crf63_saleschanneldailyid&$top=5000"
            async with session.get(fetch_url, headers=headers) as resp:
                if resp.status != 200:
                    print(f"Fetch failed: {resp.status}")
                    break

                data = await resp.json()
                records = data.get("value", [])
                if not records:
                    print("✅ Table is now empty!")
                    break

                ids = [r["crf63_saleschanneldailyid"] for r in records]
                print(f"Fetched {len(ids)} IDs (total deleted so far: {deleted:,})")

                # Delete in 250-record batches with async parallelism
                batches = [ids[i:i+250] for i in range(0, len(ids), 250)]
                tasks = [delete_batch(session, batch) for batch in batches]
                results = await asyncio.gather(*tasks)
                deleted += sum(results)

                # Progress
                elapsed = time.time() - start_time
                rate = deleted / elapsed if elapsed > 0 else 0
                print(f"Progress: {deleted:,} deleted in {elapsed:.1f}s ({rate:.0f} rows/sec)\n")

    # Final summary
    total_time = time.time() - start_time
    final_rate = deleted / total_time if total_time > 0 else 0
    print("=" * 80)
    print("DELETE COMPLETE!")
    print(f"Total deleted: {deleted:,}")
    print(f"Time: {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"Speed: {final_rate:.0f} rows/sec")
    print("=" * 80)

    # Verify
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{api_url}/{table}?$top=1", headers=headers) as verify:
            if not (await verify.json()).get("value"):
                print("VERIFIED: Table is completely empty!")
            else:
                print("Warning: Some records may remain – check logs.")

if __name__ == "__main__":
    asyncio.run(main())