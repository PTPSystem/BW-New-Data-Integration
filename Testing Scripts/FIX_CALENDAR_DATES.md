# Fix Calendar Date Timezone Issue

## Problem
Dates in the OARS BI Data table were showing **one day earlier** than expected:
- Store 002256 on **1/26** appeared as **1/25** in Dataverse

## Root Cause
Dates were stored as `2026-01-26T00:00:00Z` (midnight UTC), which converts to the previous day in negative UTC offset timezones:
- **EST (UTC-5)**: `2026-01-26T00:00:00Z` → `2026-01-25T19:00:00 EST` = **1/25** ❌

## Solution
Change dates to use **12PM (noon) UTC** instead of midnight:
- **EST (UTC-5)**: `2026-01-26T12:00:00Z` → `2026-01-26T07:00:00 EST` = **1/26** ✅

This works for all US timezones (EST, CST, MST, PST, AKST, HST).

## What Was Fixed

### 1. Future Data (Automatic)
✅ **[modules/pipeline_runner.py](modules/pipeline_runner.py)** - All new data imports now use 12PM UTC automatically

### 2. Existing Data (Run Script)
📋 **[fix_calendar_dates_to_noon.py](fix_calendar_dates_to_noon.py)** - Script to update existing records

## How to Fix Existing Data

### Prerequisites
Make sure your `.env` file contains the required credentials:
```bash
DATAVERSE_CLIENT_ID=your_client_id
DATAVERSE_CLIENT_SECRET=your_client_secret  
AZURE_TENANT_ID=your_tenant_id
DATAVERSE_ENVIRONMENT_URL=https://orgbf93e3c3.crm.dynamics.com  # Optional, defaults to production
```

Or set them as environment variables before running:
```bash
export DATAVERSE_CLIENT_ID="your_client_id"
export DATAVERSE_CLIENT_SECRET="your_client_secret"
export AZURE_TENANT_ID="your_tenant_id"
```

### Step 1: Dry Run (Preview Changes)
First, run in dry-run mode to see what will be changed:

```bash
python3 fix_calendar_dates_to_noon.py --dry-run
```

This will:
- ✅ Fetch all records from Dataverse
- ✅ Show how many records need updating
- ✅ Display sample before/after dates
- ❌ NOT make any changes

### Step 2: Apply the Fix
If the dry run looks good, run without the flag:

```bash
python3 fix_calendar_dates_to_noon.py
```

This will:
- Fetch all records from `crf63_oarsbidatas` table
- Update dates from midnight (00:00) to noon (12:00) UTC
- Process in batches of 100 records
- Skip records already at noon
- Use the existing `crf63_businesskey` for upserts

## Expected Output

```
======================================================================
Fix Calendar Date Timezone Issue - Update to 12PM UTC
======================================================================

1. Loading configuration...
   Environment: https://orgbf93e3c3.crm.dynamics.com
   Table: crf63_oarsbidatas

2. Authenticating to Dataverse...
   Dataverse access token obtained

3. Fetching all records from Dataverse...
   Fetched 1000 records so far...
   Fetched 2000 records so far...
   ✓ Total records fetched: 2500

4. Updating calendar dates to 12PM UTC...
   Records to update: 2450
   Records skipped (already correct or invalid): 50

  Batch 1/25 (100 records)...
  ...
  
✓ UPDATE COMPLETE - 2450 records updated to 12PM UTC

======================================================================
COMPLETE!
======================================================================
```

## Verification

After running the script, check a few records in Dataverse:
1. Look at any date field
2. The time portion should show around 7:00 AM EST (or 6:00 AM CST)
3. The **date** should now be correct

## Safety Features

- ✅ **Dry run mode** - Preview before making changes
- ✅ **Business key upserts** - Uses existing alternate key for safe updates
- ✅ **Skip already fixed** - Won't update records already at noon
- ✅ **Batch processing** - Updates in chunks for better performance
- ✅ **Error handling** - Skips invalid dates, logs issues

## Rollback (if needed)

If something goes wrong, you can re-import the data from OLAP:

```bash
# Re-import data for specific date range
python3 olap_to_dataverse.py --pipeline daily_sales --length 1wk
```

The new import will use the fixed 12PM timestamp automatically.
