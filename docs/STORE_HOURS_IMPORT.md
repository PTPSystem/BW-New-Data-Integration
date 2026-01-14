# Store Operating Hours Excel Import

## Overview
This script imports store operating hours master data from the Excel file to Dataverse.

- **Excel Source**: `/Users/howardshen/Library/CloudStorage/OneDrive-SharedLibraries-globalpacmgt.com/IT Project - General/BI Import/BI Dimensions.xlsx`
- **Sheet Name**: `Store hours`
- **Target Table**: `crf63_storeoperatinghour`

## Script Location
- [load_store_hours.py](/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration/load_store_hours.py)

## Excel Structure
The `Store hours` sheet contains the following columns:
- **Store Number**: Integer - Store identifier
- **Day of Week**: Integer (1-7, where 1=Monday, 7=Sunday)
- **Open Time**: Integer/String - Format: HHMM (e.g., 1100 for 11:00 AM)
- **Close Time**: Integer/String - Format: HHMM (e.g., 2300 for 11:00 PM)

## Dataverse Table Structure
The script updates the following fields in `crf63_storeoperatinghour`:
- `crf63_storenumber` (Integer)
- `crf63_dayofweek` (Integer)
- `crf63_openingtimehhmm` (String) - Converted to HH:MM format
- `crf63_closingtimehhmm` (String) - Converted to HH:MM format

## Permissions Required

⚠️ **IMPORTANT**: The service principal (`ar-bw-data-integration`) currently does not have permissions to read/write to the `crf63_storeoperatinghour` table.

### Error Message
```
Principal user (ApplicationId: d056223e-f0de-4b16-b4e0-fec2a24109ff/# ar-bw-data-integration), 
is missing prvReadcrf63_storeoperatinghour privilege on OTC
```

### Steps to Grant Permissions

You need to grant the following permissions to the application user in Dataverse:

1. **Navigate to Power Platform Admin Center**
   - Go to https://admin.powerplatform.microsoft.com/
   - Select your environment

2. **Access Application Users**
   - Go to Settings → Users + permissions → Application users
   - Find "ar-bw-data-integration"

3. **Assign Security Role or Grant Table Permissions**
   
   **Option A: Create/Assign a Security Role**
   - Create a security role with permissions for `crf63_storeoperatinghour`
   - Required privileges:
     - Read
     - Write
     - Create
     - Append
     - Append To
   
   **Option B: Grant Direct Table Permissions**
   - Navigate to the table in Power Apps (https://make.powerapps.com)
   - Go to Tables → crf63_storeoperatinghour → Settings → Permissions
   - Add permissions for the application user

4. **Alternative: PowerShell Script**
   ```powershell
   # Connect to Dataverse
   Install-Module Microsoft.PowerApps.Administration.PowerShell
   Add-PowerAppsAccount

   # Assign security role to application user
   Set-AdminPowerAppRoleAssignment `
     -EnvironmentName <environment-id> `
     -PrincipalType ServicePrincipal `
     -PrincipalObjectId a9591855-4270-4364-9b09-190e89671e5b `
     -RoleName "System Administrator"  # Or custom role with table permissions
   ```

## Usage

Once permissions are granted, run the script:

```bash
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration
.venv/bin/python load_store_hours.py
```

## How It Works

1. **Load Excel Data**: Reads the "Store hours" sheet from the Excel file
2. **Fetch Existing Records**: Queries Dataverse for all existing store operating hour records
3. **Upsert Logic**: 
   - For each store/day combination, checks if a record exists
   - **Updates** existing records with new data
   - **Creates** new records for store/day combinations that don't exist
4. **Rate Limiting**: Sleeps 100ms every 10 records to avoid throttling

## Configuration

To modify the Excel path or other settings, edit the CONFIG section at the top of [load_store_hours.py](/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration/load_store_hours.py):

```python
EXCEL_PATH = "/path/to/your/BI Dimensions.xlsx"
SHEET_NAME = "Store hours"
TABLE = "crf63_storeoperatinghours"
```

## Scheduling

This script can be scheduled to run regularly using:
- **Cron Job** (Linux/Mac)
- **Windows Task Scheduler**
- **Azure Function** (for cloud-based scheduling)
- **GitHub Actions** (for Git-based automation)

Example cron entry (daily at 2 AM):
```cron
0 2 * * * cd /path/to/BW-New-Data-Integration && .venv/bin/python load_store_hours.py >> logs/store_hours_$(date +\%Y\%m\%d).log 2>&1
```

## Related Files
- [load_csv.py](/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration/load_csv.py) - Similar pattern for CSV imports
- [pipelines/pipelines.yaml](/Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/BW-New-Data-Integration/pipelines/pipelines.yaml) - MDX-based pipeline configurations
