# Dataverse Table Creation

This folder contains scripts to create the `crf63_oarsbidata` table in Dataverse.

## Script: create_oarsbidata_table.py

Creates the OARS BI Data table with 35 columns using Dataverse Web API and interactive user authentication.

### Prerequisites

1. **Install Required Python Package:**
   ```bash
   pip install msal requests
   ```

2. **Verify Access:**
   - You must have System Administrator or System Customizer role in Dataverse
   - Dataverse environment: https://orgbf93e3c3.crm.dynamics.com

### Usage

```bash
# Navigate to this folder
cd dataverse_table_creation

# Run the script
python create_oarsbidata_table.py
```

### Authentication Flow

The script uses **interactive device code authentication**:

1. Script will display a code and URL
2. Open browser and navigate to the URL
3. Enter the code when prompted
4. Sign in with your Microsoft account
5. Script will continue automatically

**Example Output:**
```
Authentication required...
To sign in, use a web browser to open the page https://microsoft.com/devicelogin 
and enter the code ABC123XYZ to authenticate.

✓ Authenticated as: your.email@company.com
```

### What the Script Does

1. **Authenticates** using your user credentials (no app registration needed)
2. **Creates table** `crf63_oarsbidata` with display name "OARS BI Data"
3. **Creates 35 columns:**
   - **Key Fields (3):** Store Number, Calendar Date, Data Source
   - **Sales Metrics (4):** TY Net Sales, L2Y/L3Y/LY Comp Sales
   - **Cost Metrics (6):** Target/Actual Food Cost, FLMD, Labor, Mileage, Discounts
   - **Operations Metrics (6):** Total Hours, Store Days, Make/Rack/OTD Time, Avg TTDT
   - **Order Metrics (6):** TY/LY Orders, Deliveries, BOZOCORO, OTD Count, Dispatched
   - **Financial Metrics (5):** Target Profit, Actual FLM, FLMDPC, Commission, Cash Over/Short
   - **Customer Satisfaction (6):** OSAT/Accuracy Surveys, Total/Answered Calls
   - **Metadata (1):** Last Refreshed timestamp

4. **Verifies** table creation

### Expected Output

```
======================================================================
Dataverse Table Creation Script
Table: crf63_oarsbidata (OARS BI Data)
======================================================================

Authentication required...
[Follow authentication prompts]
✓ Authenticated as: your.email@company.com

Creating table crf63_oarsbidata...
✓ Table created successfully!

Waiting for table to be ready...

Creating columns...
  [1/35] Creating crf63_storenumber (Store Number)...
  [2/35] Creating crf63_calendardate (Calendar Date)...
  [3/35] Creating crf63_datasource (Data Source)...
  ...
  [35/35] Creating crf63_lastrefreshed (Last Refreshed)...

✓ Created 35/35 columns successfully

Verifying table creation...
✓ Table found: crf63_oarsbidata
  Display Name: OARS BI Data
  Total Columns: 35

======================================================================
✓ Table creation complete!
======================================================================

Next steps:
1. Verify table in Power Apps: https://make.powerapps.com
2. Proceed to Step 2: OLAP to Dataverse Migration
======================================================================
```

### Troubleshooting

**Error: "Authentication failed"**
- Ensure you have access to the Dataverse environment
- Check your network connection
- Try clearing browser cache and re-authenticating

**Error: "Failed to create table"**
- Verify you have System Administrator or System Customizer role
- Check if table already exists (delete and retry)
- Review error message for specific permission issues

**Error: "Failed to create column"**
- Some columns may fail if they already exist
- Check error message for column-specific issues
- Script will continue and report which columns succeeded

**Verification Failed:**
- Wait a few more seconds and check manually in Power Apps
- Navigate to: https://make.powerapps.com → Tables → Search "OARS BI Data"

### Manual Verification

After running the script:

1. Open **Power Apps**: https://make.powerapps.com
2. Select your environment (orgbf93e3c3)
3. Navigate to **Tables** (left menu)
4. Search for **"OARS BI Data"** or **"crf63_oarsbidata"**
5. Click on the table to view all 35 columns

### Schema Reference

**Table:**
- Schema Name: `crf63_oarsbidata`
- Display Name: `OARS BI Data`
- Ownership Type: User Owned

**Key Columns (Composite Key):**
- `crf63_storenumber` (String, 20) - Store Number
- `crf63_calendardate` (Date) - Calendar Date
- `crf63_datasource` (String, 50) - Data Source (e.g., "BI At Scale", "BI Sales Channel")

See migration document for complete column list and data types.

### Next Steps

After successful table creation:

1. ✅ **Verify** in Power Apps portal
2. 📋 **Proceed to Step 2** in `NewIntegration/Migrate-to-Truenas.md`
3. 🔄 **Test OLAP sync** using `modules/olap_to_dataverse.py`

### Alternative: Power Platform CLI

If you prefer using `pac` CLI instead:

```bash
# Install pac CLI
brew tap microsoft/powerplatform-cli
brew install pac

# Authenticate
pac auth create --environment https://orgbf93e3c3.crm.dynamics.com

# Use the shell script (if available)
../create_oars_bi_table_pac.sh
```

Note: The Python script is recommended as it provides better error handling and progress feedback.
