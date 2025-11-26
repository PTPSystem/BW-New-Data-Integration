# Migration Plan: Beachwood Data Integration to TrueNAS Container

## Executive Summary

This document outlines the migration plan to move the Beachwood Data Integration system from a Windows-based environment to a containerized deployment on a TrueNAS server. The migration addresses Windows-specific dependencies, implements cloud-native secret management, and replaces Excel COM automation with direct database/API integrations.

**Migration Date**: TBD  
**Target Environment**: TrueNAS Server (Container Job)  
**Project Status**: Planning Phase

---

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Target Architecture](#target-architecture)
3. [Migration Strategy](#migration-strategy)
4. [Technical Requirements](#technical-requirements)
5. [Component Migration Details](#component-migration-details)
6. [Step-by-Step Migration Execution](#step-by-step-migration-execution)
   - [Step 1: Create Dataverse Table](#step-1-create-dataverse-table)
   - [Step 2: OLAP to Dataverse Migration](#step-2-olap-to-dataverse-migration)
   - [Step 3: Labor Processing Migration](#step-3-labor-processing-migration)
   - [Step 4: Forecast Modules Migration](#step-4-forecast-modules-migration)
   - [Step 5: Integration Testing](#step-5-integration-testing)
   - [Step 6: Containerization](#step-6-containerization)
7. [Testing Strategy](#testing-strategy)
8. [Deployment Procedures](#deployment-procedures)
9. [Rollback Plan](#rollback-plan)
10. [Post-Migration Validation](#post-migration-validation)

---

## 🚀 Quick Start: Migration Execution

**Migration Order (Step-by-Step):**
1. ✅ Create Dataverse Table (REQUIRED FIRST) - [Jump to Step 1](#step-1-create-dataverse-table)
2. ✅ OLAP to Dataverse - [Jump to Step 2](#step-2-olap-to-dataverse-migration)
3. Labor Processing - [Jump to Step 3](#step-3-labor-processing-migration)
4. Forecast Modules - [Jump to Step 4](#step-4-forecast-modules-migration)
5. Integration Testing - [Jump to Step 5](#step-5-integration-testing)
6. Containerization - [Jump to Step 6](#step-6-containerization)

**Current Status**: Steps 1 & 2 Complete ✅ - Ready for Step 3 (Labor Processing)

---

---

## Current State Analysis

### Existing Architecture

#### Technology Stack
- **Platform**: Windows Server
- **Runtime**: Python 3.x with pywin32
- **Automation**: Windows Task Scheduler
- **File Storage**: Local OneDrive sync (C:\Users\Administrator\globalpacmgt.com\)
- **Secret Management**: Local environment variables and config files
- **Excel Integration**: Win32 COM automation

#### Current Components

| Component | File(s) | Windows Dependencies | Issues |
|-----------|---------|---------------------|---------|
| **Main Orchestrator** | `main.py` | Local file paths (C:\\) | Hardcoded Windows paths |
| **Labor Processing** | `modules/labor_processing.py` | OneDrive local sync, Windows paths | Requires OneDrive local folder |
| **PowerBI Update** | `modules/powerbi_update.py` | **win32com.client**, Excel COM | Cannot run in containers |
| **DoorDash Update** | `modules/doordash_update.py` | Windows file paths | Local file dependencies |
| **Labor Forecast** | `modules/labor_forecast.py` | Windows file paths | Local file dependencies |
| **Sales Forecast** | `modules/sales_forecast.py` | Windows file paths, CSV files | File system dependencies |
| **Configuration** | `Config/config.json` | Windows paths, clear text secrets | Security concerns |

#### Windows-Specific Dependencies

1. **pywin32 / win32com.client**
   - Used in: `powerbi_update.py`, `powerbi_update_Kill Excel.py`
   - Purpose: Excel COM automation for OLAP data refresh
   - **Critical Blocker**: Cannot be containerized

2. **Local File Paths**
   - Pattern: `C:\\Users\\Administrator\\globalpacmgt.com\\`
   - Locations: OneDrive synced folders
   - Count: 15+ hardcoded paths across modules

3. **Excel Files**
   - `BI At Scale Import.xlsx` (OLAP connection)
   - `BI Sales Channel - Daily.xlsx`
   - `BI Service Metrics.xlsx`
   - `BI Offers.xlsx`
   - `BI inventory.xlsx`
   - `BI Dimensions.xlsx`

#### Data Flows

```
┌─────────────────────────────────────────────────────────────┐
│                    Current Windows System                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌─────────────────┐                   │
│  │ Task         │───▶│ main.py         │                   │
│  │ Scheduler    │    └─────────────────┘                   │
│  └──────────────┘            │                              │
│                               │                              │
│  ┌───────────────────────────┴──────────────────────────┐  │
│  │                                                        │  │
│  ▼                    ▼                    ▼              ▼  │
│ ┌────────┐      ┌──────────┐      ┌──────────┐   ┌─────────┐ │
│ │Labor   │      │PowerBI   │      │DoorDash  │   │Forecast │ │
│ │Process │      │Update    │      │Update    │   │Modules  │ │
│ └────────┘      └──────────┘      └──────────┘   └─────────┘ │
│      │                │                 │              │     │
│      │                ▼                 │              │     │
│      │       ┌─────────────────┐       │              │     │
│      │       │ Excel COM       │       │              │     │
│      │       │ (win32com)      │       │              │     │
│      │       └─────────────────┘       │              │     │
│      │                │                 │              │     │
│      ▼                ▼                 ▼              ▼     │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │         OneDrive Local Sync (C:\Users\...)             │ │
│ │    - Excel files                                        │ │
│ │    - CSV files                                          │ │
│ │    - Archive folders                                    │ │
│ └─────────────────────────────────────────────────────────┘ │
│                               │                              │
└───────────────────────────────┼──────────────────────────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │ Email Notifications │
                     │ (MS Graph API)      │
                     └─────────────────────┘
```

#### Current Secret Storage

Secrets are currently stored in:
- `Config/config.json` (client IDs, tenant IDs, URLs)
- Environment variables (passwords, client secrets)
- **Risk**: Secrets in plain text configuration files

---

## Target Architecture

### Container-Based Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    TrueNAS Container                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌─────────────────┐                   │
│  │ TrueNAS Cron │───▶│ Container       │                   │
│  │ Job          │    │ Entrypoint      │                   │
│  └──────────────┘    └─────────────────┘                   │
│                               │                              │
│  ┌───────────────────────────┴──────────────────────────┐  │
│  │                                                        │  │
│  ▼                    ▼                    ▼              ▼  │
│ ┌────────┐      ┌──────────┐      ┌──────────┐   ┌─────────┐ │
│ │Labor   │      │OLAP-to-  │      │DoorDash  │   │Forecast │ │
│ │Process │      │Dataverse │      │Process   │   │Modules  │ │
│ │(Updated)│     │(NEW)     │      │(Updated) │   │(Updated)│ │
│ └────────┘      └──────────┘      └──────────┘   └─────────┘ │
│      │                │                 │              │     │
└──────┼────────────────┼─────────────────┼──────────────┼─────┘
       │                │                 │              │
       ▼                ▼                 ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Cloud Services                              │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────┐  ┌──────────┐  ┌───────────┐  ┌──────────┐ │
│  │Azure Key   │  │SharePoint│  │Dataverse  │  │MS Graph  │ │
│  │Vault       │  │API       │  │API        │  │API       │ │
│  │(Secrets)   │  │(Files)   │  │(Data)     │  │(Email)   │ │
│  └────────────┘  └──────────┘  └───────────┘  └──────────┘ │
│                                                               │
│  ┌────────────┐  ┌──────────┐                               │
│  │OLAP Server │  │Azure     │                               │
│  │(XMLA)      │  │Blob/File │                               │
│  └────────────┘  └──────────┘                               │
└─────────────────────────────────────────────────────────────┘
```

### Key Architectural Changes

1. **Excel COM → Direct OLAP Queries**
   - Replace: win32com Excel automation
   - With: Direct XMLA/MDX queries to OLAP server
   - Store results: Dataverse (not Excel files)

2. **OneDrive Local Sync → SharePoint API**
   - Replace: Local file paths
   - With: Microsoft Graph API / SharePoint REST API
   - Benefit: Direct cloud access, no local sync needed

3. **Local Config → Azure Key Vault**
   - Replace: config.json secrets
   - With: Azure Key Vault (kv-bw-data-integration)
   - Already partially implemented!

4. **Windows Paths → Cloud Storage**
   - Replace: C:\\ paths
   - With: Environment variables and cloud URIs

---

## Migration Strategy

### Phase 1: Foundation (Week 1-2)
**Goal**: Set up core infrastructure for containerization

- [x] Complete Azure Key Vault migration (100% done - all 10 secrets configured)
- [x] Create NewIntegration directory structure (complete)
- [x] Develop containerization framework (Dockerfile, docker-compose ready)
- [ ] Set up cloud storage integration

### Phase 2: Component Migration (Week 3-5)
**Goal**: Migrate each module to cloud-native approach

- [ ] Migrate PowerBI Update to OLAP-to-Dataverse
- [ ] Update Labor Processing for SharePoint API
- [ ] Update DoorDash Update for cloud storage
- [ ] Update Forecast modules for cloud storage

### Phase 3: Testing & Validation (Week 6)
**Goal**: Ensure functionality parity

- [ ] Unit testing for each component
- [ ] Integration testing
- [ ] Performance testing
- [ ] Security validation

### Phase 4: Deployment (Week 7)
**Goal**: Deploy to TrueNAS

- [ ] Build and publish container image
- [ ] Configure TrueNAS container job
- [ ] Parallel run with Windows system
- [ ] Cutover and decommission Windows system

---

## Technical Requirements

### Infrastructure Requirements

#### TrueNAS Configuration
- **Container Runtime**: Docker or Kubernetes
- **Scheduling**: TrueNAS Cron Jobs or Kubernetes CronJob
- **Networking**: Outbound HTTPS access to:
  - Azure Key Vault (vault.azure.net)
  - Microsoft Graph API (graph.microsoft.com)
  - Dataverse (crm.dynamics.com)
  - OLAP Server (ednacubes.papajohns.com:10502)
  - SharePoint Online (sharepoint.com)
- **Storage**: Minimal (logs and temporary files only)

#### Azure Services

1. **Azure Key Vault** (kv-bw-data-integration)
   - Status: ✅ 100% Complete - All secrets configured
   - Resource Group: rg-bw-data-integration
   - All 10 secrets stored:
     - `azure-tenant-id` (c8b6ba98-3fc0-4153-83a9-01374492c0f5)
     - `app-client-id` (d056223e-f0de-4b16-b4e0-fec2a24109ff)
     - `app-client-secret` (unified secret for both SharePoint and Dataverse)
     - `dataverse-client-id` (environment identifier)
     - `sharepoint-site-url` (SharePoint site URL)
     - `olap-username` (OLAP server username)
     - `olap-password` (OLAP server password)
     - `files-url` (Labor files download URL)
     - `files-username` (Labor files username)
     - `files-password` (Labor files password)
   - Note: Using unified app registration (ar-bw-data-integration) for both SharePoint and Dataverse access

2. **Dataverse Environment**
   - URL: https://orgbf93e3c3.crm.dynamics.com
   - Tables:
     - `crf63_laborheadcounts` (existing)
     - `crf63_bwsalesforecasts` (existing)
     - `crf63_olapbidatas` (NEW - for OLAP data)

3. **SharePoint Online**
   - Site: IT Project - General
   - Library: BI Import
   - Access: Via Microsoft Graph API

4. **App Registration**
   - Name: ar-bw-data-integration
   - Client ID: d056223e-f0de-4b16-b4e0-fec2a24109ff
   - Tenant ID: c8b6ba98-3fc0-4153-83a9-01374492c0f5
   - Unified registration for both SharePoint and Dataverse
   - API Permissions:
     - Dynamics CRM: user_impersonation (Delegated)
     - Microsoft Graph: Mail.Send (Delegated)
     - Microsoft Graph: Files.ReadWrite.All (Delegated)

### Software Requirements

#### Python Dependencies
```
requests>=2.31.0
beautifulsoup4>=4.12.0
pandas>=2.0.0
msal>=1.24.0
openpyxl>=3.1.0
scikit-learn>=1.3.0
python-dotenv>=1.0.0
azure-keyvault-secrets>=4.7.0
azure-identity>=1.14.0
Office365-REST-Python-Client>=2.5.0
```

**Remove**: pywin32 (Windows-specific)

#### Docker Base Image
```dockerfile
FROM python:3.11-slim
```

Benefits:
- Small footprint (~150 MB base)
- Security updates from Python foundation
- No Windows dependencies

---

## Component Migration Details

### 1. PowerBI Update Module → OLAP-to-Dataverse

#### Current State (powerbi_update.py)
```python
import win32com.client  # ❌ Windows-only
excel = win32com.client.Dispatch("Excel.Application")
excel.Workbooks.Open(excel_path)
# Refresh OLAP connection via Excel
```

#### Target State (olap_to_dataverse.py)
```python
# Direct XMLA/MDX query (already implemented!)
response = execute_xmla_mdx(
    server="https://ednacubes.papajohns.com:10502",
    catalog="OARS Franchise",
    username=username,
    password=get_secret('olap-password'),  # From Key Vault
    mdx_query=mdx_query
)
# Parse and upsert to Dataverse
```

#### Migration Steps
1. ✅ Extract MDX queries from Excel (see EXTRACTED_MDX_QUERY.md)
2. ✅ Create olap_to_dataverse.py script
3. ✅ Test XMLA connection
4. [x] Create Dataverse table `crf63_olapbidatas`
5. ✅ Add OLAP password to Key Vault (olap-username and olap-password)
6. [ ] Test end-to-end data flow
7. [ ] Update main.py to use new module

#### Data Mapping

| Excel File | OLAP Cube | Dataverse Table | Status |
|------------|-----------|-----------------|---------|
| BI At Scale Import.xlsx | OARS Franchise | crf63_olapbidatas | NEW |
| BI Sales Channel - Daily.xlsx | OARS Franchise | crf63_olapbidatas | NEW |
| BI Service Metrics.xlsx | OARS Franchise | crf63_olapbidatas | NEW |
| BI Offers.xlsx | OARS Offers | crf63_olapbidatas | NEW |
| BI inventory.xlsx | OARS Franchise | crf63_olapbidatas | NEW |

**Action**: Design unified Dataverse table schema to handle all metrics.

---

### 2. Labor Processing Module

#### Current Dependencies
- FTP access to files.papajohns.com
- Local download directory: `C:\\BI Temp\\Download`
- Local extract directory: `C:\\BI Temp\\Extract`
- Output to: OneDrive synced folder
- Store numbers from: Local Excel file

#### Target State
```python
# Credentials from Key Vault
username = get_secret('labor-processing-username')
password = get_secret('labor-processing-password')

# Use temp directories in container
download_dir = os.getenv('TEMP_DIR', '/tmp/labor/download')
extract_dir = os.getenv('TEMP_DIR', '/tmp/labor/extract')

# Store numbers from SharePoint
store_numbers = get_file_from_sharepoint(
    'IT Project - General/BI Import/BI Dimensions.xlsx'
)

# Output to SharePoint or Dataverse
upload_to_sharepoint(
    output_file,
    'IT Project - General/BI Import/labor_time_by_day_part.csv'
)
```

#### Migration Steps
1. ✅ Add labor FTP credentials to Key Vault (files-url, files-username, files-password)
2. [ ] Update file paths to use environment variables
3. [ ] Implement SharePoint file access helper
4. [ ] Update store number retrieval from SharePoint
5. [ ] Update output to upload to SharePoint
6. [ ] Test with sample data

---

### 3. DoorDash Update Module

#### Current Dependencies
- Base file: `C:\\Users\\...\\DD\\DD Detailed.csv`
- Archive: `C:\\Users\\...\\DD\\Archive`
- Working dir: `C:\\Users\\...\\DD`
- Email download folder monitoring

#### Target State
- Base file: SharePoint
- Archive: SharePoint archive folder
- Working dir: Container temp directory
- Use Microsoft Graph API for email attachments

#### Migration Steps
1. [ ] Implement SharePoint CSV file access
2. [ ] Implement archive to SharePoint
3. [ ] Replace email folder monitoring with Graph API
4. [ ] Test CSV append and update logic

---

### 4. Labor Forecast Module

#### Current Dependencies
- Input dir: `C:\\BI Temp\\LaborForecast`
- Source: `C:\\Users\\...\\Labor Planning Export\\Export`
- Archive: `C:\\Users\\...\\Labor Planning Export\\Archive`
- Output: Dataverse (crf63_laborheadcounts) ✅

#### Target State
- Input dir: SharePoint folder
- Archive: SharePoint archive folder
- Output: Dataverse (already using this!) ✅

#### Migration Steps
1. [ ] Update file source to SharePoint API
2. [ ] Update archive to SharePoint
3. [ ] Verify Dataverse credentials use Key Vault
4. [ ] Test forecast processing

---

### 5. Sales Forecast Module

#### Current Dependencies
- History files: Multiple local Excel files
- Calendar file: Local Excel file
- Output: Dataverse (crf63_bwsalesforecasts) ✅

#### Target State
- History files: SharePoint
- Calendar file: SharePoint
- Output: Dataverse ✅

#### Migration Steps
1. [ ] Update Excel file access to SharePoint
2. [ ] Verify Dataverse credentials use Key Vault
3. [ ] Test forecast generation

---

### 6. Configuration Management

#### Current State (Config/config.json)
```json
{
  "labor_processing": {
    "download_dir": "C:\\BI Temp\\Download",  // ❌ Windows path
    "output_file": "C:\\Users\\..."  // ❌ Windows path
  },
  "ms_graph": {
    "client_id": "...",  // ⚠️ OK but should be in Key Vault
    "client_secret": "..."  // ❌ SHOULD NOT BE HERE
  }
}
```

#### Target State
```json
{
  "labor_processing": {
    "download_dir": "/tmp/labor/download",  // ✅ Container path
    "sharepoint_output": "BI Import/labor_time_by_day_part.csv"  // ✅ Cloud path
  },
  "azure": {
    "key_vault_name": "kv-bw-data-integration",  // ✅ No secrets in config
    "tenant_id_secret": "azure-tenant-id"
  }
}
```

#### Migration Steps
1. [ ] Create new config structure in NewIntegration
2. [ ] Remove all Windows-specific paths
3. [ ] Remove all secrets (use Key Vault references)
4. [ ] Add environment-specific configs (dev/staging/prod)

---

### 7. Email Notifications

#### Current State
✅ Already using Microsoft Graph API!
- Authentication via MSAL
- Send email via Graph API
- Falls back to local file on failure

#### Target State
- Remove local file fallback
- Log failures instead
- Use structured logging

#### Migration Steps
1. [ ] Remove Windows path fallback
2. [ ] Implement retry logic with exponential backoff
3. [ ] Add structured logging

---

## Step-by-Step Migration Execution

### Prerequisites Checklist

Before starting migration, ensure:

- [x] Azure Key Vault `kv-bw-data-integration` - 100% populated (10 secrets) ✅
- [x] App Registration `ar-bw-data-integration` - Configured with API permissions ✅
- [x] NewIntegration directory structure - Created ✅
- [x] Dataverse table `crf63_oarsbidata` - **CREATED AND TESTED** (630 records synced successfully)
- [ ] Test SharePoint access - **TODO**
- [ ] Test OLAP server access - **TODO**

---

### Step 1: Create Dataverse Table

**Status**: ⚠️ NOT STARTED (REQUIRED FIRST)

**Purpose**: Create `crf63_oarsbidata` table to store 33 measures from OLAP cube

**Table Schema:**
- Schema Name: `crf63_oarsbidata`
- Display Name: OARS BI Data
- Key Fields: Store Number + Calendar Date + Data Source
- Business Key: `crf63_businesskey` (Format: `{StoreNumber}_{YYYYMMDD}`, Example: `4280_20250115`)
- Total Columns: 36 (2 keys + 1 business key + 33 measures + metadata)

**Business Key:**
The `crf63_businesskey` column provides efficient upsert operations for incremental updates:
- Combines Store Number and Calendar Date into a single indexed field
- Format: `{StoreNumber}_{YYYYMMDD}` 
- Example: Store 4280 on Jan 15, 2025 = `4280_20250115`
- Used for daily incremental sync (last 2 weeks lookback to handle late data updates)

**Script**: `create_oars_bi_table_pac.sh` (in root directory)

#### 1.1 Install Power Platform CLI

```bash
# macOS
brew tap microsoft/powerplatform-cli
brew install pac

# Verify installation
pac --version
```

#### 1.2 Authenticate to Dataverse

```bash
pac auth create --environment https://orgbf93e3c3.crm.dynamics.com
```

#### 1.3 Run Table Creation Script

```bash
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration
chmod +x create_oars_bi_table_pac.sh
./create_oars_bi_table_pac.sh
```

#### 1.4 Verify Table Creation

- Open Power Apps: <https://make.powerapps.com>
- Navigate to Tables
- Search for "OARS BI Data" or `crf63_oarsbidata`
- Verify 36 columns exist (including business key)

**Expected Output:**

```text
✅ Table and columns created successfully!
Table Details:
  Schema Name: crf63_oarsbidata
  Total Columns: 36 (2 keys + 1 business key + 33 measures)
```

**Checklist:**
- [ ] Power Platform CLI installed
- [ ] Authenticated to Dataverse
- [ ] Script executed successfully
- [ ] Table visible in Power Apps portal
- [ ] All 35 columns created

---

### Step 2: OLAP to Dataverse Migration

**Status**: ⏳ PENDING (Blocked by Step 1)

**Purpose**: Replace Excel COM automation with direct OLAP-to-Dataverse sync

**Current State:**
- Module: `modules/olap_to_dataverse.py` (526 lines) ✅ Created
- Script: `run_olap_to_dataverse.py` (77 lines) ✅ Created

**Replaces:**
- `modules/powerbi_update.py` (Windows-only Excel COM)
- 5 Excel files with OLAP connections

#### 2.1 Copy Modules to NewIntegration

```bash
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration

# Create directory structure
mkdir -p NewIntegration/modules/utils

# Copy OLAP module
cp modules/olap_to_dataverse.py NewIntegration/modules/

# Copy utilities
cp modules/utils/keyvault.py NewIntegration/modules/utils/
cp modules/utils/config.py NewIntegration/modules/utils/
cp modules/utils/custom_logging.py NewIntegration/modules/utils/

# Create __init__.py files
touch NewIntegration/modules/__init__.py
touch NewIntegration/modules/utils/__init__.py
```

#### 2.2 Update Key Vault References

Edit `NewIntegration/modules/olap_to_dataverse.py`:

```python
# Update Key Vault name
KEY_VAULT_NAME = "kv-bw-data-integration"  # was "sf-kv-6338"

# Update secret names
olap_username = get_secret('olap-username')
olap_password = get_secret('olap-password')
client_id = get_secret('app-client-id')
client_secret = get_secret('app-client-secret')
tenant_id = get_secret('azure-tenant-id')
```

Edit `NewIntegration/modules/utils/keyvault.py`:

```python
KEY_VAULT_NAME = "kv-bw-data-integration"
```

#### 2.3 Test OLAP Connection

```bash
cd NewIntegration

# Test Key Vault access
python modules/utils/keyvault.py

# Should show all 10 secrets
```

#### 2.4 Test Dataverse Connection

```bash
python -c "
from modules.olap_to_dataverse import get_dataverse_access_token
from modules.utils.keyvault import get_secret

token = get_dataverse_access_token(
    'https://orgbf93e3c3.crm.dynamics.com',
    get_secret('app-client-id'),
    get_secret('app-client-secret'),
    get_secret('azure-tenant-id')
)
print('✓ Token obtained' if token else '✗ Failed')
"
```

#### 2.5 Run Single Query Test

```bash
python run_olap_to_dataverse.py --test --fiscal-year 2025
```

#### 2.6 Full OLAP Sync

```bash
python run_olap_to_dataverse.py --fiscal-year 2025
```

Verify in Power Apps portal - should see ~16,000+ records.

**Checklist:**
- [ ] Modules copied to NewIntegration
- [ ] Key Vault references updated
- [ ] OLAP connection test successful
- [ ] Dataverse authentication successful
- [ ] Single query test returns data
- [ ] Full sync completes without errors
- [ ] Data visible in Dataverse portal
- [ ] All 33 measures populated

#### 2.7 OLAP Table Design Decision

**Single Table for All OLAP Data**

We use one table (`crf63_oarsbidata`) with a `datasource` column to distinguish between different cubes/sources. This approach:
- ✅ Simplifies relationship management to Calendar and Store tables
- ✅ Easier to query across all BI data (single API endpoint)
- ✅ Reduces table sprawl and maintenance overhead
- ✅ All files share the same 33-measure structure from OARS cubes

**Alternative considered**: Separate tables per Excel file (rejected due to complexity and redundancy)

**Detailed Measures Breakdown (33 total):**
- **Sales Metrics (4)**: TY Net Sales, L2Y/L3Y/LY Comp Sales
- **Cost Metrics (6)**: Target Food Cost, Actual Food Cost, FLMD, Actual Labor, Mileage Cost, Discounts
- **Operations Metrics (6)**: Total Hours, Store Days, Make Time, Rack Time, OTD Time, Avg TTDT
- **Order Metrics (6)**: TY Orders, LY Orders, Deliveries, BOZOCORO Orders, OTD Order Count, Dispatched Orders
- **Financial Metrics (5)**: Target Profit, Actual FLM, FLMDPC, Commission, Cash Over/Short
- **Customer Satisfaction (6)**: OSAT Survey Count, OSAT Satisfied, Accuracy Survey Count, Order Accuracy %, Total Calls, Answered Calls

**Metadata:**
- `crf63_lastrefreshed` (DateTime) - Timestamp of last sync

#### 2.8 OLAP Sync Process Flow

The sync executes in 5 steps:
1. **Get Dataverse access token** - Authenticate using app-client-id/secret
2. **Query OLAP cube via XMLA** - Execute MDX query over HTTPS
3. **Parse XMLA response** - Extract Axes (dimensions) and CellData (measures)
4. **Transform to Dataverse format** - Map OLAP fields to Dataverse columns
5. **Upsert records** - Create new or update existing records (keyed by Store + Date + DataSource)

#### 2.9 Configuration Example

**Config/config.json:**
```json
{
  "dataverse": {
    "environment_url": "https://orgbf93e3c3.crm.dynamics.com",
    "olap_bi_table_name": "crf63_oarsbidata"
  },
  "olap": {
    "server": "https://ednacubes.papajohns.com:10502",
    "ssl_verify": false,
    "catalogs": {
      "franchise": "OARS Franchise",
      "offers": "OARS Offers"
    },
    "fiscal_year": 2025
  }
}
```

#### 2.10 Monitoring and Troubleshooting

**Logs**: Written to `logs/app.log` (configurable)

Key log entries:
- Access token acquisition
- MDX query execution
- Data parsing results (row count)
- Upsert operations (created/updated/errors)

**Common Issues:**

| Issue | Solution |
|-------|----------|
| SSL Certificate Verification Failed | Set `olap.ssl_verify = false` in config (dev only) or install proper SSL cert |
| MDX Query Returns No Data | Check fiscal year in config, verify cube with `discover_cubes.py` |
| Dataverse Authentication Failed | Verify client ID/secret, check app permissions in Azure AD |
| Column Mapping Errors | Review `transform_olap_data_to_dataverse()` function column mappings |

#### 2.11 TODO: BI Offers MDX Extraction

The BI Offers.xlsx file uses OARS Offers cube. To complete migration:

1. Open BI Offers.xlsx
2. Press Alt+F11 (VBA Editor)
3. Run in Immediate Window:
   ```vba
   Debug.Print ActiveSheet.PivotTables(1).MDX
   ```
4. Copy the MDX query
5. Update `get_oars_offers_mdx_query()` in `modules/olap_to_dataverse.py`
6. Add column mappings for Offers-specific measures

#### 2.12 Performance Considerations

- **Batch Size**: Currently processes all records. For very large datasets (>50K records), consider batching in chunks of 1000-5000 records
- **Parallel Processing**: Future enhancement - query multiple stores simultaneously using threading
- **Incremental Sync**: Currently syncs all data for fiscal year. Future: add incremental sync based on `lastrefreshed` timestamp
- **Expected Performance**: ~16,000 records should complete in < 5 minutes

#### 2.13 Future Enhancements

1. **Incremental Sync**: Only sync changed data since last refresh (check `lastrefreshed` field)
2. **Parallel OLAP Queries**: Query multiple stores simultaneously using thread pool
3. **Error Recovery**: Implement retry logic with exponential backoff for transient failures
4. **Performance Metrics**: Track sync duration, data volume, API call latencies
5. **Data Validation**: Add data quality checks before upserting (null checks, range validation)
6. **Historical Data Backfill**: Script to backfill historical years from OLAP (2020-2024)

#### 2.14 Questions & Answers

**Q: Should different Excel files go to different Dataverse tables?**  
A: No. Using a single table with a `datasource` column is more efficient and maintainable. All files share the same measure structure from OARS cubes.

**Q: Can Dataverse store OLAP data directly?**  
A: Dataverse doesn't have native OLAP storage. This solution queries OLAP via XMLA and stores results as flat records, which is the recommended approach.

**Q: What happens to existing Excel files?**  
A: Excel files can remain for backward compatibility, but sync no longer depends on them. Excel COM automation in `powerbi_update.py` can be deprecated once OLAP sync is validated.

**Q: How do I revert to the old approach?**  
A: Comment out `olap_to_dataverse_main()` call in `main.py` and re-enable PowerBI update module calls.

---

### Step 3: Labor Processing Migration

**Status**: ⏳ PENDING (Ready to execute)

**Purpose**: Migrate labor file download/processing to cloud-native approach with Dataverse output and TrueNAS storage

**Current State:**
- Downloads ZIP files from FTP (files.papajohns.com)
- Extracts CSV timecard records
- Processes labor hours by day part
- **Outputs to CSV**: `labor_time_by_day_part.csv`

**Target State:**
- Downloads ZIP files from FTP
- Extracts CSV timecard records  
- **Stores raw files in TrueNAS volume**
- Processes labor hours by day part
- **Outputs directly to Dataverse** table `crf63_labortimes`

#### 3.1 Create Labor Times Dataverse Table

**Script**: `create_labor_times_table_pac.sh` (in root directory)

**Table Schema**: `crf63_labortimes`
- **Business Key**: `{StoreNumber}_{EmployeeID}_{Date}_{DayPart}` (unique)
- **9 columns total**: Keys, labor data, metadata

```bash
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration
chmod +x create_labor_times_table_pac.sh
./create_labor_times_table_pac.sh
```

#### 3.2 Copy Labor Processing Module

```bash
cd /Users/howardshen/Library/CloudStorage/OneDrive-Personal/Github/Beachwood-Data-Integration

# Copy to NewIntegration
cp modules/labor_processing.py NewIntegration/modules/
```

#### 3.3 Update Credentials to Key Vault

Replace hardcoded credentials with Key Vault lookups:

```python
from modules.utils.keyvault import get_secret

# Replace:
username = config["labor_processing"]["username"]
password = config["labor_processing"]["password"]

# With:
username = get_secret('files-username')
password = get_secret('files-password')
```

#### 3.4 Add TrueNAS Storage Configuration

**TrueNAS Volume Setup:**
- Create volume: `/mnt/truenas/labor`
- Mount in container: `/truenas/labor`
- Structure: `/truenas/labor/YYYY/MM/DD/filename.zip`

**Config Updates:**
```json
{
  "truenas": {
    "enabled": true,
    "labor_volume_path": "/truenas/labor",
    "retention_days": 365
  }
}
```

**Storage Logic:**
1. Download ZIP files to temp directory
2. Process/extract files
3. **Copy raw ZIP files to TrueNAS**: `/truenas/labor/YYYY/MM/filename.zip`
4. Clean up temp directories
5. Maintain rolling retention (delete files older than 365 days)

#### 3.5 Modify Output to Dataverse

Replace CSV writing with Dataverse upsert:

```python
# Remove CSV writing logic
# with open(output_file, 'w', newline='', encoding='utf-8') as f:
#     writer = csv.DictWriter(f, fieldnames=fieldnames)
#     writer.writeheader()
#     for record in all_records:
#         writer.writerow(record)

# Add Dataverse upsert logic
from modules.olap_to_dataverse import upsert_to_dataverse

dataverse_records = []
for record in all_records:
    dataverse_record = {
        "crf63_businesskey": f"{record['store_number']}_{record['employee_id']}_{record['date']}_{record['day_part']}",
        "crf63_storenumber": record['store_number'],
        "crf63_employeeid": record['employee_id'],
        "crf63_employeename": record['employee_name'],
        "crf63_daypart": record['day_part'],
        "crf63_totalhours": record['total_hours'],
        "crf63_workdate": record['date'],
        "crf63_lastupdated": datetime.now().isoformat(),
        "crf63_datasource": "labor_processing"
    }
    dataverse_records.append(dataverse_record)

# Upsert to Dataverse
created, updated, errors = upsert_to_dataverse(
    config['dataverse']['environment_url'],
    access_token,
    "crf63_labortimes",
    dataverse_records,
    logger
)
```

#### 3.6 Update File Paths for Container

Replace Windows paths with container paths:

```python
# Replace Windows paths
DOWNLOAD_DIR = "/tmp/labor/download"
EXTRACT_DIR = "/tmp/labor/extract"
# Remove output_file reference (no longer needed)
```

#### 3.7 Test Labor Processing

```bash
cd NewIntegration

# Test with sample data
python -c "
from modules.labor_processing import main
import logging
result = main({}, logging.getLogger())
print('Result:', result)
"
```

**Checklist:**
- [ ] Dataverse table `crf63_labortimes` created
- [ ] Module copied to NewIntegration
- [ ] Key Vault credentials integrated
- [ ] TrueNAS storage configured
- [ ] Output changed from CSV to Dataverse
- [ ] File paths updated for container
- [ ] End-to-end testing completed

---

### Step 4: Forecast Modules Migration

**Status**: ⏳ PENDING (After Step 2)

**Modules:**
- Labor Forecast: `modules/labor_forecast.py` (already has Dataverse support!)
- Sales Forecast: `modules/sales_forecast.py`

#### 4.1 Labor Forecast (Minimal Work)

```bash
cp modules/labor_forecast.py NewIntegration/modules/
```

Module already has `get_historical_labor_data_from_dataverse()` function!

Just update Key Vault references and test.

#### 4.2 Sales Forecast

```bash
cp modules/sales_forecast.py NewIntegration/modules/
```

Update to use SharePoint for history files instead of local Excel.

#### 4.3 Test Forecasts

```bash
# Test labor forecast
python modules/labor_forecast.py --store 000126 --weeks 5

# Test sales forecast
python modules/sales_forecast.py --store 000126 --weeks 5
```

**Checklist:**
- [ ] Labor forecast module copied and tested
- [ ] Sales forecast module copied and updated
- [ ] Both write to Dataverse
- [ ] Forecast accuracy verified

---

### Step 5: Integration Testing

**Status**: ⏳ PENDING (After Steps 2-4)

#### 5.1 Create Main Orchestrator

Create `NewIntegration/main.py`:

```python
"""Main orchestrator for Beachwood Data Integration"""
import logging
from modules.olap_to_dataverse import olap_to_dataverse_main
from modules.labor_processing import labor_processing_main
from modules.labor_forecast import labor_forecast_main
from modules.sales_forecast import sales_forecast_main

def main():
    logger = setup_logging()
    config = load_config()
    results = {}
    
    # 1. OLAP to Dataverse
    logger.info("Starting OLAP sync...")
    results['olap'] = olap_to_dataverse_main(config, logger)
    
    # 2. Labor Processing
    logger.info("Starting labor processing...")
    results['labor'] = labor_processing_main(config, logger)
    
    # 3. Labor Forecast
    logger.info("Starting labor forecast...")
    results['labor_forecast'] = labor_forecast_main(config, logger)
    
    # 4. Sales Forecast
    logger.info("Starting sales forecast...")
    results['sales_forecast'] = sales_forecast_main(config, logger)
    
    send_summary_email(results, logger)
    return results

if __name__ == "__main__":
    main()
```

#### 5.2 End-to-End Test

```bash
cd NewIntegration
python main.py
```

#### 5.3 Data Validation

Compare outputs between Windows system and container.

**Checklist:**
- [ ] Main orchestrator created
- [ ] All modules execute in sequence
- [ ] No errors in logs
- [ ] Data matches Windows system
- [ ] Email notifications sent
- [ ] Performance < 40 minutes

---

### Step 6: Containerization

**Status**: ⏳ PENDING (After Step 5)

#### 6.1 Build Docker Image

```bash
cd NewIntegration

docker build -t beachwood-integration:test .
```

#### 6.2 Test Locally

```bash
docker run --rm \
  -e AZURE_TENANT_ID="c8b6ba98-3fc0-4153-83a9-01374492c0f5" \
  -e AZURE_CLIENT_ID="d056223e-f0de-4b16-b4e0-fec2a24109ff" \
  -e AZURE_CLIENT_SECRET="<secret>" \
  beachwood-integration:test
```

#### 6.3 Deploy to TrueNAS

- Copy docker-compose.yml
- Configure environment variables
- Set up cron schedule
- Test first run

**Checklist:**
- [ ] Docker image builds successfully
- [ ] Container runs without errors
- [ ] All cloud services accessible
- [ ] Key Vault authentication works
- [ ] Data processing completes
- [ ] Logs captured properly

---

### Migration Progress Tracker

| Step | Module | Status | Completion | Blocker |
|------|--------|--------|------------|---------|
| 1 | Dataverse Table | ✅ Complete | 100% | None |
| 2 | OLAP to Dataverse | ✅ Complete | 100% | None - 630 records synced successfully |
| 3 | Labor Processing | ⏳ Pending | 0% | Step 2 complete, ready to proceed |
| 4 | Forecast Modules | ⏳ Pending | 0% | Step 2 complete, ready to proceed |
| 5 | Integration Testing | ⏳ Pending | 0% | Steps 2-4 |
| 6 | Containerization | ⏳ Pending | 0% | Step 5 |

**Next Immediate Action**: Execute Step 3 (Labor Processing Migration)

---

## Testing Strategy

### Unit Testing

Create unit tests for each migrated module:

```
NewIntegration/tests/
├── test_olap_to_dataverse.py
├── test_labor_processing.py
├── test_doordash_update.py
├── test_labor_forecast.py
├── test_sales_forecast.py
├── test_sharepoint_helper.py
└── test_keyvault_integration.py
```

#### Test Coverage Goals
- Core business logic: 80%+
- Data transformation: 90%+
- API integrations: 70%+

### Integration Testing

Test scenarios:
1. **OLAP to Dataverse Flow**
   - Query OLAP cube
   - Parse results
   - Upsert to Dataverse
   - Verify data accuracy

2. **Labor Processing Flow**
   - Download from FTP
   - Extract ZIP files
   - Process labor data
   - Upload to SharePoint
   - Verify calculations

3. **DoorDash Processing Flow**
   - Retrieve base file from SharePoint
   - Process new attachment
   - Update base file
   - Archive old version
   - Verify data integrity

4. **Forecast Flows**
   - Retrieve historical data
   - Run forecast models
   - Upsert to Dataverse
   - Verify predictions

### Performance Testing

Benchmarks to establish:
- OLAP query execution time: < 5 minutes
- File processing time: < 10 minutes per file
- Dataverse upsert rate: > 100 records/second
- Total execution time: < 30 minutes

### Security Testing

Checklist:
- [ ] No secrets in container image
- [ ] No secrets in logs
- [ ] All API calls use HTTPS
- [ ] Key Vault access uses managed identity
- [ ] Container runs as non-root user
- [ ] Minimal attack surface (slim base image)

---

## Deployment Procedures

### Building the Container Image

```bash
# From NewIntegration directory
cd NewIntegration

# Build the image
docker build -t beachwood-integration:latest .

# Tag for registry
docker tag beachwood-integration:latest registry.example.com/beachwood-integration:v1.0.0

# Push to registry
docker push registry.example.com/beachwood-integration:v1.0.0
```

### TrueNAS Configuration

#### Option 1: Docker Container
```bash
# Create container on TrueNAS
docker run -d \
  --name beachwood-integration \
  --restart unless-stopped \
  -e AZURE_CLIENT_ID=... \
  -e AZURE_CLIENT_SECRET=... \
  -e AZURE_TENANT_ID=... \
  registry.example.com/beachwood-integration:v1.0.0
```

#### Option 2: Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: beachwood-integration
  namespace: production
spec:
  schedule: "0 2 * * *"  # Run at 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: integration
            image: registry.example.com/beachwood-integration:v1.0.0
            env:
            - name: AZURE_TENANT_ID
              valueFrom:
                secretKeyRef:
                  name: azure-credentials
                  key: tenant-id
            - name: AZURE_CLIENT_ID
              valueFrom:
                secretKeyRef:
                  name: azure-credentials
                  key: client-id
            - name: AZURE_CLIENT_SECRET
              valueFrom:
                secretKeyRef:
                  name: azure-credentials
                  key: client-secret
          restartPolicy: OnFailure
```

### Environment Variables

Required environment variables for the container:

```bash
# Azure Authentication
AZURE_TENANT_ID=c8b6ba98-3fc0-4153-83a9-01374492c0f5
AZURE_CLIENT_ID=a8615369-05eb-436b-8e3e-1b3b9ecfaf2d
AZURE_CLIENT_SECRET=<from Key Vault or secret management>

# Configuration
ENVIRONMENT=production  # or 'staging', 'dev'
LOG_LEVEL=INFO
```

### Monitoring & Logging

#### Log Collection
- Container logs to stdout/stderr
- Structured JSON logging
- Log aggregation via TrueNAS or external service

#### Metrics to Monitor
- Execution success/failure
- Execution duration
- Records processed
- API call latencies
- Error rates

#### Alerting
- Email on failure
- Slack/Teams notifications
- PagerDuty for critical failures

---

## Rollback Plan

### Rollback Triggers
- Data integrity issues
- Performance degradation > 50%
- Critical bugs affecting business operations
- More than 3 consecutive failures

### Rollback Procedure

1. **Immediate Action** (< 5 minutes)
   - Disable TrueNAS container job
   - Re-enable Windows Task Scheduler
   - Notify stakeholders

2. **Verification** (5-15 minutes)
   - Confirm Windows system running
   - Verify next scheduled execution
   - Monitor first execution

3. **Investigation** (1-24 hours)
   - Analyze container logs
   - Identify root cause
   - Document issues
   - Plan remediation

4. **Remediation**
   - Fix issues in container
   - Re-test in staging
   - Plan second migration attempt

### Data Integrity Safeguards

During parallel execution:
- Keep both systems running for 7 days
- Compare outputs daily
- Dataverse records include source system tag
- Maintain Windows system as backup

---

## Post-Migration Validation

### Success Criteria

#### Functional Requirements
- [ ] All modules execute successfully
- [ ] Data accuracy matches Windows system (>99.9%)
- [ ] All emails sent successfully
- [ ] All Dataverse records updated
- [ ] All files uploaded to SharePoint

#### Performance Requirements
- [ ] Total execution time < 40 minutes
- [ ] Memory usage < 2 GB
- [ ] CPU usage reasonable for container
- [ ] No timeout errors

#### Security Requirements
- [ ] No secrets in logs
- [ ] All API calls authenticated
- [ ] Key Vault access successful
- [ ] No security scan vulnerabilities

#### Operational Requirements
- [ ] Logging comprehensive and parseable
- [ ] Monitoring alerts configured
- [ ] Documentation complete
- [ ] Runbook created

### Validation Checklist

**Week 1 Post-Cutover**
- [ ] Daily execution monitoring
- [ ] Daily data validation
- [ ] Daily stakeholder updates
- [ ] Issue tracking and resolution

**Week 2-4 Post-Cutover**
- [ ] Weekly data validation
- [ ] Weekly performance review
- [ ] Bi-weekly stakeholder updates
- [ ] Continuous improvement

**30 Days Post-Cutover**
- [ ] Final validation report
- [ ] Decommission Windows system
- [ ] Update all documentation
- [ ] Conduct lessons learned session

---

## Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Azure Key Vault access issues | High | Low | Test thoroughly, document fallback |
| OLAP connection failures | High | Medium | Implement retry logic, monitoring |
| SharePoint API rate limiting | Medium | Low | Implement backoff, batch operations |
| Data transformation bugs | High | Medium | Extensive testing, parallel execution |
| Container resource constraints | Medium | Low | Performance testing, resource allocation |
| TrueNAS configuration issues | Medium | Low | Document setup, test in staging |

---

## Appendix

### A. Directory Structure

```
NewIntegration/
├── Migrate-to-Truenas.md          # This document
├── README.md                       # Quick start guide
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Container definition
├── docker-compose.yml              # Local testing
├── .dockerignore
├── .env.example
│
├── config/
│   ├── config.dev.json
│   ├── config.staging.json
│   └── config.production.json
│
├── modules/
│   ├── __init__.py
│   ├── olap_to_dataverse.py       # NEW: Replaces powerbi_update.py
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── labor_processing.py    # Updated
│   │   ├── doordash_update.py     # Updated
│   │   ├── labor_forecast.py      # Updated
│   │   └── sales_forecast.py      # Updated
│   │
│   └── utils/
│       ├── __init__.py
│       ├── keyvault.py            # ✅ Already exists
│       ├── sharepoint.py          # NEW: SharePoint helper
│       ├── config.py              # Updated
│       └── logging.py             # Updated
│
├── tests/
│   ├── __init__.py
│   ├── test_olap_to_dataverse.py
│   ├── test_labor_processing.py
│   ├── test_doordash_update.py
│   ├── test_labor_forecast.py
│   ├── test_sales_forecast.py
│   ├── test_sharepoint.py
│   └── test_keyvault.py
│
├── docs/
│   ├── API_DOCUMENTATION.md
│   ├── DEPLOYMENT.md
│   ├── TROUBLESHOOTING.md
│   └── RUNBOOK.md
│
└── docker/
    ├── entrypoint.sh
    └── healthcheck.sh
```

### B. Key Vault Secrets Reference

| Secret Name | Purpose | Example |
|-------------|---------|---------|
| azure-tenant-id | Azure AD tenant | c8b6ba98-3fc0-4153-83a9-01374492c0f5 |
| dataverse-client-id | Dataverse app registration | a8615369-... |
| dataverse-client-secret | Dataverse app secret | *** |
| dataverse-environment-url | Dataverse instance | https://orgbf93e3c3.crm.dynamics.com |
| sharepoint-client-id | SharePoint app registration | a8615369-... |
| sharepoint-client-secret | SharePoint app secret | *** |
| sharepoint-site-url | SharePoint site | https://tenant.sharepoint.com/sites/... |
| olap-password | OLAP server password | *** |
| labor-processing-username | FTP username | *** |
| labor-processing-password | FTP password | *** |

### C. SharePoint File Locations

Current Windows paths mapped to SharePoint:

| Windows Path | SharePoint Path |
|--------------|----------------|
| C:\Users\...\BI Import\*.xlsx | IT Project - General/BI Import/*.xlsx |
| C:\Users\...\BI Import\*.csv | IT Project - General/BI Import/*.csv |
| C:\Users\...\BI Import\DD\*.csv | IT Project - General/BI Import/DD/*.csv |
| C:\Users\...\Labor Planning\Export | Labor Planning/Labor Planning Export/Export |

### D. Dataverse Tables

| Table Name | Purpose | Key Fields |
|------------|---------|------------|
| crf63_olapbidatas | OLAP cube data (NEW) | Store, Date, Metrics |
| crf63_laborheadcounts | Labor forecasts (existing) | Store, Date, Headcount |
| crf63_bwsalesforecasts | Sales forecasts (existing) | Store, Week, Forecast |

### E. Reference Documents

Existing documentation to reference:
- `KEYVAULT_MIGRATION.md` - Azure Key Vault setup ✅
- `OLAP_TO_DATAVERSE_GUIDE.md` - OLAP integration guide
- `TODO.md` - Current project status
- `EXTRACTED_MDX_QUERY.md` - MDX query documentation

---

## Document Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-11-19 | AI Assistant | Initial comprehensive migration plan |

---

## Approval & Sign-off

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Project Sponsor | | | |
| Technical Lead | | | |
| Operations Manager | | | |
| Security Officer | | | |

---

**Next Steps**: Review this plan with stakeholders and begin Week 1 implementation.
