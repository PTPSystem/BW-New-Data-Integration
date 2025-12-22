# Adding New MDX Pipeline - Step-by-Step Guide

This guide explains how to add a new MDX query as a pipeline using the config-driven architecture.

## Overview

The system uses a generic XMLA parser with config-driven hierarchy mapping. Adding a new pipeline requires:
1. Adding pipeline configuration to `pipelines/pipelines.yaml`
2. Creating a mapping file in `pipelines/mappings/`
3. No Python code changes needed!

---

## Step 1: Analyze Your MDX Query

### Identify Components

Given a new MDX query, identify these components:

```mdx
SELECT 
  {[Measures].[Measure1], [Measures].[Measure2]}      ← MEASURES (Axis0/COLUMNS)
  DIMENSION PROPERTIES ... ON COLUMNS,
  
  NON EMPTY CrossJoin(
    {[Dimension1].[Hierarchy1].[Level].AllMembers},   ← DIMENSIONS (Axis1/ROWS)
    {[Dimension2].[Hierarchy2].[Level].AllMembers}    ← DIMENSIONS (Axis1/ROWS)
  )
  DIMENSION PROPERTIES ... ON ROWS
  
FROM [CubeName]                                       ← CATALOG
WHERE (${slicer})                                     ← SLICER (template variable)
```

### Extract Information

1. **Catalog Name**: From the `FROM [CubeName]` clause
2. **Measures**: All items in the `SELECT {[Measures].[...]}` section
3. **Dimensions**: Each hierarchy in the `CrossJoin()` on ROWS
4. **Hierarchy Patterns**: The full path like `[Franchise].[Store Number Label]`

---

## Step 2: Create Hierarchy Mappings

For each dimension hierarchy, create a regex pattern that will match it.

### Pattern Creation Rules

1. Use `.*` for wildcards within the hierarchy path
2. Match the meaningful part that identifies the dimension
3. Assign a simple field name (camelCase or PascalCase)

### Examples from Existing Pipelines

```yaml
# Store Number - matches various store hierarchies
- pattern: "Franchise.*Store"
  field: "StoreNumber"

# Calendar Date - matches various date hierarchies  
- pattern: "Calendar.*Date"
  field: "CalendarDate"

# Item Number - matches inventory item hierarchies
- pattern: ".*Item_Number"
  field: "ItemNumber"

# Item Description - matches description hierarchies
- pattern: ".*Item_Description"
  field: "ItemDescription"

# Source Actor - matches channel actor
- pattern: "Source.*Actor"
  field: "SourceActor"

# Source Channel - matches channel type
- pattern: "Source.*Channel"
  field: "SourceChannel"

# Day Part - matches time of day
- pattern: "Day Part.*Day Part"
  field: "DayPart"

# Offer Code - matches offer hierarchies
- pattern: "Offer Code.*Hierarchy"
  field: "OfferCode"

# Offer Description - matches offer description
- pattern: "Offer.*POS Description"
  field: "OfferDescription"
```

### Pattern Matching Logic

The generic parser uses Python's `re.search()`, so your pattern should:
- Be unique enough to match only one hierarchy
- Use `.*` to skip variable parts
- Focus on the distinctive keywords in the hierarchy name

---

## Step 3: Add to pipelines.yaml

### Template

```yaml
pipelines:
  your_pipeline_name:
    catalog: YourCatalogName              # From FROM clause
    parser: celldata                      # Always use 'celldata' for now
    mapping: mappings/your_pipeline.yaml  # Mapping file path
    hierarchy_mappings:                   # Dimension → Field mappings
      - pattern: "Pattern1"
        field: "FieldName1"
      - pattern: "Pattern2"
        field: "FieldName2"
      # ... one for each dimension
    mdx: |-
      SELECT 
      {
        [Measures].[Measure1],
        [Measures].[Measure2]
        # ... all your measures
      }
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
      NON EMPTY CrossJoin(
        Hierarchize({[Dim1].[Hierarchy1].[Level].AllMembers}),
        Hierarchize({[Dim2].[Hierarchy2].[Level].AllMembers})
      )
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
      FROM [YourCube]
      WHERE (${slicer})
      CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
```

### Important Notes

1. **Always use `${slicer}`** as the WHERE clause placeholder
2. **Measures must be on COLUMNS** (Axis0)
3. **Dimensions must be on ROWS** (Axis1)
4. **Order matters**: List hierarchy_mappings in the same order as they appear in your MDX ROWS
5. **parser**: Always set to `celldata` (generic parser auto-activates when hierarchy_mappings present)

---

## Step 4: Create Mapping File

Location: `pipelines/mappings/your_pipeline.yaml`

### Template

```yaml
table: crf63_yourtablename

alternate_key: crf63_businesskey

business_key:
  format: "{Field1}_{Field2}_{Field3}"  # Combine dimensions to make unique key
  # Examples:
  # "{StoreNumber}_{CalendarDate:%Y%m%d}"
  # "{ItemNumber}_{CalendarDate:%Y%m%d}_{StoreNumber}"
  # "{StoreNumber}_{CalendarDate:%Y%m%d}_{OfferCode}"

fields:
  # Map each DIMENSION field to Dataverse column
  FieldName1:
    dataverse: crf63_fieldname1
    type: string  # or: date, int, decimal
  
  FieldName2:
    dataverse: crf63_fieldname2
    type: date
  
  # ... one for each dimension

measures:
  # Map each MEASURE to Dataverse column
  Measure Name From MDX:
    dataverse: crf63_measurename
    type: decimal  # or: int, string
    default: 0     # Optional: default value if null
  
  Another Measure:
    dataverse: crf63_anothermeasure
    type: decimal
  
  # ... one for each measure
```

### Field Types

- `string`: Text values (store numbers, descriptions, etc.)
- `date`: Calendar dates (auto-formatted as YYYY-MM-DD for Dataverse)
- `int`: Whole numbers (order counts, etc.)
- `decimal`: Numbers with decimals (sales, costs, percentages)

### Business Key Rules

**CRITICAL**: Never include comment/description fields in business keys due to chance html encoding issues

Use only:
- Business identifiers (store numbers, item numbers)
- Dates
- Stable categorical values (offer codes, channels)

**Bad examples** (DO NOT USE):
```yaml
business_key:
  format: "{ItemDescription}_{StoreNumber}"  # ❌ Description can change
  format: "{Comments}_{Date}"                 # ❌ Comments are user-editable
```

**Good examples**:
```yaml
business_key:
  format: "{StoreNumber}_{CalendarDate:%Y%m%d}"                    # ✓
  format: "{ItemNumber}_{CalendarDate:%Y%m%d}_{StoreNumber}"       # ✓
  format: "{StoreNumber}_{CalendarDate:%Y%m%d}_{SourceChannel}"    # ✓
```

---

## Step 5: Real-World Example

### Given This MDX

```mdx
SELECT {
  [Measures].[Revenue USD],
  [Measures].[Order Count],
  [Measures].[Labor Hours]
} 
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
NON EMPTY CrossJoin(
  Hierarchize({[Stores].[Store ID].[Store ID].AllMembers}),
  Hierarchize({[Time].[Date].[Date].AllMembers})
)
DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
FROM [Sales Data]
WHERE (${slicer})
```

### Step 5a: Identify Components

- **Catalog**: `Sales Data`
- **Measures**: Revenue USD, Order Count, Labor Hours
- **Dimensions**: 
  - `[Stores].[Store ID]` → StoreID
  - `[Time].[Date]` → Date

### Step 5b: Create Pipeline Entry

```yaml
pipelines:
  store_daily_metrics:
    catalog: Sales Data
    parser: celldata
    mapping: mappings/store_daily_metrics.yaml
    hierarchy_mappings:
      - pattern: "Stores.*Store ID"
        field: "StoreID"
      - pattern: "Time.*Date"
        field: "Date"
    mdx: |-
      SELECT {
        [Measures].[Revenue USD],
        [Measures].[Order Count],
        [Measures].[Labor Hours]
      } 
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
      NON EMPTY CrossJoin(
        Hierarchize({[Stores].[Store ID].[Store ID].AllMembers}),
        Hierarchize({[Time].[Date].[Date].AllMembers})
      )
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
      FROM [Sales Data]
      WHERE (${slicer})
      CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
```

### Step 5c: Create Mapping File

`pipelines/mappings/store_daily_metrics.yaml`:

```yaml
table: crf63_storedailymetrics

alternate_key: crf63_businesskey

business_key:
  format: "{StoreID}_{Date:%Y%m%d}"

fields:
  StoreID:
    dataverse: crf63_storeid
    type: string
  
  Date:
    dataverse: crf63_date
    type: date

measures:
  Revenue USD:
    dataverse: crf63_revenueusd
    type: decimal
    default: 0
  
  Order Count:
    dataverse: crf63_ordercount
    type: int
    default: 0
  
  Labor Hours:
    dataverse: crf63_laborhours
    type: decimal
    default: 0
```

---

## Step 6: Test Your Pipeline

### 6a: Test with 1 Week Data

```bash
python olap_to_dataverse.py --pipeline your_pipeline_name --length 1wk --print-mdx
```

This will:
- Show the rendered MDX query
- Execute the query against OLAP
- Parse the response using the generic parser
- Display how many records were processed

### 6b: Check for Common Issues

**Error: "No mapping for hierarchy"**
- Your hierarchy pattern didn't match
- Check the actual hierarchy name in the XMLA response
- Adjust your pattern to match it

**Error: "No measures found on Axis0"**
- Measures not on COLUMNS
- Move measures from ROWS to COLUMNS in your MDX

**Error: "No row tuples found on Axis1"**
- Dimensions not on ROWS
- Move dimensions from COLUMNS to ROWS in your MDX
- Check that your WHERE clause is returning data

**Record count is 0**
- Your slicer may be filtering out all data
- Try with `--length 1wk` to get recent data
- Verify the MyView ID exists in your cube

### 6c: Verify Output

Check that:
1. Number of records matches expectations
2. All measures appear in output
3. All dimensions are correctly parsed
4. Business keys are being generated correctly

---

## Step 7: Production Deployment

Once tested:

### Option A: Use --pipeline flag

```bash
# Run your new pipeline
python olap_to_dataverse.py --pipeline your_pipeline_name --length 1wk
```

### Option B: Use pipeline name directly with --query

**New in v2.0**: Pipeline names are now automatically available as `--query` options!

```bash
# Use the pipeline name directly (recommended)
python olap_to_dataverse.py --query your_pipeline_name --length 1wk
```

The `--query` parameter now accepts any pipeline name from `pipelines.yaml`. No Python code changes needed!

**Example**: If your pipeline is named `store_daily_metrics` in `pipelines.yaml`:
```bash
python olap_to_dataverse.py --query store_daily_metrics --length 1wk
```

**Note**: For backward compatibility, legacy query names still work:
- `--query daily` → maps to `daily_sales` pipeline
- `--query sales_channel` → maps to `sales_channel` pipeline
- `--query offers` → maps to `offers` pipeline
- `--query inventory` → maps to `inventory` pipeline

---

## Troubleshooting Guide

### Issue: Parser returns wrong field names

**Symptom**: Fields named like `[Franchise].[Store Number]` instead of `StoreNumber`

**Solution**: Your hierarchy pattern didn't match. Check the pattern:
1. Run with `--print-mdx` to see the query
2. Look for "WARNING: No mapping for hierarchy" in output
3. Adjust pattern to match the actual hierarchy name

### Issue: Some measures are missing

**Symptom**: Not all measures appear in the Dataverse records

**Solution**: 
1. Check that measure names in `measures:` section match EXACTLY what's in the MDX
2. Measure names are case-sensitive
3. Include spaces and special characters exactly as they appear

### Issue: Business key violations

**Symptom**: "Duplicate key" or "Key already exists" errors

**Solution**:
1. Verify your business_key format creates unique values
2. Check that you're including ALL dimension fields needed for uniqueness
3. Ensure date format is consistent (use `%Y%m%d`)

### Issue: Date fields show time component

**Symptom**: Dates appear as "2025-12-21 00:00:00" instead of "2025-12-21"

**Solution**: Set `type: date` in the mapping file (not `type: string`)

---

## Quick Reference

### Minimal Pipeline (2 dimensions, 1 measure)

**pipelines.yaml**:
```yaml
pipelines:
  simple_example:
    catalog: MyCube
    parser: celldata
    mapping: mappings/simple_example.yaml
    hierarchy_mappings:
      - pattern: "Stores.*Store"
        field: "Store"
      - pattern: "Calendar.*Date"
        field: "Date"
    mdx: |-
      SELECT {[Measures].[Sales]} 
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS,
      NON EMPTY CrossJoin(
        Hierarchize({[Stores].[Store].[Store].AllMembers}),
        Hierarchize({[Calendar].[Date].[Date].AllMembers})
      )
      DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
      FROM [MyCube]
      WHERE (${slicer})
      CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
```

**mappings/simple_example.yaml**:
```yaml
table: crf63_simpleexample
alternate_key: crf63_businesskey
business_key:
  format: "{Store}_{Date:%Y%m%d}"
fields:
  Store:
    dataverse: crf63_store
    type: string
  Date:
    dataverse: crf63_date
    type: date
measures:
  Sales:
    dataverse: crf63_sales
    type: decimal
    default: 0
```

---

## Summary Checklist

When adding a new MDX pipeline:

- [ ] Identify catalog, measures, and dimensions from MDX
- [ ] Create hierarchy_mappings (one per dimension)
- [ ] Add pipeline entry to `pipelines/pipelines.yaml`
- [ ] Create mapping file in `pipelines/mappings/`
- [ ] Define business_key format (using stable identifiers only)
- [ ] Map all dimensions to Dataverse fields
- [ ] Map all measures to Dataverse fields
- [ ] Test with `--length 1wk --print-mdx`
- [ ] Verify record count and field values
- [ ] Deploy to production

**No Python code changes needed - it's all configuration!** 🎉

---

## Additional Resources

- **Existing Examples**: Look at `pipelines/pipelines.yaml` for all 4 working examples
- **Generic Parser Code**: See `modules/generic_xmla_parser.py` for implementation details
- **Mapping Examples**: Check `pipelines/mappings/` folder for all mapping file formats
- **Test Run**: Always test with 1 week of data first before full deployment

---

## Need Help?

If you encounter issues:

1. Check existing pipelines in `pipelines/pipelines.yaml` for similar examples
2. Run with `--print-mdx` to see the actual query being executed
3. Look for "WARNING" messages in the output about unmapped hierarchies
4. Verify your MDX query returns data when run directly in Excel/SSMS
5. Check that MyView IDs referenced in slicer exist in your cube

The generic parser automatically handles all XMLA response parsing - you only need to provide the configuration! 🚀
