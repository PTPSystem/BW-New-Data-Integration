# Python Environment Instructions

When generating terminal commands to run Python scripts, ALWAYS use the virtual environment located at `.venv`.

**Do not** use `python` or `python3` directly.

**Preferred formats:**
1. Activation: `source .venv/bin/activate && python <script_name>.py`
2. Direct execution: `.venv/bin/python <script_name>.py`

**Example:**
```bash
# Correct
source .venv/bin/activate && python olap_to_dataverse.py
# OR
.venv/bin/python olap_to_dataverse.py

# Incorrect
python olap_to_dataverse.py
```
# Dataverse Alternate Key Instructions

When creating or configuring Dataverse alternate keys:

**NEVER use comment fields or description fields as part of alternate keys.**

Alternate keys should only contain:
- Business identifiers (e.g., `crf63_businesskey`)
- Technical identifiers (e.g., item numbers, store numbers, dates)
- Fields with stable, unique values

**Rationale:**
- Comment and description fields are user-editable and can change frequently
- They may contain special characters, line breaks, or formatting
- They are not suitable for uniquely identifying records
- Using them causes key violations and data integrity issues

**Example:**
```yaml
# Correct - using business key field
alternate_key: crf63_businesskey
business_key:
  format: "{StoreNumber}_{CalendarDate:%Y%m%d}_{ItemNumber}"

# Incorrect - using description field
alternate_key: crf63_itemdescription  # DON'T DO THIS
```

# Adding New MDX Pipelines

When asked to add a new MDX query or create a new pipeline:

**ALWAYS refer to the comprehensive guide:** `docs/ADD_NEW_PIPELINE.md`

This guide contains:
- Step-by-step instructions for adding new pipelines
- How to create hierarchy mappings from MDX queries
- Pipeline configuration templates
- Mapping file creation guide
- Real-world examples and troubleshooting

**Quick reference:**
1. Analyze MDX query to identify catalog, measures, and dimensions
2. Create hierarchy_mappings with regex patterns (see examples in guide)
3. Add pipeline entry to `pipelines/pipelines.yaml`
4. Create mapping file in `pipelines/mappings/`
5. Test with `--pipeline your_name --length 1wk --print-mdx`

**No Python code changes needed - it's 100% configuration-driven!**

See `docs/ADD_NEW_PIPELINE.md` for complete instructions and examples.