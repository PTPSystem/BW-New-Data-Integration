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
