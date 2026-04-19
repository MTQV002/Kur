---
name: diagnose-error
description: Workflow for diagnosing and fixing SQL errors
---

# Diagnose Error — Workflow

## Error Categories

### 1. Syntax Errors
- Missing quotes, parentheses, commas
- Wrong keyword order
- **Fix:** Re-read schema, regenerate with correct syntax

### 2. Column/Table Not Found
- Wrong table name or alias
- Column doesn't exist in schema
- **Fix:** Check schema_context, use correct names

### 3. Type Mismatch
- Comparing string to integer
- Date format issues
- **Fix:** Add explicit CAST or ::type

### 4. Aggregation Errors
- Column in SELECT not in GROUP BY
- Using WHERE instead of HAVING for aggregates
- **Fix:** Add missing GROUP BY columns

### 5. Permission Errors
- Attempting DML (INSERT/UPDATE/DELETE)
- Accessing restricted tables
- **Fix:** Rewrite as SELECT only

## Self-Correction Workflow

```
1. Read error message carefully
2. Identify error category (1-5 above)
3. Check schema_context for correct names/types
4. Regenerate SQL with the fix
5. If still failing after 3 retries → ask user to clarify
```
