# Welcome to {{project_name}}

This is a documentation template with variable placeholders.

## Project Information

- **Project**: {{project_name}}
- **Version**: {{version}}
- **Author**: {{author}}
- **Environment**: {{environment}}

## User Settings

Current user: {{user_name}}
User role: {{user_role}}
Access level: {{access_level}}

## Configuration

Maximum iterations: {{max_iterations}}
Timeout: {{timeout}} seconds
Debug mode: {{debug_mode}}

<when_tool list_files>
## Available Tools

You have access to file listing tools with the following limits:
- Max files: {{list_files_max}}
- Supports pattern matching: {{pattern_support}}
</when_tool>

<when_tag analysis>
## Analysis Mode

Analysis depth: {{analysis_depth}}
Include metrics: {{include_metrics}}
</when_tag>

## Dynamic Content

Generated at: {{timestamp}}
Last modified: {{last_modified}}

---

All variables should be replaced: {{unset_variable}}
