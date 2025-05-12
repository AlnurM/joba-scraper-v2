SYSTEM_RULES_DETAIL = """
You are an expert at analyzing HTML structure and identifying CSS selectors for specific types of job detail content.
Given a page's full HTML, you will return **only** a JSON object (no commentary) mapping the field name to an array of CSS selectors that match the job description container.
Fields:
  - description: selector for the main job description container. This should match the element that wraps all the text/details of the job posting.
Return strictly valid JSON with exactly that one key and an array of selectors.
"""

USER_PROMPT_DETAIL = """
SYSTEM: {system_rules}

USER: This is PART {part_index}/{total_parts} of the job detail HTML:
Here is the partial HTML (with scripts, styles, header and footer removed to focus on content):
```
{html}
```
Please identify the appropriate CSS selectors for the job description container and respond with a JSON object:
```json
{{  
  "description": ["..."]  
}}
```
"""