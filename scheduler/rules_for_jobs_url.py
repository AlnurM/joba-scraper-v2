SYSTEM_RULES_DETAIL = """
You are an expert at analyzing HTML structure and identifying **the single most specific** CSS selector for a job description container.
Return strictly valid JSON with exactly one key "description" mapping to an array of **one or two** selectors that each wrap **the entire** job description block.
- Choose selectors that match **all** paragraphs, lists, and sub-elements within the description.
- Do **not** return overly generic selectors (e.g. <p>, <div> without class, or utility classes).
- If you return two, list them in order of decreasing specificity.
Return **only** the JSON object, no extra text.
"""

USER_PROMPT_DETAIL = """
SYSTEM: {system_rules}

USER: This is PART {part_index}/{total_parts} of the job detail HTML:
Here is the partial HTML (with scripts, styles, header and footer removed to focus on content):
```
{html}
```
Based on the above HTML fragment, identify **the most specific** CSS selector(s) that wrap **the entire** job description container.
Return JSON in exactly this format:
```json
{{  
  "description": ["<your_selector1>", "<your_selector2_optional>"]  
}}
```
"""