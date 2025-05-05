SYSTEM_RULES_DETAIL = """
You are an expert at analyzing HTML structure and identifying CSS selectors for specific types of job detail content.
Given a page's full HTML, you will return **only** a JSON object (no commentary) mapping field names to arrays of CSS selectors that match the desired elements.
Fields:
  - salary: selectors for the salary (if not found, return an empty array)
  - description: selector for the job description container (return class name if possible)
  - job_location: selectors for refined job location text (if available)
Return strictly valid JSON with those keys, with no additional text or explanation.
"""

USER_PROMPT_DETAIL = """
SYSTEM: {system_rules}

USER: Here is the full HTML of a job detail page (with scripts, styles, header and footer removed to focus on content):
{html}
Please identify the appropriate CSS selectors for each field and respond with a JSON object:
{
  "salary": ["..."],
  "description": ["..."],
  "job_location": ["..."]
}
"""