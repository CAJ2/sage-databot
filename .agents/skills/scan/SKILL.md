---
name: scan
description: Use when the user says "run a scan", "check for security issues", "check codebase health", or asks about recent scan results. Shows codebase scan results from cubic, including security vulnerabilities and code quality issues.
---

# Codebase Scan Results

This skill fetches and presents codebase scan results from cubic to help identify security vulnerabilities, data integrity issues, and code quality problems.

## When to Activate

- User wants to check the overall health of the codebase
- User asks for security vulnerabilities or "bugs across the repo"
- User mentions "running a scan" or "checking scan results"
- User asks about a specific issue identified in a scan

## How to Use

1. **Detect the repository**: Run `git remote get-url origin` to extract the owner and repo name.

2. **List recent scans**: If the user hasn't provided a specific scan ID, call `list_scans` with the owner and repo.
   - Present the list of recent scans with their status, issue count, and date.
   - Ask the user if they want to see the details for a specific scan.

3. **Get scan details**: If a scan ID is provided (or once the user selects one), call `get_scan` with the scanId.
   - Group issues by category: Security, Data Integrity, Business Logic, Stability.
   - For each category, show the number of issues and their severity levels.

4. **Deep dive into issues**: If the user asks about a specific issue or category:
   - Call `get_issue` with the issue ID to show the full analysis report.
   - Present the code context, the reason why it was flagged, and the suggested remediation.

## Presentation

- Start with a high-level summary of the scan (e.g., "Found 12 security issues and 5 code quality problems").
- Use a table or clear list to show categories and issue counts.
- Highlight critical and high-severity issues.
- Offer to fix specific issues once they've been reviewed.
