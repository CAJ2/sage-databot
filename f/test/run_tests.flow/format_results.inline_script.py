from typing import Any


def main(test_results: list[dict[str, Any]]):
    """Format test results into a markdown summary for PR comments."""
    all_suites = []
    total_tests = 0
    total_passed = 0
    total_failed = 0
    total_duration_ms = 0.0

    for result in test_results:
        suite_name = result.get("suite_name", "unknown")
        tests = result.get("tests", [])
        passed = result.get("passed", 0)
        failed = result.get("failed", 0)
        duration_ms = result.get("duration_ms", 0.0)

        all_suites.append(
            {
                "name": suite_name,
                "total": len(tests),
                "passed": passed,
                "failed": failed,
                "duration_ms": duration_ms,
                "tests": tests,
            }
        )
        total_tests += len(tests)
        total_passed += passed
        total_failed += failed
        total_duration_ms += duration_ms

    # Format duration
    total_secs = total_duration_ms / 1000
    if total_secs >= 60:
        duration_str = f"{int(total_secs // 60)}m {int(total_secs % 60)}s"
    else:
        duration_str = f"{total_secs:.1f}s"

    status_emoji = "✅" if total_failed == 0 else "❌"
    status_line = (
        f"{status_emoji} {total_passed}/{total_tests} passed | ⏱️ {duration_str}"
    )

    # Build markdown
    lines = [
        "## 🧪 Integration Test Results",
        "",
        f"**Status:** {status_line}",
        "",
        "| Suite | Tests | Passed | Failed | Duration |",
        "|-------|-------|--------|--------|----------|",
    ]

    for suite in all_suites:
        s_dur = f"{suite['duration_ms'] / 1000:.1f}s"
        lines.append(
            f"| {suite['name']} | {suite['total']} | {suite['passed']} | {suite['failed']} | {s_dur} |"
        )

    # Failed test details
    failed_tests = []
    for suite in all_suites:
        for test in suite["tests"]:
            if not test.get("passed", True):
                failed_tests.append(
                    f"- **{suite['name']}/{test.get('name', 'unknown')}**: {test.get('error', 'Unknown error')}"
                )

    lines.append("")
    if failed_tests:
        lines.append(f"<details><summary>Failed Tests ({len(failed_tests)})</summary>")
        lines.append("")
        lines.extend(failed_tests)
        lines.append("")
        lines.append("</details>")
    else:
        lines.append("<details><summary>Failed Tests (0)</summary>")
        lines.append("")
        lines.append("None 🎉")
        lines.append("")
        lines.append("</details>")

    markdown = "\n".join(lines)

    return {
        "summary": status_line,
        "markdown": markdown,
        "suites": all_suites,
        "total": total_tests,
        "passed": total_passed,
        "failed": total_failed,
        "duration_ms": total_duration_ms,
        "success": total_failed == 0,
    }
