# requirements: project

"""
Integration test framework for Windmill scripts.

Provides Test, TestSuite, and assertion helpers.
Each test script returns results via TestSuite.results().
"""

import time
import traceback
from dataclasses import dataclass, field, asdict

from f.test.cleanup import CleanupTracker


class Test:
    """Context object passed to each test function."""

    def __init__(self, cleanup: CleanupTracker):
        self.cleanup = cleanup


@dataclass
class TestResult:
    name: str
    passed: bool
    duration_ms: float
    error: str | None = None
    details: str | None = None


@dataclass
class TestSuite:
    """
    Collects and runs test functions, recording pass/fail results.

    Usage:
        suite = TestSuite("my_tests")
        suite.run(test_func)
        return suite.results()
    """

    name: str
    _results: list[TestResult] = field(default_factory=list, repr=False)
    _cleanup: CleanupTracker = field(default_factory=CleanupTracker, repr=False)

    def run(self, test_fn):
        """Run a test function, passing a Test context with cleanup."""
        name = test_fn.__name__
        t = Test(self._cleanup)
        start = time.monotonic()
        try:
            test_fn(t)
            elapsed = (time.monotonic() - start) * 1000
            self._results.append(TestResult(name=name, passed=True, duration_ms=elapsed))
            print(f"  ✅ {name} ({elapsed:.0f}ms)")
        except AssertionError as e:
            elapsed = (time.monotonic() - start) * 1000
            self._results.append(
                TestResult(
                    name=name,
                    passed=False,
                    duration_ms=elapsed,
                    error=str(e),
                    details=traceback.format_exc(),
                )
            )
            print(f"  ❌ {name}: {e}")
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            self._results.append(
                TestResult(
                    name=name,
                    passed=False,
                    duration_ms=elapsed,
                    error=f"{type(e).__name__}: {e}",
                    details=traceback.format_exc(),
                )
            )
            print(f"  💥 {name}: {type(e).__name__}: {e}")

    def cleanup(self):
        """Execute cleanup for all tracked resources."""
        self._cleanup.execute()

    def results(self) -> dict[str, object]:
        """Return serializable results dict. Runs cleanup first."""
        self.cleanup()
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        failed = total - passed
        duration_ms = sum(r.duration_ms for r in self._results)
        return {
            "suite_name": self.name,
            "tests": [asdict(r) for r in self._results],
            "total": total,
            "passed": passed,
            "failed": failed,
            "duration_ms": duration_ms,
        }


# --- Assertion helpers ---

def assert_eq(actual, expected, msg: str = ""):
    """Assert that actual == expected."""
    if actual != expected:
        detail = f"Expected {expected!r}, got {actual!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_ne(actual, not_expected, msg: str = ""):
    """Assert that actual != not_expected."""
    if actual == not_expected:
        detail = f"Did not expect {not_expected!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_true(value, msg: str = ""):
    """Assert that value is truthy."""
    if not value:
        detail = f"Expected truthy, got {value!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_false(value, msg: str = ""):
    """Assert that value is falsy."""
    if value:
        detail = f"Expected falsy, got {value!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_contains(container, item, msg: str = ""):
    """Assert that item is in container."""
    if item not in container:
        detail = f"{item!r} not found in {container!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_isinstance(obj, cls, msg: str = ""):
    """Assert that obj is an instance of cls."""
    if not isinstance(obj, cls):
        detail = f"Expected instance of {cls.__name__}, got {type(obj).__name__}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_raises(exc_type, fn, *args, **kwargs):
    """Assert that fn(*args, **kwargs) raises exc_type."""
    try:
        fn(*args, **kwargs)
    except exc_type:
        return
    except Exception as e:
        raise AssertionError(f"Expected {exc_type.__name__}, got {type(e).__name__}: {e}")
    raise AssertionError(f"Expected {exc_type.__name__}, but no exception was raised")


def assert_len(container, expected_len: int, msg: str = ""):
    """Assert that len(container) == expected_len."""
    actual = len(container)
    if actual != expected_len:
        detail = f"Expected length {expected_len}, got {actual}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_startswith(s: str, prefix: str, msg: str = ""):
    """Assert that string s starts with prefix."""
    if not s.startswith(prefix):
        detail = f"{s!r} does not start with {prefix!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def assert_gt(actual, threshold, msg: str = ""):
    """Assert that actual > threshold."""
    if not actual > threshold:
        detail = f"Expected > {threshold!r}, got {actual!r}"
        raise AssertionError(f"{msg}: {detail}" if msg else detail)


def main():
    """Self-test the framework."""
    suite = TestSuite("framework_self_test")

    def test_assert_eq_pass(t: Test):
        assert_eq(1, 1)
        assert_eq("hello", "hello")

    def test_assert_eq_fail(t: Test):
        assert_raises(AssertionError, assert_eq, 1, 2)

    def test_assert_true_pass(t: Test):
        assert_true(True)
        assert_true(1)
        assert_true("nonempty")

    def test_assert_contains_pass(t: Test):
        assert_contains([1, 2, 3], 2)
        assert_contains("hello world", "world")

    def test_assert_raises_pass(t: Test):
        def raise_value_error():
            raise ValueError("test")
        assert_raises(ValueError, raise_value_error)

    suite.run(test_assert_eq_pass)
    suite.run(test_assert_eq_fail)
    suite.run(test_assert_true_pass)
    suite.run(test_assert_contains_pass)
    suite.run(test_assert_raises_pass)

    return suite.results()
