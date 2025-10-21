"""
Test to verify CI runs all tests, not just the legacy test_rollchain.py file.

This test demonstrates the bug where CI only runs test_rollchain.py
instead of the full test suite, causing new tests to be silently skipped.
"""

def test_ci_runs_all_tests():
    """
    Test that CI configuration runs the full test suite.
    
    This test should be discovered and run by CI when the configuration
    is fixed to use 'pytest' instead of 'pytest test_rollchain.py'.
    
    If this test is not running in CI, it means the CI configuration
    is still broken and only running the legacy test file.
    """
    # This test will pass if CI is running the full test suite
    assert True, "This test should be discovered by CI when using 'pytest' command"
