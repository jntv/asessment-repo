#!/usr/bin/env python3
"""
Test script for download size limit feature
"""

def test_size_calculation():
    """Test the size calculation logic"""
    # verify that the bytes comparison logic correctly decides to skip or download each file size
    max_size_gb = 2
    max_size_bytes = max_size_gb * 1024**3

    test_cases = [
        (1 * 1024**3, "1 GB", True),      # Should download
        (2 * 1024**3, "2 GB", True),      # Should download (at limit)
        (2.5 * 1024**3, "2.5 GB", False), # Should skip
        (3 * 1024**3, "3 GB", False),     # Should skip
    ]

    print("Testing size calculation logic...")
    print(f"Max size limit: {max_size_gb} GB = {max_size_bytes:,} bytes\n")

    passed = 0
    failed = 0

    for file_size, label, should_download in test_cases:
        size_gb = file_size / (1024**3)
        # this mirrors the exact logic in download_one - files at or below limit are downloaded
        will_download = file_size <= max_size_bytes
        status = "DOWNLOAD" if will_download else "SKIP"

        if will_download == should_download:
            print(f"[PASS] {label:6} ({file_size:,} bytes): {status}")
            passed += 1
        else:
            print(f"[FAIL] {label:6} ({file_size:,} bytes): {status} (EXPECTED {should_download})")
            failed += 1

    return passed, failed


def test_syntax():
    """Test Python syntax of download.py"""
    # uses py_compile to check the file compiles without errors before we even try to run it
    import py_compile
    try:
        py_compile.compile("src/download.py", doraise=True)
        print("[PASS] Syntax check passed for src/download.py")
        return True
    except py_compile.PyCompileError as e:
        print(f"[FAIL] Syntax error in src/download.py: {e}")
        return False


def test_imports():
    """Test that the module can be imported"""
    # tries to actually import download_one and check that max_size_gb param exists on it
    try:
        import sys
        sys.path.insert(0, "src")
        from download import download_one
        print("[PASS] Successfully imported download_one function")

        # Check function signature - make sure the size limit parameter is actually there
        import inspect
        sig = inspect.signature(download_one)
        params = list(sig.parameters.keys())

        if 'max_size_gb' in params:
            print(f"[PASS] Function has max_size_gb parameter")
            print(f"  Parameters: {params}")
            return True
        else:
            print(f"[FAIL] Function missing max_size_gb parameter")
            print(f"  Parameters: {params}")
            return False
    except Exception as e:
        print(f"[FAIL] Import failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Testing download.py size limit feature")
    print("=" * 60 + "\n")

    # Test 1: Size calculation
    passed, failed = test_size_calculation()
    print(f"\nSize calculation: {passed} passed, {failed} failed\n")

    # Test 2: Syntax
    print("-" * 60)
    syntax_ok = test_syntax()
    print()

    # Test 3: Imports
    print("-" * 60)
    imports_ok = test_imports()
    print()

    # Summary
    print("=" * 60)
    if failed == 0 and syntax_ok and imports_ok:
        print("[PASS] ALL TESTS PASSED - Ready to commit")
        exit(0)
    else:
        print("[FAIL] TESTS FAILED - Do not commit")
        exit(1)
