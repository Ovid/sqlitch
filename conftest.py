# conftest.py
import os
import pytest

@pytest.fixture(autouse=True)
def check_sqitch_conf():
    before = os.path.exists('sqitch.conf')
    yield
    after = os.path.exists('sqitch.conf')
    if after and not before:
        # This will show you exactly which test created it
        test_name = os.environ.get('PYTEST_CURRENT_TEST', 'unknown')
        pytest.fail(f"Test created sqitch.conf and didn't clean up: {test_name}", pytrace=False)
