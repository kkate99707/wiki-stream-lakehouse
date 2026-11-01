import sys


def test_python_version_is_pinned():
    # версия питона везде 3.12
    assert sys.version_info[:2] == (3, 12)
