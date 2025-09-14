#!/usr/bin/env python3
"""
Unit test to ensure no mock/demo data in production code
"""

import os
import re
import sys
import unittest
from pathlib import Path

class TestNoMocks(unittest.TestCase):
    """Ensure no mock/demo/sample data generators in codebase"""

    FORBIDDEN_PATTERNS = [
        r'\bmock\b',
        r'\bdemo\b',
        r'\bsample[\s_]*data\b',
        r'\bfake[\s_]*data\b',
        r'\btest[\s_]*data\b',
        r'\bdummy\b',
        r'Team\d+',
        r'Home\d+',
        r'Away\d+',
        r'generate_mock',
        r'generate_demo',
        r'generate_sample',
    ]

    EXCLUDE_DIRS = {
        '__pycache__',
        'venv',
        '.git',
        'docs',
        'tests',
        'node_modules',
        '.pytest_cache'
    }

    EXCLUDE_FILES = {
        'README.md',
        'INFRASTRUCTURE_ANALYSIS.md',
        'test_no_mocks.py',  # This file itself
        'assert_no_mocks.sh'
    }

    def test_no_mock_in_collectors(self):
        """Check collectors directory for mock data"""
        violations = self._scan_directory('collectors')
        self.assertEqual(len(violations), 0,
                        f"Found mock/demo violations in collectors:\n" + "\n".join(violations))

    def test_no_mock_in_services(self):
        """Check services directory for mock data"""
        violations = self._scan_directory('services')
        self.assertEqual(len(violations), 0,
                        f"Found mock/demo violations in services:\n" + "\n".join(violations))

    def test_no_mock_env_vars(self):
        """Ensure MOCK/DEMO env vars cause failure"""
        # Check if runtime would detect mock env vars
        mock_env_vars = ['MOCK', 'DEMO', 'USE_MOCK', 'USE_DEMO', 'ENABLE_MOCK']
        for var in mock_env_vars:
            if os.getenv(var, '').lower() in ['1', 'true', 'yes', 'on']:
                self.fail(f"Mock environment variable {var} is set. Production does not allow mocks.")

    def _scan_directory(self, directory):
        """Scan directory for forbidden patterns"""
        violations = []
        base_path = Path(__file__).parent.parent

        dir_path = base_path / directory
        if not dir_path.exists():
            return violations

        for file_path in dir_path.rglob('*.py'):
            # Skip excluded directories
            if any(excluded in file_path.parts for excluded in self.EXCLUDE_DIRS):
                continue

            # Skip excluded files
            if file_path.name in self.EXCLUDE_FILES:
                continue

            # Read and check file
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    line_num = 0
                    for line in content.splitlines():
                        line_num += 1
                        for pattern in self.FORBIDDEN_PATTERNS:
                            if re.search(pattern, line, re.IGNORECASE):
                                violations.append(
                                    f"{file_path.relative_to(base_path)}:{line_num} - "
                                    f"Pattern '{pattern}' found: {line.strip()[:80]}"
                                )
            except Exception as e:
                pass  # Skip files that can't be read

        return violations


if __name__ == '__main__':
    # Runtime guardrail - exit if mock env vars detected
    mock_vars = ['MOCK', 'DEMO', 'USE_MOCK', 'USE_DEMO', 'ENABLE_MOCK']
    for var in mock_vars:
        if os.getenv(var, '').lower() in ['1', 'true', 'yes', 'on']:
            print(f"ERROR: MOCK DISALLOWED - {var} environment variable is set")
            print("Production systems do not allow mock data. Exiting.")
            sys.exit(1)

    unittest.main()