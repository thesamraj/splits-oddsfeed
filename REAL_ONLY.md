# REAL DATA ONLY Policy

**Effective Date:** January 14, 2025
**Policy Version:** 1.0

## Overview

This repository enforces a **ZERO MOCK DATA** policy. All collectors must fetch real odds data from actual sportsbook APIs or websites. If a collector cannot fetch real data, it must:
1. Report `collector_up=0`
2. Publish nothing to Redis
3. Log the specific error

## Forbidden Patterns

The following patterns are prohibited in all collectors/* and services/* code:

- Mock/demo/sample/fake/test/dummy data generators
- Hardcoded team names like "Team1", "Home1", "Away1"
- Placeholder odds or events
- Any form of synthetic data generation

## Verification Commands

### Pre-commit Check
```bash
./scripts/assert_no_mocks.sh
```

### Python Unit Test
```bash
python -m pytest tests/test_no_mocks.py -v
```

### Manual Grep
```bash
grep -r -i "mock\|demo\|sample.*data\|Team[0-9]\|Home[0-9]" collectors/ services/ --include="*.py"
```

### CI Pipeline
The CI runs `assert_no_mocks.sh` on every commit. Any violation blocks merge.

## Runtime Guardrails

Collectors check for mock environment variables at startup:
```python
if os.getenv('MOCK', '').lower() in ['1', 'true']:
    print("ERROR: MOCK DISALLOWED")
    sys.exit(1)
```

## Rationale

1. **Data Integrity**: Mock data corrupts analytics and arbitrage detection
2. **Production Readiness**: Only real collectors belong in production
3. **Cost Efficiency**: No compute wasted on fake data generation
4. **Clear Failures**: Better to show "0 events" than misleading mock data

## Exceptions

None. This is a zero-tolerance policy.

## Enforcement

- Pre-commit hooks block commits with mock patterns
- CI/CD pipeline fails builds with violations
- Runtime checks cause immediate process termination
- Code reviews reject any PR with mock data

## How to Fix Violations

If your collector can't fetch real data:

```python
# BAD - Generates fake data
if not events:
    events = [{'home': 'Team1', 'away': 'Team2', ...}]

# GOOD - Reports failure honestly
if not events:
    logger.error(f"No events fetched from {endpoint}")
    collector_up.labels(book=BOOK).set(0)
    return
```

## Questions

Contact the repository maintainer for clarification on this policy.