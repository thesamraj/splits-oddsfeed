# Security Policy

## Secrets Management

### Core Principles
1. **Never commit real credentials** to the repository
2. Use environment variables for all sensitive configuration
3. Store secrets only in:
   - Local `.env` files (gitignored)
   - Render/Cloud provider dashboard (encrypted at rest)
   - Dedicated secret managers (AWS Secrets Manager, HashiCorp Vault)

### Credential Types
- **Database URLs**: PostgreSQL/Neon connection strings
- **API Keys**: Third-party service credentials
- **Webhooks**: Slack, Discord, monitoring endpoints
- **Proxy URLs**: Residential proxy credentials
- **JWT Secrets**: Token signing keys

### Local Development
```bash
# 1. Copy the example file
cp .env.example .env

# 2. Edit .env with real credentials (this file is gitignored)
vim .env

# 3. Never commit .env or any file with real credentials
git status  # Verify .env is not staged
```

### Production Deployment
1. Set secrets in Render dashboard (Environment tab)
2. Use `sync: false` in render.yaml for sensitive vars
3. Rotate credentials regularly (quarterly minimum)

## CI Security Scanners

### Automated Scanning
The repository runs multiple security scanners on every push/PR:

1. **Gitleaks**: Detects secrets in git history
   - Config: `.gitleaks.toml`
   - Action: `.github/workflows/secret_scan.yml`

2. **TruffleHog**: Verifies exposed credentials
   - Checks if credentials are active
   - Only reports verified findings

3. **Custom Scanner**: `scripts/verify_no_secrets.sh`
   - Checks for Neon URLs, API keys, private keys
   - Runs in CI and locally

### Pre-commit Hooks
```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Run manually
pre-commit run --all-files

# Or use Make target
make verify
```

Pre-commit automatically runs:
- Secret scanning (verify_no_secrets.sh)
- Denylist checking (deny_generators.sh --dry-run)
- Gitleaks for git history
- Python formatting (black, ruff)
- YAML validation
- Large file prevention

### Manual Verification
```bash
# Run secret scan locally
./scripts/verify_no_secrets.sh

# Check for sandbox references
./scripts/deny_generators.sh

# Scan git history
docker run --rm -v $(pwd):/code zricethezav/gitleaks:latest detect --source="/code"
```

## Incident Response

### If Secrets Are Exposed
1. **Immediate Actions**:
   - Rotate the exposed credential immediately
   - Update all services using the credential
   - Check logs for unauthorized access

2. **Remediation**:
   - Remove secret from repository
   - If in history, consider rewriting git history
   - Update .gitignore to prevent recurrence

3. **Documentation**:
   - Create incident report in `docs/incidents/`
   - Document timeline, impact, and lessons learned

### Credential Rotation

#### Neon Database
1. Log into Neon Console
2. Navigate to Settings → Connection
3. Reset password or regenerate connection string
4. Update in:
   - Local `.env`
   - Render dashboard
   - Any other services

#### API Keys
1. Generate new key from provider dashboard
2. Update all references
3. Revoke old key after confirming new key works

## Security Checklist

### Before Committing
- [ ] Run `./scripts/verify_no_secrets.sh`
- [ ] Check `git diff` for any credentials
- [ ] Verify `.env` is in `.gitignore`
- [ ] Use placeholders in `.env.example`

### Before Deploying
- [ ] All secrets set in Render dashboard
- [ ] No hardcoded credentials in Docker images
- [ ] Proxy URLs configured if needed
- [ ] Database uses SSL/TLS

### Regular Audits
- [ ] Quarterly credential rotation
- [ ] Review access logs
- [ ] Update dependencies for security patches
- [ ] Scan for vulnerable dependencies

## Reporting Security Issues

If you discover a security vulnerability:

1. **Do NOT** open a public issue
2. Email security details to: [security@your-domain.com]
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

## Data Retention and Privacy

### Automated Data Cleanup
To minimize exposure and comply with data retention policies:

1. **Automated Retention (GitHub Actions)**
   - Workflow: `.github/workflows/neon_retention.yml`
   - Schedule: Weekly on Sundays at 4:05 AM UTC
   - Retention periods:
     - Ticks: 45 days
     - Odds: 90 days

2. **Manual Cleanup**
   ```bash
   # Delete old ticks
   psql "$DATABASE_URL" -c "DELETE FROM odds_ticks WHERE ts < now() - interval '45 days';"

   # Delete old odds
   psql "$DATABASE_URL" -c "DELETE FROM odds WHERE ts < now() - interval '90 days';"
   ```

3. **Monitoring**
   - Check retention job status in GitHub Actions
   - Slack notifications on failure (if configured)
   - Statistics reported after each run

### Data Privacy Best Practices
- Never store PII (Personally Identifiable Information)
- Use minimal data retention periods
- Implement secure deletion (no soft deletes for sensitive data)
- Regular audits of data access patterns

## Tools and Resources

- [Gitleaks](https://github.com/zricethezav/gitleaks)
- [TruffleHog](https://github.com/trufflesecurity/trufflehog)
- [OWASP Dependency Check](https://owasp.org/www-project-dependency-check/)
- [Snyk](https://snyk.io/) - Vulnerability scanning
- [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/)
- [HashiCorp Vault](https://www.vaultproject.io/)
#### Render Service Secrets
1. Open the Render service → Environment tab.
2. Update secret values (ensure `sync: false` in `render.yaml`).
3. Redeploy affected services to pick up new env.

### If Secret Scan Fails in CI
1. Treat the finding as real until proven otherwise.
2. Inspect the workflow logs and the reported file/line.
3. Remove the secret and replace with a placeholder immediately.
4. Rotate the exposed credential (Neon/Render/API provider) and update env.
5. If the secret is in git history, consider history rewrite; document in `docs/incidents/`.
