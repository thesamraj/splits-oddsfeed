# UNIBET API CREDENTIALS ACQUISITION MEMO

## Overview
Unibet requires authenticated API access through the Kindred Group developer portal. This memo outlines the steps for obtaining legitimate access when business requirements are met.

## Required Steps

### 1. Business Registration
- **Portal**: https://developer.kindredgroup.com/
- **Requirements**:
  - Valid business entity
  - Clear use case justification
  - Compliance with gambling regulations
  - Data handling and security policies

### 2. Application Process
- **Documentation Needed**:
  - Business license and registration
  - Technical integration plan
  - Data usage and retention policies
  - Security and compliance certifications
  - Geographic market focus

### 3. API Access Request
- **Specify Requirements**:
  - Unibet odds feed access
  - Real-time and historical data needs
  - Geographic markets (US, EU, etc.)
  - Expected API call volume
  - Integration timeline

### 4. IP Whitelisting
- **Production IPs**: Submit for approval
- **Development IPs**: Include staging/test environments
- **Security Requirements**: SSL/TLS, proper authentication

### 5. Credentials Integration
Once approved, update collectors with:
```yaml
environment:
  - KAMBI_API_KEY=<provided_key>
  - KAMBI_CLIENT_ID=<provided_id>
  - KAMBI_BRAND=<authenticated_token>
  - KAMBI_BASE_URL=<authenticated_endpoint>
```

## Timeline
- **Application Review**: 1-2 weeks
- **Business Approval**: 2-4 weeks
- **Technical Setup**: 1 week
- **Total**: 4-7 weeks typical

## Technical Readiness
✅ UB collector infrastructure complete
✅ Normalizer mapping ready
✅ API endpoints configured
✅ Database schema supports UB brand

## Success Criteria
- HTTP 200 responses with JSON odds data
- Events[] arrays populated with markets
- Sub-second API response times
- Proper brand=unibet mapping

## Contact Information
- **Portal**: https://developer.kindredgroup.com/
- **Support**: developer-support@kindredgroup.com
- **Business Development**: partnership-team@kindredgroup.com

**Note**: This is for legitimate business use only. All gambling regulations and compliance requirements must be met.
