# Enterprise Security Testing Framework

A comprehensive security testing suite for enterprise data engineering platforms, providing automated validation of security controls, compliance frameworks, and threat resistance.

## Overview

This framework provides comprehensive security testing across multiple domains:

- **Data Loss Prevention (DLP)** - Validates sensitive data detection, classification, and protection
- **Compliance Framework** - Tests GDPR, HIPAA, PCI-DSS, SOX compliance controls
- **Access Control** - RBAC/ABAC authentication and authorization testing
- **API Security** - REST API and GraphQL security validation
- **Penetration Testing** - Automated security vulnerability assessments
- **Performance Impact** - Security controls performance validation
- **Integration Testing** - End-to-end security workflow validation

## Quick Start

### Basic Usage

```bash
# Run all security tests
python scripts/run_security_tests.py

# Run specific categories
python scripts/run_security_tests.py --categories dlp compliance access_control

# Include penetration tests (use with caution)
python scripts/run_security_tests.py --include-penetration

# Verbose output with fail-fast
python scripts/run_security_tests.py --verbose --fail-fast
```

### Individual Test Categories

```bash
# DLP tests only
pytest tests/security/test_dlp_validation.py -v

# Compliance tests only  
pytest tests/security/test_compliance_framework.py -v

# Access control tests only
pytest tests/security/test_access_control.py -v

# API security tests only
pytest tests/security/test_api_security.py -v

# Performance impact tests
pytest tests/security/test_security_performance.py -v

# Integration tests
pytest tests/security/test_security_integration.py -v
```

### Using Test Markers

```bash
# Run all security tests
pytest -m security

# Run DLP tests
pytest -m dlp

# Run compliance tests
pytest -m compliance

# Run access control tests
pytest -m access_control

# Run API security tests
pytest -m api_security

# Run penetration tests (use with caution)
pytest -m penetration

# Run performance tests
pytest -m "security and performance"

# Run integration tests
pytest -m "security and integration"
```

## Test Categories

### 1. Data Loss Prevention (DLP) Tests

**File**: `test_dlp_validation.py`  
**Marker**: `@pytest.mark.dlp`

Tests the enterprise DLP system for:
- Sensitive data detection (SSN, credit cards, emails, phone numbers, etc.)
- Data classification and redaction
- Policy enforcement (GDPR, HIPAA, PCI-DSS, SOX)
- Performance under load
- Integration with APIs and databases

Key test classes:
- `TestSensitiveDataDetection` - Data pattern detection
- `TestDataClassification` - Classification accuracy
- `TestDataRedaction` - Redaction and masking
- `TestDLPPolicyEnforcement` - Policy compliance
- `TestDLPPerformance` - Performance characteristics

### 2. Compliance Framework Tests

**File**: `test_compliance_framework.py`  
**Marker**: `@pytest.mark.compliance`

Validates compliance with major regulatory frameworks:
- **GDPR** - Data protection and privacy rights
- **HIPAA** - Healthcare data protection
- **PCI-DSS** - Payment card security
- **SOX** - Financial reporting controls

Key test classes:
- `TestComplianceEngine` - Core framework functionality
- `TestGDPRCompliance` - GDPR-specific controls
- `TestHIPAACompliance` - HIPAA-specific controls
- `TestPCIDSSCompliance` - PCI-DSS-specific controls
- `TestSOXCompliance` - SOX-specific controls
- `TestComplianceViolations` - Violation handling

### 3. Access Control Tests

**File**: `test_access_control.py`  
**Marker**: `@pytest.mark.access_control`

Tests authentication and authorization systems:
- Role-Based Access Control (RBAC)
- Attribute-Based Access Control (ABAC)
- Multi-Factor Authentication (MFA)
- Session management
- JWT token security

Key test classes:
- `TestRBACManager` - Role and permission management
- `TestABACEngine` - Attribute-based policies
- `TestAuthentication` - User authentication
- `TestMFA` - Multi-factor authentication
- `TestAuthorization` - Access control decisions

### 4. API Security Tests

**File**: `test_api_security.py`  
**Marker**: `@pytest.mark.api_security`

Comprehensive API security validation:
- Authentication and authorization
- SQL injection prevention
- XSS attack prevention
- CSRF protection
- Input validation
- Rate limiting
- Security headers

Key test classes:
- `TestAPIAuthentication` - Auth mechanisms
- `TestSQLInjectionAttacks` - SQL injection resistance
- `TestXSSAttacks` - XSS prevention
- `TestCSRFAttacks` - CSRF protection
- `TestAuthenticationBypass` - Bypass prevention
- `TestInputValidationAttacks` - Input sanitization
- `TestAPIRateLimiting` - Rate limiting and DDoS protection
- `TestSecurityHeaders` - Security header validation

### 5. Penetration Testing Suite

**File**: `test_penetration_testing.py`  
**Marker**: `@pytest.mark.penetration`

⚠️ **Use with caution** - These tests simulate real attacks:
- Web application attacks
- Cryptographic attacks
- Business logic attacks
- Infrastructure attacks

Key test classes:
- `TestWebApplicationAttacks` - SSTI, NoSQL injection, etc.
- `TestCryptographicAttacks` - Weak encryption, timing attacks
- `TestBusinessLogicAttacks` - Race conditions, privilege escalation
- `TestInfrastructureAttacks` - HTTP method tampering, header injection

### 6. Security Performance Tests

**File**: `test_security_performance.py`  
**Marker**: `@pytest.mark.performance @pytest.mark.security`

Tests performance impact of security controls:
- DLP processing throughput
- Compliance assessment performance
- Authentication/authorization latency
- Security middleware overhead
- Concurrent operation handling

Key test classes:
- `TestDLPPerformance` - DLP system performance
- `TestCompliancePerformance` - Compliance framework performance
- `TestAuthenticationPerformance` - Auth system performance
- `TestSecurityMiddlewarePerformance` - Middleware impact

### 7. Security Integration Tests

**File**: `test_security_integration.py`  
**Marker**: `@pytest.mark.integration @pytest.mark.security`

End-to-end security workflow validation:
- Multi-layer security validation
- Security event correlation
- API security pipeline
- Compliance workflow integration
- Real-time threat detection

Key test classes:
- `TestSecurityWorkflowIntegration` - Complete workflows
- `TestAPISecurityIntegration` - API security pipelines
- `TestComplianceIntegration` - Compliance workflows
- `TestSecurityMonitoringIntegration` - Monitoring and alerting

## Configuration

### Test Fixtures and Utilities

The framework includes comprehensive fixtures in `conftest.py`:

- **Security Test Data** - Sample PII, attack payloads, test scenarios
- **DLP Manager** - Enterprise DLP system instance
- **Compliance Engine** - Compliance assessment engine
- **Auth Manager** - Authentication and authorization manager
- **Test Users** - Various user roles and permissions
- **API Client** - Authenticated test client
- **Mock Services** - External service mocks

### Environment Setup

1. **Install Dependencies**:
   ```bash
   pip install pytest pytest-asyncio pytest-cov hypothesis
   ```

2. **Configure Test Environment**:
   ```bash
   export SECURITY_TEST_MODE=true
   export DLP_ENCRYPTION_KEY=your_test_key
   ```

3. **Setup Test Database** (if needed):
   ```bash
   pytest --setup-only tests/security/
   ```

## Security Considerations

### Penetration Testing

⚠️ **Important**: Penetration tests simulate real attacks and should only be run in controlled environments:

```bash
# Only run penetration tests when explicitly requested
pytest -m penetration --tb=short

# Or use the security test runner
python scripts/run_security_tests.py --include-penetration
```

### Test Data Security

- All test data uses synthetic/dummy values
- No real PII or sensitive data is used
- Test encryption keys are isolated from production
- Test results are sanitized before logging

### Network Isolation

- Tests run in isolated environments
- No external network access required (mocked)
- Local services only for integration tests

## Reporting and Analysis

### Automated Reports

The security test runner generates comprehensive reports:

```bash
# Generate full reports
python scripts/run_security_tests.py

# Reports generated in reports/security/:
# - security_test_report_YYYYMMDD_HHMMSS.json
# - security_test_report_YYYYMMDD_HHMMSS.html  
# - security_test_summary_YYYYMMDD_HHMMSS.csv
```

### Coverage Analysis

Security test coverage can be analyzed:

```bash
# Run with coverage
pytest tests/security/ --cov=src --cov-report=html

# Security-specific coverage
pytest -m security --cov=src/core/security --cov-report=html
```

### Performance Benchmarking

Performance tests generate benchmarks:

```bash
# Run performance tests with benchmarking
pytest -m "security and performance" --benchmark-enable
```

## CI/CD Integration

### GitHub Actions

Example workflow for automated security testing:

```yaml
name: Security Tests
on: [push, pull_request]

jobs:
  security-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Dependencies
        run: pip install -r requirements.txt
      
      - name: Run Security Tests
        run: python scripts/run_security_tests.py --categories dlp compliance access_control api_security
      
      - name: Upload Security Reports
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: security-reports
          path: reports/security/
```

### Pre-commit Hooks

Add security tests to pre-commit:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: security-tests
        name: Security Tests
        entry: python scripts/run_security_tests.py --categories dlp access_control --fail-fast
        language: python
        pass_filenames: false
```

## Best Practices

### Test Organization

1. **Categorize tests** using appropriate markers
2. **Use descriptive test names** explaining the security aspect being tested
3. **Include both positive and negative test cases**
4. **Document security assumptions** and test limitations

### Security Testing Guidelines

1. **Start with critical security controls** (DLP, access control)
2. **Run compliance tests regularly** for regulatory requirements
3. **Use penetration tests sparingly** and in controlled environments
4. **Monitor performance impact** of security controls
5. **Validate end-to-end security workflows**

### Maintenance

1. **Update attack patterns** regularly to match current threats
2. **Review compliance controls** when regulations change
3. **Benchmark performance** after security updates
4. **Validate test coverage** of new security features

## Troubleshooting

### Common Issues

1. **Test Timeouts**:
   ```bash
   # Increase timeout for complex tests
   pytest tests/security/ --timeout=600
   ```

2. **Permission Errors**:
   ```bash
   # Check test user permissions
   pytest tests/security/test_access_control.py -v
   ```

3. **DLP Detection Issues**:
   ```bash
   # Debug DLP pattern matching
   pytest tests/security/test_dlp_validation.py::TestSensitiveDataDetection -v -s
   ```

4. **Compliance Assessment Failures**:
   ```bash
   # Run specific compliance framework
   pytest -m compliance -k "gdpr" -v
   ```

### Debug Mode

Enable debug mode for detailed security test information:

```bash
# Debug mode with detailed logging
SECURITY_DEBUG=true pytest tests/security/ -v -s --log-cli-level=DEBUG
```

## Contributing

When adding new security tests:

1. Follow the existing test structure and naming conventions
2. Use appropriate pytest markers
3. Include comprehensive docstrings
4. Add both positive and negative test cases
5. Update this README with new test categories
6. Ensure tests are production-safe (no real data, no external calls)

## Security Test Results Interpretation

### Success Criteria

- **DLP Tests**: All sensitive data types detected with >90% accuracy
- **Compliance Tests**: All critical controls assessed as compliant
- **Access Control Tests**: All unauthorized access attempts blocked
- **API Security Tests**: All injection attacks prevented
- **Performance Tests**: Security overhead <10% performance impact

### Failure Analysis

Review test failures by category:
1. **Critical Failures**: Address immediately (access control, compliance violations)
2. **Security Vulnerabilities**: Patch and re-test
3. **Performance Issues**: Optimize or accept trade-offs
4. **Configuration Issues**: Update security policies

## Support

For security testing framework support:
- Review test documentation and examples
- Check troubleshooting section for common issues
- Analyze test reports for specific failure details
- Use debug mode for detailed investigation

Remember: Security testing is an ongoing process. Regularly run tests, update attack patterns, and review security controls to maintain a strong security posture.