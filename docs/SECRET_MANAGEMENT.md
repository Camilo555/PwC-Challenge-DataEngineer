# Secret Management System

## Overview

The application includes a comprehensive secret management system that supports multiple providers for secure storage and retrieval of sensitive configuration data.

## Supported Providers

### 1. Environment Variables (Development)
- **Provider**: `env`
- **Use Case**: Development and testing
- **Security**: Low (secrets stored in environment)

### 2. HashiCorp Vault (Recommended for Production)
- **Provider**: `vault`
- **Use Case**: Production deployments
- **Security**: High (enterprise-grade secret management)

### 3. AWS Secrets Manager
- **Provider**: `aws`  
- **Use Case**: AWS cloud deployments
- **Security**: High (AWS managed service)

## Configuration

### Environment Variables

Set the following environment variables to configure the secret provider:

```bash
# Secret provider selection
SECRET_PROVIDER=vault|aws|env

# HashiCorp Vault configuration
VAULT_URL=https://vault.example.com:8200
VAULT_TOKEN=your-vault-token
VAULT_MOUNT_POINT=secret

# AWS Secrets Manager configuration  
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

## Required Secrets

The system expects the following secrets to be configured:

### Critical Secrets (Required)
- `SECRET_KEY` - Application secret key
- `JWT_SECRET_KEY` - JWT signing key
- `DATABASE_URL` - Database connection string
- `BASIC_AUTH_PASSWORD` - Basic authentication password

### Optional Secrets
- `TYPESENSE_API_KEY` - Typesense search API key
- `SPARK_AUTH_SECRET` - Spark authentication secret
- `REDIS_PASSWORD` - Redis password
- `GRAFANA_ADMIN_PASSWORD` - Grafana admin password
- `SMTP_PASSWORD` - Email SMTP password
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key

## CLI Operations

### Initialize Secrets

Initialize missing secrets with auto-generated secure values:

```bash
# Initialize missing secrets
python scripts/secret_management.py init

# Force regenerate all secrets
python scripts/secret_management.py init --force
```

### Rotate Secrets

Rotate (regenerate) existing secrets:

```bash
# Rotate all secrets
python scripts/secret_management.py rotate

# Rotate specific secrets
python scripts/secret_management.py rotate SECRET_KEY JWT_SECRET_KEY
```

### Health Check

Check the health of the secret management system:

```bash
python scripts/secret_management.py health
```

### Export Secrets

Export secrets for deployment:

```bash
# Export as .env file
python scripts/secret_management.py export --format env

# Export to file
python scripts/secret_management.py export --format env --output .env.production

# Export as JSON/YAML
python scripts/secret_management.py export --format json
python scripts/secret_management.py export --format yaml
```

## Application Integration

### Automatic Initialization

The application automatically initializes secrets on startup in production:

```python
from core.startup import startup_application

# This will initialize secrets if needed
await startup_application()
```

### Manual Integration

Use the secret manager directly in your code:

```python
from core.security.secret_manager import get_secret, get_required_secret

# Get optional secret
api_key = await get_secret('TYPESENSE_API_KEY')

# Get required secret (raises error if missing)
database_url = await get_required_secret('DATABASE_URL')
```

### Configuration Integration

Use secrets in configuration classes:

```python
from core.config.secret_integration import SecretConfigMixin

class MyConfig(BaseSettings, SecretConfigMixin):
    secret_key: str
    
    @classmethod
    def create_with_secrets(cls):
        return cls.from_secrets()
```

## Deployment Scenarios

### Development Environment

```bash
# Use environment variables
export SECRET_PROVIDER=env
export SECRET_KEY=dev-secret-key
export DATABASE_URL=sqlite:///./dev.db

# Start application
python -m api.main
```

### Production with HashiCorp Vault

```bash
# Configure Vault
export SECRET_PROVIDER=vault
export VAULT_URL=https://vault.company.com:8200
export VAULT_TOKEN=your-vault-token

# Initialize secrets
python scripts/secret_management.py init

# Start application
python -m api.main
```

### Production with AWS Secrets Manager

```bash
# Configure AWS
export SECRET_PROVIDER=aws
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Initialize secrets
python scripts/secret_management.py init

# Start application
python -m api.main
```

### Docker Deployment

```dockerfile
FROM python:3.10-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY src/ /app/src/
COPY scripts/ /app/scripts/
WORKDIR /app

# Initialize secrets at runtime
RUN python scripts/secret_management.py init

# Start application
CMD ["python", "-m", "api.main"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: retail-etl
spec:
  template:
    spec:
      initContainers:
      - name: secret-init
        image: retail-etl:latest
        command: ["python", "scripts/secret_management.py", "init"]
        env:
        - name: SECRET_PROVIDER
          value: "vault"
        - name: VAULT_URL
          valueFrom:
            secretKeyRef:
              name: vault-config
              key: url
        - name: VAULT_TOKEN
          valueFrom:
            secretKeyRef:
              name: vault-config
              key: token
      containers:
      - name: app
        image: retail-etl:latest
        env:
        - name: SECRET_PROVIDER
          value: "vault"
```

## Security Best Practices

### 1. Provider Selection
- Use `env` only for development
- Use `vault` or `aws` for production
- Never commit secrets to version control

### 2. Secret Rotation
- Rotate secrets regularly (monthly/quarterly)
- Use automated rotation for critical secrets
- Monitor secret access patterns

### 3. Access Control
- Limit secret access to necessary services
- Use least-privilege principles
- Audit secret access logs

### 4. Monitoring
- Monitor secret health regularly
- Set up alerts for secret failures
- Track secret rotation schedules

## Troubleshooting

### Common Issues

1. **Secret Provider Not Available**
   ```
   Error: Secret provider 'vault' not configured
   ```
   - Check provider configuration
   - Verify network connectivity
   - Validate credentials

2. **Missing Required Secrets**
   ```
   Error: Required secret 'SECRET_KEY' not found
   ```
   - Run secret initialization: `python scripts/secret_management.py init`
   - Check provider connectivity
   - Verify secret exists in provider

3. **Authentication Failures**
   ```
   Error: Vault authentication failed
   ```
   - Check VAULT_TOKEN validity
   - Verify token permissions
   - Ensure token is not expired

### Health Check Commands

```bash
# Check secret system health
python scripts/secret_management.py health

# Check application startup
python src/core/startup.py

# Validate configuration
python -c "from core.config import settings; print('Config OK')"
```

### Debug Mode

Enable debug logging for secret operations:

```bash
export LOG_LEVEL=DEBUG
python scripts/secret_management.py health
```

## API Reference

### Secret Manager Functions

```python
# Get secret manager instance
from core.security.secret_manager import get_secret_manager
manager = get_secret_manager()

# Get secret
value = await manager.get_secret('SECRET_KEY')

# Set secret
success = await manager.set_secret('SECRET_KEY', 'new-value')

# Delete secret
success = await manager.delete_secret('SECRET_KEY')

# Get required secret (raises if missing)
value = await manager.get_required_secret('SECRET_KEY')
```

### Secret Initialization Functions

```python
# Initialize secrets
from core.security.secret_initialization import get_secret_initializer
initializer = get_secret_initializer()

# Initialize missing secrets
result = await initializer.initialize_secrets()

# Rotate secrets
result = await initializer.rotate_secrets(['SECRET_KEY'])

# Health check
health = await initializer.health_check()
```

## Monitoring and Alerting

### Metrics

The secret management system exposes the following metrics:

- `secret_manager_requests_total` - Total secret requests
- `secret_manager_errors_total` - Total secret errors
- `secret_manager_cache_hits_total` - Cache hit rate
- `secret_health_check_status` - Health check status

### Alerts

Set up alerts for:

- Secret provider connectivity issues
- Missing required secrets
- Authentication failures
- High error rates

### Logs

Monitor logs for:

- Secret initialization events
- Authentication failures
- Provider connectivity issues
- Health check results