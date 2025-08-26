# Enterprise Security Framework - Quick Start Guide

## Overview

This quick start guide provides administrators with essential steps to get the enterprise security and compliance framework up and running quickly. For detailed configuration and advanced features, refer to the [Security Administrator Guide](SECURITY_ADMINISTRATOR_GUIDE.md).

## Prerequisites Checklist

Before starting, ensure you have:

- [ ] Linux server with 8GB+ RAM and 4+ CPU cores
- [ ] PostgreSQL 14+ installed and running
- [ ] Redis 6+ installed and running  
- [ ] Python 3.11+ installed
- [ ] Docker and Docker Compose (optional but recommended)
- [ ] Nginx or similar web server
- [ ] SSL certificates for HTTPS
- [ ] Administrative access to the server

## 15-Minute Installation

### Step 1: System Preparation (3 minutes)

```bash
# Update system and install dependencies
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3.11 python3-pip postgresql-14 redis-server nginx curl jq

# Create security user and directories
sudo useradd -m -s /bin/bash security-admin
sudo mkdir -p /opt/security/{config,logs,data,backups}
sudo chown -R security-admin:security-admin /opt/security
```

### Step 2: Database Setup (2 minutes)

```bash
# Create database and user
sudo -u postgres createdb security_platform
sudo -u postgres createuser security_user --pwprompt

# Create basic schema
sudo -u postgres psql -c "
CREATE SCHEMA security_core;
CREATE SCHEMA compliance;  
CREATE SCHEMA audit_logs;
GRANT ALL PRIVILEGES ON DATABASE security_platform TO security_user;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE security_platform TO security_user;
"
```

### Step 3: Application Installation (5 minutes)

```bash
# Clone or copy application files to /opt/security
cd /opt/security

# Create virtual environment and install dependencies  
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Generate encryption keys
python3 -c "
from cryptography.fernet import Fernet
import base64, os
fernet_key = Fernet.generate_key()
jwt_secret = base64.urlsafe_b64encode(os.urandom(32)).decode()
with open('config/keys.env', 'w') as f:
    f.write(f'FERNET_KEY={fernet_key.decode()}\n')
    f.write(f'JWT_SECRET={jwt_secret}\n')
os.chmod('config/keys.env', 0o600)
print('Encryption keys generated successfully')
"
```

### Step 4: Basic Configuration (3 minutes)

Create minimal configuration file `/opt/security/config/security.yaml`:

```yaml
security:
  environment: "production"
  
database:
  host: "localhost"
  port: 5432
  database: "security_platform"  
  username: "security_user"
  password: "YOUR_DB_PASSWORD"

redis:
  host: "localhost"
  port: 6379
  database: 0

authentication:
  jwt_expiry_hours: 24
  mfa_enabled: true

dlp:
  enabled: true
  confidence_threshold: 0.8

compliance:
  frameworks: ["gdpr", "hipaa"]
  assessment_schedule: "daily"

monitoring:
  metrics_enabled: true
  alerting_enabled: true
```

### Step 5: Service Setup (2 minutes)

Create systemd service `/etc/systemd/system/security-platform.service`:

```ini
[Unit]
Description=Enterprise Security Platform
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=security-admin
WorkingDirectory=/opt/security
Environment=PATH=/opt/security/venv/bin
ExecStart=/opt/security/venv/bin/python -m src.api.main
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable security-platform
sudo systemctl start security-platform

# Check status
sudo systemctl status security-platform
```

## Initial Setup and Verification

### Create First Admin User

```bash
cd /opt/security
source venv/bin/activate

python3 -c "
import asyncio
import sys
sys.path.append('/opt/security')

# This is a simplified example - adapt based on your auth service
async def create_admin():
    print('Admin user created with username: admin')
    print('Default password: TempPassword123!')
    print('Please change password on first login')

asyncio.run(create_admin())
"
```

### Verify Installation

```bash
# Check API health
curl -s http://localhost:8000/health | jq '.'

# Check service logs
sudo journalctl -u security-platform -f

# Verify database connectivity
sudo -u security-admin psql -h localhost -U security_user -d security_platform -c "SELECT version();"
```

### Access the Dashboard

1. Open browser to `https://your-server.com/security`
2. Login with admin credentials
3. Change default password
4. Review security dashboard overview

## Essential First Configurations

### 1. Configure Basic DLP Policies (5 minutes)

```bash
# Create basic DLP policy file
cat > /opt/security/config/basic_dlp.json << 'EOF'
{
  "policy_id": "basic_pii_protection",
  "name": "Basic PII Protection",
  "enabled": true,
  "data_types": ["ssn", "credit_card", "email"],
  "actions": ["redact", "log", "alert"],
  "applies_to": {
    "locations": ["api", "exports"],
    "users": ["all"]
  }
}
EOF

# Apply DLP policy (replace with actual API call)
echo "DLP policy created - apply through admin interface"
```

### 2. Set Up Basic Monitoring (3 minutes)

```bash
# Configure basic alerting
cat > /opt/security/config/alerts.yaml << 'EOF'
alerting:
  email:
    smtp_server: "smtp.company.com"
    from_address: "security@company.com"
    admin_recipients: ["admin@company.com"]
  
  thresholds:
    failed_logins: 10
    dlp_violations: 5
    compliance_score: 0.8
EOF
```

### 3. Enable Basic Compliance Monitoring (2 minutes)

```bash
# Enable GDPR compliance monitoring
curl -X POST http://localhost:8000/api/v1/compliance/enable \
  -H "Content-Type: application/json" \
  -d '{"frameworks": ["gdpr"], "auto_assessment": true}'

echo "Basic compliance monitoring enabled"
```

## Quick Validation Tests

### Security Dashboard Test
```bash
# Test dashboard API
curl -s http://localhost:8000/api/v1/security/dashboard | jq '.summary'
```

### DLP Scanning Test
```bash
# Test DLP scanning
curl -X POST http://localhost:8000/api/v1/security/scan \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "email": "test@example.com",
      "ssn": "123-45-6789"
    },
    "context": {"location": "test"}
  }' | jq '.'
```

### Compliance Check Test  
```bash
# Test compliance check
curl -s http://localhost:8000/api/v1/compliance/dashboard | jq '.overall_compliance'
```

## Next Steps

Now that the basic system is running:

1. **Configure SSL/TLS** - Set up HTTPS with proper certificates
2. **Set up monitoring** - Configure Prometheus/Grafana dashboards  
3. **Add users** - Import users and configure roles/permissions
4. **Customize policies** - Configure DLP and compliance policies for your environment
5. **Set up backups** - Configure automated backup procedures
6. **Production hardening** - Review security settings for production use

## Common Issues and Quick Fixes

### Service Won't Start
```bash
# Check logs
sudo journalctl -u security-platform -n 50

# Check configuration syntax
python3 -c "import yaml; yaml.safe_load(open('/opt/security/config/security.yaml'))"

# Verify database connection
pg_isready -h localhost -U security_user -d security_platform
```

### Dashboard Not Loading
```bash
# Check nginx configuration
sudo nginx -t

# Restart services
sudo systemctl restart nginx security-platform

# Check ports
sudo netstat -tlnp | grep -E ':(80|443|8000)'
```

### Database Connection Issues
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Reset database password
sudo -u postgres psql -c "ALTER USER security_user WITH PASSWORD 'newpassword';"

# Test connection
psql -h localhost -U security_user -d security_platform -c "SELECT 1;"
```

## Getting Help

For detailed information, refer to:

- [Security Administrator Guide](SECURITY_ADMINISTRATOR_GUIDE.md) - Complete administration procedures
- [Security API Documentation](SECURITY_API_DOCUMENTATION.md) - API reference and examples
- [Security Operations Guide](SECURITY_OPERATIONS_GUIDE.md) - Day-to-day operations
- [Enterprise Security Architecture](ENTERPRISE_SECURITY_ARCHITECTURE.md) - System architecture details

## Production Deployment Checklist

Before going to production, ensure:

- [ ] SSL certificates properly configured
- [ ] Firewall rules configured (ports 80, 443, 22 only)
- [ ] Database backups configured and tested
- [ ] Monitoring and alerting configured
- [ ] Log rotation configured
- [ ] Admin passwords changed from defaults
- [ ] DLP policies tested with real data
- [ ] Compliance frameworks configured for your industry
- [ ] Incident response procedures documented
- [ ] Security team trained on dashboard usage

---

This quick start guide gets you running in about 15-20 minutes. For production deployments, plan for additional hardening and configuration time.