# Enterprise Security Administrator Guide

## Overview

This guide provides comprehensive instructions for security administrators to install, configure, and manage the enterprise security and compliance framework. It includes setup procedures, configuration management, user administration, and system maintenance tasks.

## Table of Contents

1. [Installation and Setup](#installation-and-setup)
2. [Security Configuration Management](#security-configuration-management)
3. [User and Access Management](#user-and-access-management)
4. [Monitoring and Alerting Configuration](#monitoring-and-alerting-configuration)
5. [Policy Management](#policy-management)
6. [System Maintenance](#system-maintenance)
7. [Backup and Disaster Recovery](#backup-and-disaster-recovery)

---

## Installation and Setup

### Prerequisites

#### System Requirements
```yaml
minimum_requirements:
  cpu: "4 cores"
  memory: "16 GB RAM"
  storage: "500 GB SSD"
  network: "1 Gbps"

recommended_requirements:
  cpu: "8 cores"
  memory: "32 GB RAM"
  storage: "1 TB SSD"
  network: "10 Gbps"
```

#### Software Dependencies
```bash
# Required software packages
sudo apt update
sudo apt install -y \
  python3.11 \
  python3-pip \
  postgresql-14 \
  redis-server \
  nginx \
  docker.io \
  docker-compose \
  curl \
  jq
```

#### Environment Setup
```bash
# Create security system user
sudo useradd -m -s /bin/bash security-admin
sudo usermod -aG sudo security-admin

# Create directory structure
sudo mkdir -p /opt/security/{config,logs,data,backups}
sudo chown -R security-admin:security-admin /opt/security

# Set environment variables
cat >> ~/.bashrc << 'EOF'
export SECURITY_HOME="/opt/security"
export SECURITY_CONFIG="$SECURITY_HOME/config"
export SECURITY_LOGS="$SECURITY_HOME/logs"
EOF
```

### Installation Steps

#### 1. Database Setup

**PostgreSQL Configuration**:
```bash
# Install and configure PostgreSQL
sudo -u postgres createdb security_platform
sudo -u postgres createuser security_user --pwprompt

# Create security database schema
psql -U security_user -d security_platform << 'EOF'
-- Create security tables
CREATE SCHEMA security_core;
CREATE SCHEMA compliance;
CREATE SCHEMA audit_logs;

-- Set proper permissions
GRANT USAGE ON SCHEMA security_core TO security_user;
GRANT USAGE ON SCHEMA compliance TO security_user;
GRANT USAGE ON SCHEMA audit_logs TO security_user;
EOF
```

**Database Configuration File** (`/opt/security/config/database.conf`):
```ini
[database]
host = localhost
port = 5432
database = security_platform
username = security_user
password = your_secure_password
pool_size = 20
max_overflow = 10
pool_timeout = 30
pool_recycle = 3600

[redis]
host = localhost
port = 6379
database = 0
password = your_redis_password
```

#### 2. Security Framework Installation

**Python Environment Setup**:
```bash
# Create virtual environment
python3 -m venv /opt/security/venv
source /opt/security/venv/bin/activate

# Install security framework
pip install -r requirements.txt

# Install additional security packages
pip install \
  cryptography==41.0.7 \
  pyjwt[crypto]==2.8.0 \
  passlib[bcrypt]==1.7.4 \
  python-multipart==0.0.6 \
  phonenumbers==8.13.24
```

**Framework Configuration** (`/opt/security/config/security.yaml`):
```yaml
security:
  framework:
    name: "Enterprise Security Platform"
    version: "1.0.0"
    environment: "production"
    
  encryption:
    algorithm: "AES-256-GCM"
    key_rotation_days: 90
    
  authentication:
    jwt_secret_key: "your-jwt-secret-key-here"
    jwt_algorithm: "HS256"
    jwt_expiry_hours: 24
    mfa_enabled: true
    
  dlp:
    enabled: true
    scan_timeout: 30
    confidence_threshold: 0.8
    
  compliance:
    frameworks:
      - gdpr
      - hipaa
      - pci_dss
      - sox
    assessment_schedule: "daily"
    
  monitoring:
    metrics_enabled: true
    alerting_enabled: true
    dashboard_enabled: true
```

#### 3. Service Configuration

**Systemd Service File** (`/etc/systemd/system/security-platform.service`):
```ini
[Unit]
Description=Enterprise Security Platform
After=network.target postgresql.service redis.service
Requires=postgresql.service redis.service

[Service]
Type=forking
User=security-admin
Group=security-admin
WorkingDirectory=/opt/security
Environment=PATH=/opt/security/venv/bin
ExecStart=/opt/security/venv/bin/python -m src.api.main
ExecReload=/bin/kill -HUP $MAINPID
KillMode=mixed
TimeoutStopSec=5
PrivateTmp=true
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Enable and Start Service**:
```bash
# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable security-platform
sudo systemctl start security-platform

# Check service status
sudo systemctl status security-platform
```

#### 4. Web Server Configuration

**Nginx Configuration** (`/etc/nginx/sites-available/security-platform`):
```nginx
server {
    listen 443 ssl http2;
    server_name security.company.com;
    
    ssl_certificate /etc/ssl/certs/security.company.com.crt;
    ssl_certificate_key /etc/ssl/private/security.company.com.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;
    
    # Security headers
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload";
    add_header X-Content-Type-Options nosniff;
    add_header X-Frame-Options DENY;
    add_header X-XSS-Protection "1; mode=block";
    add_header Referrer-Policy "strict-origin-when-cross-origin";
    
    # API endpoints
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
    
    # WebSocket endpoints
    location /ws/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    # Static files
    location /static/ {
        alias /opt/security/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}

server {
    listen 80;
    server_name security.company.com;
    return 301 https://$server_name$request_uri;
}
```

---

## Security Configuration Management

### Core Security Settings

#### 1. Encryption Configuration

**Encryption Key Management**:
```python
# Generate encryption keys
from cryptography.fernet import Fernet
import base64
import os

def generate_encryption_keys():
    """Generate encryption keys for the security platform"""
    
    # Generate Fernet key for data encryption
    fernet_key = Fernet.generate_key()
    
    # Generate JWT secret key
    jwt_secret = base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')
    
    # Save keys securely
    with open('/opt/security/config/encryption.keys', 'w') as f:
        f.write(f"FERNET_KEY={fernet_key.decode()}\n")
        f.write(f"JWT_SECRET={jwt_secret}\n")
    
    # Set restrictive permissions
    os.chmod('/opt/security/config/encryption.keys', 0o600)
    
    return {
        'fernet_key': fernet_key.decode(),
        'jwt_secret': jwt_secret
    }

# Run key generation
keys = generate_encryption_keys()
print("Encryption keys generated successfully")
```

**Key Rotation Procedure**:
```bash
#!/bin/bash
# rotate_keys.sh

BACKUP_DIR="/opt/security/backups/keys/$(date +%Y%m%d_%H%M%S)"
mkdir -p $BACKUP_DIR

# Backup current keys
cp /opt/security/config/encryption.keys $BACKUP_DIR/

# Generate new keys
python3 << 'EOF'
import sys
sys.path.append('/opt/security')
from scripts.generate_keys import generate_encryption_keys
generate_encryption_keys()
EOF

# Restart services with new keys
sudo systemctl restart security-platform

echo "Key rotation completed successfully"
```

#### 2. Authentication Configuration

**Multi-Factor Authentication Setup**:
```yaml
# /opt/security/config/auth.yaml
authentication:
  methods:
    - password
    - mfa_totp
    - mfa_sms
  
  password_policy:
    min_length: 12
    require_uppercase: true
    require_lowercase: true
    require_numbers: true
    require_symbols: true
    password_history: 24
    password_expiry_days: 90
  
  mfa_settings:
    totp:
      issuer: "Security Platform"
      algorithm: "SHA256"
      digits: 6
      period: 30
    
    sms:
      provider: "twilio"
      template: "Your security code is: {code}"
      expiry_minutes: 5
  
  session_management:
    idle_timeout_minutes: 30
    absolute_timeout_hours: 8
    concurrent_sessions_limit: 3
```

**OAuth2 Provider Configuration**:
```python
# /opt/security/config/oauth2_config.py
OAUTH2_PROVIDERS = {
    'google': {
        'client_id': 'your-google-client-id',
        'client_secret': 'your-google-client-secret',
        'server_metadata_url': 'https://accounts.google.com/.well-known/openid_configuration',
        'client_kwargs': {
            'scope': 'openid email profile'
        }
    },
    'microsoft': {
        'client_id': 'your-microsoft-client-id',
        'client_secret': 'your-microsoft-client-secret',
        'server_metadata_url': 'https://login.microsoftonline.com/common/v2.0/.well-known/openid_configuration',
        'client_kwargs': {
            'scope': 'openid email profile'
        }
    }
}
```

#### 3. Data Loss Prevention Configuration

**DLP Policy Configuration** (`/opt/security/config/dlp_policies.yaml`):
```yaml
dlp_policies:
  - policy_id: "pii_protection"
    name: "Personal Information Protection"
    enabled: true
    data_types:
      - ssn
      - credit_card
      - email
      - phone
    classification_levels:
      - confidential
      - restricted
    actions:
      - redact
      - log
      - alert
    applies_to:
      locations:
        - api
        - database
        - exports
      users:
        - all
    exceptions:
      users:
        - data_analyst
        - compliance_officer
      contexts:
        - internal_testing
        - audit_review
  
  - policy_id: "hipaa_phi_protection"
    name: "HIPAA PHI Protection"
    enabled: true
    data_types:
      - medical_record
      - health_id
    classification_levels:
      - restricted
    actions:
      - encrypt
      - log
      - alert
    compliance_frameworks:
      - hipaa
```

**Custom DLP Pattern Configuration**:
```python
# /opt/security/config/custom_dlp_patterns.py
CUSTOM_DLP_PATTERNS = [
    {
        'name': 'Company Employee ID',
        'pattern': r'EMP-\d{6}',
        'data_type': 'employee_id',
        'confidence_threshold': 0.9,
        'context_keywords': ['employee', 'staff', 'personnel'],
        'classification': 'internal'
    },
    {
        'name': 'Project Code',
        'pattern': r'PROJ-[A-Z]{3}-\d{4}',
        'data_type': 'project_code',
        'confidence_threshold': 0.85,
        'context_keywords': ['project', 'initiative', 'code'],
        'classification': 'confidential'
    },
    {
        'name': 'Customer Account Number',
        'pattern': r'ACCT-\d{10}',
        'data_type': 'account_number',
        'confidence_threshold': 0.95,
        'context_keywords': ['account', 'customer'],
        'classification': 'restricted'
    }
]
```

### Network Security Configuration

#### 1. Firewall Rules

**UFW Configuration**:
```bash
#!/bin/bash
# configure_firewall.sh

# Reset firewall rules
sudo ufw --force reset

# Default policies
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Allow SSH (restrict to management network)
sudo ufw allow from 192.168.100.0/24 to any port 22

# Allow HTTP/HTTPS
sudo ufw allow 80
sudo ufw allow 443

# Allow PostgreSQL (internal only)
sudo ufw allow from 10.0.0.0/8 to any port 5432

# Allow Redis (internal only)
sudo ufw allow from 10.0.0.0/8 to any port 6379

# Allow security platform API
sudo ufw allow from 10.0.0.0/8 to any port 8000

# Enable firewall
sudo ufw --force enable

# Show status
sudo ufw status numbered
```

#### 2. SSL/TLS Configuration

**Generate SSL Certificates**:
```bash
#!/bin/bash
# generate_ssl_certs.sh

DOMAIN="security.company.com"
CERT_DIR="/etc/ssl/security"

# Create certificate directory
sudo mkdir -p $CERT_DIR

# Generate private key
sudo openssl genrsa -out $CERT_DIR/$DOMAIN.key 4096

# Generate certificate signing request
sudo openssl req -new -key $CERT_DIR/$DOMAIN.key -out $CERT_DIR/$DOMAIN.csr \
  -subj "/C=US/ST=State/L=City/O=Company/OU=IT/CN=$DOMAIN"

# Generate self-signed certificate (for testing)
sudo openssl x509 -req -days 365 -in $CERT_DIR/$DOMAIN.csr \
  -signkey $CERT_DIR/$DOMAIN.key -out $CERT_DIR/$DOMAIN.crt

# Set proper permissions
sudo chmod 600 $CERT_DIR/$DOMAIN.key
sudo chmod 644 $CERT_DIR/$DOMAIN.crt

echo "SSL certificates generated for $DOMAIN"
```

---

## User and Access Management

### User Administration

#### 1. Create Administrative Users

**Security Administrator Creation**:
```python
#!/usr/bin/env python3
# create_admin_user.py

import sys
import asyncio
sys.path.append('/opt/security')

from src.api.v1.services.enhanced_auth_service import get_auth_service
from src.core.security.enhanced_access_control import get_access_control_manager

async def create_security_admin():
    """Create security administrator user"""
    
    auth_service = get_auth_service()
    access_manager = get_access_control_manager()
    
    # Create admin user
    admin_user = await auth_service.create_user(
        username="security_admin",
        email="security@company.com",
        password="TempPassword123!",
        require_password_change=True
    )
    
    # Assign admin permissions
    admin_permissions = [
        "perm_admin_system",
        "perm_audit_logs", 
        "perm_compliance_report",
        "perm_admin_users",
        "perm_security_scan",
        "perm_dlp_management"
    ]
    
    for permission in admin_permissions:
        await access_manager.assign_permission(admin_user.id, permission)
    
    print(f"Security administrator created: {admin_user.username}")
    print("Default password: TempPassword123!")
    print("User must change password on first login")

if __name__ == "__main__":
    asyncio.run(create_security_admin())
```

#### 2. Role-Based Access Control Setup

**Role Definitions** (`/opt/security/config/rbac_roles.yaml`):
```yaml
roles:
  security_admin:
    description: "Full system administration access"
    permissions:
      - perm_admin_system
      - perm_audit_logs
      - perm_compliance_report
      - perm_admin_users
      - perm_security_scan
      - perm_dlp_management
      - perm_policy_management
  
  security_analyst:
    description: "Security monitoring and analysis"
    permissions:
      - perm_audit_logs
      - perm_security_scan
      - perm_incident_response
      - perm_threat_analysis
  
  compliance_officer:
    description: "Compliance monitoring and reporting"
    permissions:
      - perm_compliance_report
      - perm_audit_logs
      - perm_policy_view
      - perm_violation_management
  
  data_protection_officer:
    description: "Data privacy and protection oversight"
    permissions:
      - perm_dlp_management
      - perm_compliance_report
      - perm_data_governance
      - perm_privacy_requests
```

**Bulk User Import Script**:
```python
#!/usr/bin/env python3
# import_users.py

import csv
import asyncio
import sys
sys.path.append('/opt/security')

from src.api.v1.services.enhanced_auth_service import get_auth_service
from src.core.security.enhanced_access_control import get_access_control_manager

async def import_users_from_csv(csv_file):
    """Import users from CSV file"""
    
    auth_service = get_auth_service()
    access_manager = get_access_control_manager()
    
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            try:
                # Create user
                user = await auth_service.create_user(
                    username=row['username'],
                    email=row['email'],
                    first_name=row['first_name'],
                    last_name=row['last_name'],
                    password=row.get('password', 'TempPassword123!'),
                    require_password_change=True
                )
                
                # Assign role
                if row.get('role'):
                    await access_manager.assign_role(user.id, row['role'])
                
                print(f"Created user: {user.username}")
                
            except Exception as e:
                print(f"Error creating user {row['username']}: {e}")

# Usage
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python import_users.py users.csv")
        sys.exit(1)
    
    asyncio.run(import_users_from_csv(sys.argv[1]))
```

#### 3. Permission Management

**Dynamic Permission Assignment**:
```python
#!/usr/bin/env python3
# manage_permissions.py

import asyncio
import argparse
import sys
sys.path.append('/opt/security')

from src.core.security.enhanced_access_control import get_access_control_manager

async def manage_user_permissions(action, username, permission=None, role=None):
    """Manage user permissions and roles"""
    
    access_manager = get_access_control_manager()
    
    # Find user by username
    user = await access_manager.get_user_by_username(username)
    if not user:
        print(f"User not found: {username}")
        return
    
    if action == "assign_permission" and permission:
        await access_manager.assign_permission(user.id, permission)
        print(f"Assigned permission '{permission}' to user '{username}'")
    
    elif action == "revoke_permission" and permission:
        await access_manager.revoke_permission(user.id, permission)
        print(f"Revoked permission '{permission}' from user '{username}'")
    
    elif action == "assign_role" and role:
        await access_manager.assign_role(user.id, role)
        print(f"Assigned role '{role}' to user '{username}'")
    
    elif action == "revoke_role" and role:
        await access_manager.revoke_role(user.id, role)
        print(f"Revoked role '{role}' from user '{username}'")
    
    elif action == "list_permissions":
        permissions = await access_manager.get_user_permissions(user.id)
        print(f"Permissions for user '{username}':")
        for perm in permissions:
            print(f"  - {perm}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manage user permissions')
    parser.add_argument('action', choices=['assign_permission', 'revoke_permission', 
                                         'assign_role', 'revoke_role', 'list_permissions'])
    parser.add_argument('username', help='Username')
    parser.add_argument('--permission', help='Permission name')
    parser.add_argument('--role', help='Role name')
    
    args = parser.parse_args()
    
    asyncio.run(manage_user_permissions(
        args.action, args.username, args.permission, args.role
    ))
```

### Access Control Policies

#### 1. Attribute-Based Access Control (ABAC)

**ABAC Policy Configuration** (`/opt/security/config/abac_policies.yaml`):
```yaml
abac_policies:
  - policy_id: "data_access_by_classification"
    name: "Data Access Based on Classification"
    description: "Control data access based on data classification and user clearance"
    condition: |
      (user.clearance_level >= resource.classification_level) AND
      (user.department == resource.owning_department OR user.role == "security_admin")
    
  - policy_id: "time_based_access"
    name: "Time-based Access Control"
    description: "Restrict access based on time and location"
    condition: |
      (environment.time >= "08:00" AND environment.time <= "18:00") OR
      (user.role IN ["security_admin", "on_call_engineer"])
    
  - policy_id: "sensitive_operation_mfa"
    name: "MFA Required for Sensitive Operations"
    description: "Require MFA for sensitive operations"
    condition: |
      action.type IN ["delete", "export", "modify_permissions"] 
      IMPLIES user.mfa_verified == true
```

#### 2. Privileged Access Management

**Privileged Session Configuration**:
```python
# /opt/security/config/pam_config.py
PRIVILEGED_ACCESS_CONFIG = {
    'session_timeout': 3600,  # 1 hour
    'approval_required': True,
    'approval_timeout': 1800,  # 30 minutes
    'session_recording': True,
    'break_glass_enabled': True,
    'emergency_access_timeout': 300,  # 5 minutes
    
    'privileged_roles': [
        'security_admin',
        'system_admin',
        'database_admin'
    ],
    
    'privileged_operations': [
        'user_management',
        'policy_modification',
        'system_configuration',
        'audit_log_access'
    ]
}
```

---

## Monitoring and Alerting Configuration

### Security Metrics Configuration

#### 1. Prometheus Integration

**Prometheus Configuration** (`/opt/security/config/prometheus.yml`):
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "/opt/security/config/security_rules.yml"

scrape_configs:
  - job_name: 'security-platform'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s
    
  - job_name: 'security-dlp'
    static_configs:
      - targets: ['localhost:8001']
    metrics_path: '/dlp/metrics'
    scrape_interval: 60s
    
  - job_name: 'security-compliance'
    static_configs:
      - targets: ['localhost:8002']
    metrics_path: '/compliance/metrics'
    scrape_interval: 300s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - localhost:9093
```

**Security Alerting Rules** (`/opt/security/config/security_rules.yml`):
```yaml
groups:
  - name: security_alerts
    rules:
      - alert: HighSecurityThreatRate
        expr: rate(security_threats_total[5m]) > 10
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "High security threat detection rate"
          description: "Security threat detection rate is {{ $value }} per second"
      
      - alert: DLPViolationSpike
        expr: rate(dlp_violations_total[15m]) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DLP violation spike detected"
          description: "DLP violations increased to {{ $value }} per second"
      
      - alert: ComplianceScoreDecline
        expr: compliance_score < 0.8
        for: 10m
        labels:
          severity: high
        annotations:
          summary: "Compliance score below threshold"
          description: "Compliance score dropped to {{ $value }}"
```

#### 2. Grafana Dashboard Setup

**Dashboard Configuration** (`/opt/security/config/grafana_dashboard.json`):
```json
{
  "dashboard": {
    "title": "Enterprise Security Dashboard",
    "panels": [
      {
        "title": "Security Threat Overview",
        "type": "stat",
        "targets": [
          {
            "expr": "security_threats_active",
            "legendFormat": "Active Threats"
          }
        ]
      },
      {
        "title": "DLP Incidents Timeline",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(dlp_incidents_total[1h])",
            "legendFormat": "DLP Incidents per Hour"
          }
        ]
      },
      {
        "title": "Compliance Status",
        "type": "piechart",
        "targets": [
          {
            "expr": "compliance_controls_compliant",
            "legendFormat": "Compliant"
          },
          {
            "expr": "compliance_controls_non_compliant", 
            "legendFormat": "Non-Compliant"
          }
        ]
      }
    ]
  }
}
```

#### 3. Alert Notification Configuration

**Alertmanager Configuration** (`/opt/security/config/alertmanager.yml`):
```yaml
global:
  smtp_smarthost: 'smtp.company.com:587'
  smtp_from: 'security-alerts@company.com'
  smtp_auth_username: 'security-alerts@company.com'
  smtp_auth_password: 'smtp_password'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'security-team'
  routes:
    - match:
        severity: critical
      receiver: 'security-oncall'
    - match:
        severity: high
      receiver: 'security-team'

receivers:
  - name: 'security-team'
    email_configs:
      - to: 'security-team@company.com'
        subject: 'Security Alert: {{ .GroupLabels.alertname }}'
        body: |
          {{ range .Alerts }}
          Alert: {{ .Annotations.summary }}
          Description: {{ .Annotations.description }}
          {{ end }}
  
  - name: 'security-oncall'
    email_configs:
      - to: 'security-oncall@company.com'
        subject: 'CRITICAL Security Alert: {{ .GroupLabels.alertname }}'
    pagerduty_configs:
      - service_key: 'your-pagerduty-service-key'
        description: 'Critical security alert: {{ .GroupLabels.alertname }}'
```

---

## Policy Management

### Security Policy Configuration

#### 1. DLP Policy Management

**DLP Policy Administration Script**:
```python
#!/usr/bin/env python3
# manage_dlp_policies.py

import asyncio
import json
import sys
sys.path.append('/opt/security')

from src.core.security.enterprise_dlp import EnterpriseDLPManager

async def manage_dlp_policies(action, policy_file=None, policy_id=None):
    """Manage DLP policies"""
    
    dlp_manager = EnterpriseDLPManager()
    
    if action == "list":
        policies = dlp_manager.policy_engine.policies
        print("Current DLP Policies:")
        for pid, policy in policies.items():
            status = "Enabled" if policy.enabled else "Disabled"
            print(f"  {pid}: {policy.name} ({status})")
    
    elif action == "add" and policy_file:
        with open(policy_file, 'r') as f:
            policy_data = json.load(f)
        
        # Create and add policy
        policy = create_dlp_policy_from_dict(policy_data)
        dlp_manager.policy_engine.add_policy(policy)
        print(f"Added policy: {policy.policy_id}")
    
    elif action == "remove" and policy_id:
        dlp_manager.policy_engine.remove_policy(policy_id)
        print(f"Removed policy: {policy_id}")
    
    elif action == "enable" and policy_id:
        if policy_id in dlp_manager.policy_engine.policies:
            dlp_manager.policy_engine.policies[policy_id].enabled = True
            print(f"Enabled policy: {policy_id}")
    
    elif action == "disable" and policy_id:
        if policy_id in dlp_manager.policy_engine.policies:
            dlp_manager.policy_engine.policies[policy_id].enabled = False
            print(f"Disabled policy: {policy_id}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage DLP policies')
    parser.add_argument('action', choices=['list', 'add', 'remove', 'enable', 'disable'])
    parser.add_argument('--policy-file', help='Policy configuration file')
    parser.add_argument('--policy-id', help='Policy ID')
    
    args = parser.parse_args()
    
    asyncio.run(manage_dlp_policies(args.action, args.policy_file, args.policy_id))
```

#### 2. Compliance Policy Updates

**Compliance Policy Update Script**:
```bash
#!/bin/bash
# update_compliance_policies.sh

POLICY_DIR="/opt/security/config/compliance"
BACKUP_DIR="/opt/security/backups/compliance/$(date +%Y%m%d_%H%M%S)"

# Create backup
mkdir -p $BACKUP_DIR
cp -r $POLICY_DIR/* $BACKUP_DIR/

echo "Backing up current compliance policies to $BACKUP_DIR"

# Update GDPR policies
echo "Updating GDPR compliance policies..."
curl -X POST "http://localhost:8000/api/v1/compliance/policies/update" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d @$POLICY_DIR/gdpr_policies.json

# Update HIPAA policies  
echo "Updating HIPAA compliance policies..."
curl -X POST "http://localhost:8000/api/v1/compliance/policies/update" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d @$POLICY_DIR/hipaa_policies.json

# Validate policy updates
echo "Validating policy updates..."
curl -X GET "http://localhost:8000/api/v1/compliance/policies/validate" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

echo "Compliance policy update completed"
```

### Access Control Policy Management

**Access Policy Update Script**:
```python
#!/usr/bin/env python3
# update_access_policies.py

import asyncio
import yaml
import sys
sys.path.append('/opt/security')

from src.core.security.enhanced_access_control import get_access_control_manager

async def update_access_policies(policy_file):
    """Update access control policies from YAML file"""
    
    access_manager = get_access_control_manager()
    
    # Load policies from file
    with open(policy_file, 'r') as f:
        policies_data = yaml.safe_load(f)
    
    # Update RBAC policies
    if 'rbac_policies' in policies_data:
        for policy_data in policies_data['rbac_policies']:
            await access_manager.update_rbac_policy(policy_data)
            print(f"Updated RBAC policy: {policy_data['policy_id']}")
    
    # Update ABAC policies
    if 'abac_policies' in policies_data:
        for policy_data in policies_data['abac_policies']:
            await access_manager.update_abac_policy(policy_data)
            print(f"Updated ABAC policy: {policy_data['policy_id']}")
    
    print("Access control policies updated successfully")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python update_access_policies.py policy_file.yaml")
        sys.exit(1)
    
    asyncio.run(update_access_policies(sys.argv[1]))
```

---

## System Maintenance

### Regular Maintenance Tasks

#### 1. Database Maintenance

**Database Cleanup Script**:
```bash
#!/bin/bash
# database_maintenance.sh

DB_NAME="security_platform"
DB_USER="security_user"
RETENTION_DAYS=90

echo "Starting database maintenance..."

# Clean old audit logs
psql -U $DB_USER -d $DB_NAME << EOF
DELETE FROM audit_logs.security_events 
WHERE timestamp < NOW() - INTERVAL '$RETENTION_DAYS days';

DELETE FROM audit_logs.access_logs
WHERE timestamp < NOW() - INTERVAL '$RETENTION_DAYS days';

DELETE FROM audit_logs.dlp_incidents
WHERE timestamp < NOW() - INTERVAL '$RETENTION_DAYS days' AND resolved = true;
EOF

# Vacuum and analyze tables
psql -U $DB_USER -d $DB_NAME << EOF
VACUUM ANALYZE audit_logs.security_events;
VACUUM ANALYZE audit_logs.access_logs;
VACUUM ANALYZE audit_logs.dlp_incidents;
VACUUM ANALYZE security_core.threat_intelligence;
VACUUM ANALYZE compliance.assessments;
EOF

# Update table statistics
psql -U $DB_USER -d $DB_NAME << EOF
ANALYZE;
EOF

echo "Database maintenance completed"
```

#### 2. Log Rotation and Cleanup

**Log Rotation Configuration** (`/etc/logrotate.d/security-platform`):
```
/opt/security/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    copytruncate
    create 0644 security-admin security-admin
    
    postrotate
        systemctl reload security-platform
    endscript
}
```

#### 3. System Health Checks

**Health Check Script**:
```bash
#!/bin/bash
# system_health_check.sh

LOG_FILE="/opt/security/logs/health_check.log"
ALERT_THRESHOLD=85

echo "$(date): Starting system health check" >> $LOG_FILE

# Check disk usage
DISK_USAGE=$(df /opt/security | awk 'NR==2 {print $5}' | sed 's/%//')
if [ $DISK_USAGE -gt $ALERT_THRESHOLD ]; then
    echo "WARNING: Disk usage is ${DISK_USAGE}%" >> $LOG_FILE
    # Send alert
    curl -X POST "http://localhost:8000/api/v1/alerts/send" \
      -H "Content-Type: application/json" \
      -d "{\"type\": \"disk_usage\", \"severity\": \"warning\", \"message\": \"Disk usage is ${DISK_USAGE}%\"}"
fi

# Check service status
if ! systemctl is-active --quiet security-platform; then
    echo "ERROR: Security platform service is not running" >> $LOG_FILE
    # Send critical alert
    curl -X POST "http://localhost:8000/api/v1/alerts/send" \
      -H "Content-Type: application/json" \
      -d '{"type": "service_down", "severity": "critical", "message": "Security platform service is down"}'
fi

# Check database connectivity
if ! pg_isready -h localhost -p 5432 -U security_user > /dev/null 2>&1; then
    echo "ERROR: Database is not accessible" >> $LOG_FILE
fi

# Check API health
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health)
if [ $HTTP_STATUS -ne 200 ]; then
    echo "ERROR: API health check failed with status $HTTP_STATUS" >> $LOG_FILE
fi

echo "$(date): Health check completed" >> $LOG_FILE
```

### Performance Optimization

#### 1. Database Performance Tuning

**PostgreSQL Optimization** (`/etc/postgresql/14/main/postgresql.conf`):
```ini
# Memory settings
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB

# Connection settings
max_connections = 100
shared_preload_libraries = 'pg_stat_statements'

# Logging settings
log_min_duration_statement = 1000
log_statement = 'all'
log_duration = on

# Performance settings
checkpoint_completion_target = 0.7
wal_buffers = 16MB
default_statistics_target = 100
```

#### 2. Application Performance Tuning

**Application Configuration** (`/opt/security/config/performance.yaml`):
```yaml
performance:
  api:
    worker_processes: 4
    max_requests: 1000
    max_requests_jitter: 50
    timeout: 30
    
  database:
    pool_size: 20
    max_overflow: 10
    pool_timeout: 30
    pool_recycle: 3600
    
  cache:
    redis_max_connections: 50
    cache_default_timeout: 3600
    cache_key_prefix: "security:"
    
  dlp:
    max_concurrent_scans: 10
    scan_timeout: 30
    pattern_cache_size: 1000
    
  compliance:
    assessment_batch_size: 100
    assessment_timeout: 300
    result_cache_timeout: 1800
```

---

## Backup and Disaster Recovery

### Backup Configuration

#### 1. Database Backup

**Automated Database Backup Script**:
```bash
#!/bin/bash
# backup_database.sh

BACKUP_DIR="/opt/security/backups/database"
RETENTION_DAYS=30
DB_NAME="security_platform"
DB_USER="security_user"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR

# Full database backup
pg_dump -U $DB_USER -h localhost -d $DB_NAME \
  --verbose --format=custom \
  --file="$BACKUP_DIR/security_platform_$TIMESTAMP.backup"

# Compress backup
gzip "$BACKUP_DIR/security_platform_$TIMESTAMP.backup"

# Schema-only backup
pg_dump -U $DB_USER -h localhost -d $DB_NAME \
  --schema-only --verbose \
  --file="$BACKUP_DIR/security_platform_schema_$TIMESTAMP.sql"

# Remove old backups
find $BACKUP_DIR -name "*.backup.gz" -mtime +$RETENTION_DAYS -delete
find $BACKUP_DIR -name "*_schema_*.sql" -mtime +$RETENTION_DAYS -delete

echo "Database backup completed: security_platform_$TIMESTAMP.backup.gz"
```

#### 2. Configuration Backup

**Configuration Backup Script**:
```bash
#!/bin/bash
# backup_configuration.sh

BACKUP_DIR="/opt/security/backups/config"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CONFIG_DIRS=(
    "/opt/security/config"
    "/etc/nginx/sites-available/security-platform"
    "/etc/systemd/system/security-platform.service"
    "/etc/ssl/security"
)

# Create backup directory
mkdir -p "$BACKUP_DIR/$TIMESTAMP"

# Backup configuration files
for dir in "${CONFIG_DIRS[@]}"; do
    if [ -e "$dir" ]; then
        cp -r "$dir" "$BACKUP_DIR/$TIMESTAMP/"
    fi
done

# Create archive
cd "$BACKUP_DIR"
tar -czf "config_backup_$TIMESTAMP.tar.gz" "$TIMESTAMP"
rm -rf "$TIMESTAMP"

# Remove old backups (keep 30 days)
find $BACKUP_DIR -name "config_backup_*.tar.gz" -mtime +30 -delete

echo "Configuration backup completed: config_backup_$TIMESTAMP.tar.gz"
```

### Disaster Recovery

#### 1. Recovery Procedures

**Database Recovery Script**:
```bash
#!/bin/bash
# recover_database.sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 <backup_file.backup.gz>"
    exit 1
fi

BACKUP_FILE="$1"
DB_NAME="security_platform"
DB_USER="security_user"
RECOVERY_DB="${DB_NAME}_recovery"

# Stop services
sudo systemctl stop security-platform

# Create recovery database
sudo -u postgres createdb $RECOVERY_DB

# Restore from backup
gunzip -c "$BACKUP_FILE" | pg_restore -U $DB_USER -d $RECOVERY_DB --verbose

# Verify recovery
if pg_isready -h localhost -p 5432 -U $DB_USER -d $RECOVERY_DB; then
    echo "Recovery database created successfully"
    echo "To use recovery database, update configuration and restart services"
else
    echo "Recovery failed"
    exit 1
fi
```

#### 2. Service Recovery

**Service Recovery Checklist**:
```bash
#!/bin/bash
# service_recovery.sh

echo "Starting service recovery process..."

# Check system requirements
echo "1. Checking system requirements..."
python3 --version
psql --version
redis-server --version

# Restore configuration
echo "2. Restoring configuration..."
if [ -f "/opt/security/backups/config/latest_config.tar.gz" ]; then
    cd /opt/security
    tar -xzf "/opt/security/backups/config/latest_config.tar.gz"
    echo "Configuration restored"
fi

# Start services
echo "3. Starting services..."
sudo systemctl start postgresql
sudo systemctl start redis-server
sudo systemctl start nginx
sudo systemctl start security-platform

# Verify services
echo "4. Verifying services..."
sleep 10

if systemctl is-active --quiet security-platform; then
    echo "✓ Security platform service is running"
else
    echo "✗ Security platform service failed to start"
fi

if curl -s http://localhost:8000/health > /dev/null; then
    echo "✓ API health check passed"
else
    echo "✗ API health check failed"
fi

echo "Service recovery process completed"
```

### Monitoring Recovery Status

**Recovery Monitoring Script**:
```python
#!/usr/bin/env python3
# monitor_recovery.py

import requests
import time
import sys

def check_service_health():
    """Check the health of all security services"""
    
    checks = {
        'api': 'http://localhost:8000/health',
        'database': 'http://localhost:8000/api/v1/health/database',
        'redis': 'http://localhost:8000/api/v1/health/redis',
        'dlp': 'http://localhost:8000/api/v1/health/dlp',
        'compliance': 'http://localhost:8000/api/v1/health/compliance'
    }
    
    results = {}
    
    for service, endpoint in checks.items():
        try:
            response = requests.get(endpoint, timeout=10)
            results[service] = {
                'status': 'healthy' if response.status_code == 200 else 'unhealthy',
                'response_time': response.elapsed.total_seconds()
            }
        except Exception as e:
            results[service] = {
                'status': 'error',
                'error': str(e)
            }
    
    return results

def main():
    print("Starting recovery monitoring...")
    
    max_attempts = 60  # 10 minutes with 10-second intervals
    attempt = 0
    
    while attempt < max_attempts:
        print(f"\nAttempt {attempt + 1}/{max_attempts}")
        
        health_results = check_service_health()
        all_healthy = True
        
        for service, result in health_results.items():
            status = result['status']
            if status == 'healthy':
                print(f"✓ {service}: {status}")
            else:
                print(f"✗ {service}: {status}")
                all_healthy = False
        
        if all_healthy:
            print("\n✓ All services are healthy - recovery successful!")
            sys.exit(0)
        
        attempt += 1
        if attempt < max_attempts:
            print("Waiting 10 seconds before next check...")
            time.sleep(10)
    
    print(f"\n✗ Recovery monitoring timed out after {max_attempts} attempts")
    sys.exit(1)

if __name__ == "__main__":
    main()
```

---

This administrator guide provides comprehensive instructions for installing, configuring, and maintaining the enterprise security and compliance framework. Regular review and updates of these procedures ensure optimal system performance and security posture.