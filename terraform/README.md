# 🏗️ PwC Retail Data Platform - Infrastructure as Code

Enterprise-grade Terraform infrastructure for multi-cloud deployment of the PwC Retail Data Platform across AWS, Azure, and Google Cloud Platform.

## 🎯 Overview

This Terraform configuration provides:

- **🌥️ Multi-Cloud Support**: Deploy to AWS EKS, Azure AKS, or Google GKE
- **🔒 Security Hardened**: KMS encryption, VPC isolation, IAM roles, audit logging
- **📈 Auto-scaling**: Horizontal Pod Autoscaler with CPU/memory metrics
- **💾 High Availability**: Multi-AZ deployment, automated backups, disaster recovery
- **📊 Monitoring**: Integrated observability with cloud-native tools
- **🔄 Environment Management**: Dev/staging/production configurations

## 📁 Project Structure

```
terraform/
├── 📂 environments/                    # Environment-specific configurations
│   ├── dev/terraform.tfvars           # Development environment
│   ├── staging/terraform.tfvars       # Staging environment
│   └── prod/terraform.tfvars          # Production environment
├── 📂 modules/                        # Reusable Terraform modules
│   ├── 📂 aws/                        # AWS EKS, RDS, S3, ECR
│   ├── 📂 azure/                      # Azure AKS, PostgreSQL, Storage
│   ├── 📂 gcp/                        # Google GKE, Cloud SQL, Storage
│   ├── 📂 kubernetes/                 # K8s applications and services
│   ├── 📂 monitoring/                 # Observability stack
│   └── 📂 security/                   # Security and compliance
├── 📂 scripts/                        # Deployment automation
│   ├── deploy.sh                      # Linux/macOS deployment script
│   ├── deploy.ps1                     # Windows PowerShell script
│   └── setup-backend.sh               # Terraform backend setup
├── main.tf                           # Main Terraform configuration
├── variables.tf                      # Input variables
├── outputs.tf                        # Output values
├── Makefile                         # Convenient command targets
└── README.md                        # This file
```

## 🚀 Quick Start

### Prerequisites

- **Terraform** 1.5.0+ installed
- **Cloud CLI** configured:
  - AWS CLI for AWS deployments
  - Azure CLI for Azure deployments  
  - Google Cloud SDK for GCP deployments
- **kubectl** for Kubernetes management

### 1️⃣ Environment Setup

```bash
# Clone repository and navigate to terraform directory
git clone <repository-url>
cd terraform

# Choose your deployment method:
```

### 2️⃣ Quick Deployment (Recommended)

**Using Makefile (Easiest):**
```bash
# Development environment
make plan ENV=dev
make apply ENV=dev

# Staging environment
make plan ENV=staging CLOUD_PROVIDER=aws
make apply ENV=staging CLOUD_PROVIDER=aws

# Production environment  
make plan ENV=prod CLOUD_PROVIDER=aws
make apply ENV=prod CLOUD_PROVIDER=aws
```

**Using Deployment Scripts:**
```bash
# Linux/macOS
./scripts/deploy.sh -e dev -c aws
./scripts/deploy.sh -e prod -c aws -a  # Auto-approve

# Windows PowerShell
.\scripts\deploy.ps1 -Environment dev -CloudProvider aws
.\scripts\deploy.ps1 -Environment prod -CloudProvider aws -AutoApprove
```

### 3️⃣ Manual Deployment

```bash
# Initialize Terraform
terraform init

# Plan deployment
terraform plan -var-file=environments/prod/terraform.tfvars

# Apply changes
terraform apply -var-file=environments/prod/terraform.tfvars

# Show outputs
terraform output
```

## 🌥️ Cloud Provider Configurations

### AWS Deployment

**Infrastructure Includes:**
- **EKS Cluster**: Kubernetes 1.28 with managed node groups
- **RDS PostgreSQL**: Multi-AZ with automated backups
- **S3 Data Lake**: Versioning, encryption, and lifecycle policies
- **ECR Repositories**: Container image storage with scanning
- **VPC**: 3-AZ setup with public/private subnets
- **Load Balancing**: Application Load Balancer with SSL
- **Security**: KMS keys, IAM roles, security groups
- **Monitoring**: CloudWatch dashboards and alarms

```bash
# Setup backend (one-time)
./scripts/setup-backend.sh -e prod -r us-west-2

# Deploy
make apply ENV=prod CLOUD_PROVIDER=aws

# Configure kubectl
aws eks update-kubeconfig --region us-west-2 --name pwc-retail-data-platform-prod
```

### Azure Deployment

**Infrastructure Includes:**
- **AKS Cluster**: System/user node pools with auto-scaling
- **Azure Database**: PostgreSQL Flexible Server with HA
- **Azure Storage**: Blob storage with hierarchical namespace
- **Container Registry**: Geo-replication enabled
- **Virtual Network**: Subnet delegation and private endpoints
- **Application Gateway**: WAF protection and SSL offloading
- **Key Vault**: Secrets and certificate management
- **Monitoring**: Azure Monitor and Log Analytics

```bash
# Deploy to Azure
make apply ENV=prod CLOUD_PROVIDER=azure

# Configure kubectl
az aks get-credentials --resource-group pwc-retail-data-platform-prod \
  --name pwc-retail-data-platform-prod
```

### Google Cloud Deployment

**Infrastructure Includes:**
- **GKE Cluster**: Autopilot with Workload Identity
- **Cloud SQL**: PostgreSQL with high availability
- **Cloud Storage**: Multi-regional buckets
- **Artifact Registry**: Container image management
- **VPC**: Private Google access and custom routes
- **Load Balancing**: Global load balancer with SSL
- **KMS**: Customer-managed encryption keys
- **Monitoring**: Cloud Operations Suite

```bash
# Deploy to GCP
make apply ENV=prod CLOUD_PROVIDER=gcp

# Configure kubectl
gcloud container clusters get-credentials pwc-retail-data-platform-prod \
  --region us-west1 --project your-project-id
```

## 📋 Environment Configurations

### Development (`environments/dev/`)
```hcl
# Optimized for development and testing
cluster_version     = "1.28"
node_instance_types = ["t3.medium"]
node_desired_size   = 2
node_max_size      = 4
db_instance_class  = "db.t3.micro"
enable_monitoring  = true
enable_audit_logs  = false
```

### Staging (`environments/staging/`)
```hcl
# Pre-production validation environment
cluster_version     = "1.28" 
node_instance_types = ["t3.large"]
node_desired_size   = 3
node_max_size      = 6
db_instance_class  = "db.t3.small"
enable_monitoring  = true
enable_audit_logs  = true
```

### Production (`environments/prod/`)
```hcl
# Enterprise production configuration
cluster_version       = "1.28"
node_instance_types   = ["m5.large", "m5.xlarge"]
node_desired_size     = 5
node_max_size        = 20
db_instance_class    = "db.r5.large"
db_allocated_storage = 200
enable_monitoring    = true
enable_audit_logs    = true
secrets_backend      = "aws-secrets-manager"
```

## 🛠️ Management Commands

### Environment Operations
```bash
# Plan and apply changes
make plan ENV=<env> [CLOUD_PROVIDER=<provider>]
make apply ENV=<env> [CLOUD_PROVIDER=<provider>]
make apply-auto ENV=<env>           # Auto-approve

# Destroy infrastructure
make destroy ENV=<env>

# Show outputs and state
make output ENV=<env>
make state-list ENV=<env>
make refresh ENV=<env>
```

### Development Tools
```bash
# Code quality
make format                         # Format Terraform files
make validate                       # Validate configuration  
make lint                          # Run format + validate
make security-scan                 # Security analysis

# Documentation and visualization
make docs                          # Generate documentation
make graph ENV=<env>               # Dependency graph
make cost-estimate ENV=<env>       # Cost estimation

# Cleanup
make clean                         # Remove temporary files
```

### Backend Management
```bash
# Setup S3 backend for state management
make setup-backend ENV=<env>

# Or manually
./scripts/setup-backend.sh -e prod -r us-west-2
```

## 🔒 Security Features

### Encryption
- **At Rest**: KMS encryption for all storage (EBS, RDS, S3/Blob/GCS)
- **In Transit**: TLS/SSL encryption for all network traffic
- **Secrets**: Managed secret stores (AWS Secrets Manager, Key Vault, Secret Manager)

### Network Security
- **VPC Isolation**: Private subnets for workloads
- **Security Groups**: Least-privilege access rules
- **Network ACLs**: Additional layer of security
- **Private Endpoints**: Secure access to cloud services

### Access Control
- **IAM Roles**: Service-specific least-privilege permissions
- **RBAC**: Kubernetes role-based access control
- **Audit Logging**: Comprehensive audit trails
- **Resource Tagging**: Compliance and cost allocation

## 📊 Monitoring & Observability

### Cloud-Native Monitoring
- **AWS**: CloudWatch dashboards, alarms, and insights
- **Azure**: Azure Monitor, Log Analytics, and Application Insights
- **GCP**: Cloud Monitoring, Cloud Logging, and Cloud Trace

### Kubernetes Monitoring
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Visualization dashboards  
- **Health Checks**: Liveness and readiness probes
- **Auto-scaling**: HPA based on CPU/memory metrics

## 🔄 Disaster Recovery

### Backup Strategy
- **Database**: Automated daily backups with point-in-time recovery
- **State Files**: Versioned S3 backend with DynamoDB locking
- **Configurations**: Version controlled in Git

### High Availability
- **Multi-AZ**: Resources distributed across availability zones
- **Auto-scaling**: Automatic capacity adjustment
- **Load Balancing**: Traffic distribution and failover
- **Rolling Updates**: Zero-downtime deployments

## 🔍 Troubleshooting

### Common Issues

**Backend State Locking:**
```bash
# Force unlock if needed (use with caution)
terraform force-unlock <LOCK_ID>

# Or use specific backend config
terraform init -backend-config=environments/prod/backend.hcl
```

**Resource Conflicts:**
```bash
# Import existing resources
make import ENV=prod RESOURCE=aws_instance.example ID=i-12345

# Or use terraform import directly
terraform import -var-file=environments/prod/terraform.tfvars aws_instance.example i-12345
```

**Permission Issues:**
```bash
# Verify cloud CLI authentication
aws sts get-caller-identity     # AWS
az account show                 # Azure  
gcloud auth list               # GCP

# Check required permissions
terraform plan -var-file=environments/dev/terraform.tfvars
```

### Debug Mode
```bash
# Enable debug logging
export TF_LOG=DEBUG
terraform plan -var-file=environments/dev/terraform.tfvars

# Or use deployment scripts with debug
./scripts/deploy.sh -e dev -c aws -v  # Verbose mode
```

## 📝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Test** your changes (`make lint && make validate`)
4. **Commit** your changes (`git commit -m 'Add amazing feature'`)
5. **Push** to the branch (`git push origin feature/amazing-feature`)
6. **Open** a Pull Request

### Code Standards
- Use consistent naming conventions
- Add comments for complex resources
- Test in development environment first
- Update documentation as needed

## 📞 Support

- **Issues**: Report bugs and feature requests via GitHub Issues
- **Documentation**: Check the main project README for application details
- **Discussions**: Use GitHub Discussions for questions and ideas

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.