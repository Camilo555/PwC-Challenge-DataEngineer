# Multi-Cloud Networking Module
# Provides secure, optimized networking across AWS, Azure, and GCP
# Features: VPC peering, transit gateways, zero-trust architecture

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.5"
}

# AWS Transit Gateway for centralized connectivity
resource "aws_ec2_transit_gateway" "main" {
  description                     = "Main transit gateway for multi-cloud connectivity"
  default_route_table_association = "enable"
  default_route_table_propagation = "enable"
  dns_support                     = "enable"
  vpn_ecmp_support               = "enable"
  
  tags = {
    Name        = "${var.project_name}-tgw"
    Environment = var.environment
    Project     = var.project_name
  }
}

# AWS VPC for data platform
resource "aws_vpc" "data_platform" {
  cidr_block           = var.aws_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = {
    Name        = "${var.project_name}-aws-vpc"
    Environment = var.environment
    Project     = var.project_name
  }
}

# AWS Private Subnets
resource "aws_subnet" "aws_private" {
  count             = length(var.aws_availability_zones)
  vpc_id            = aws_vpc.data_platform.id
  cidr_block        = cidrsubnet(var.aws_vpc_cidr, 8, count.index + 1)
  availability_zone = var.aws_availability_zones[count.index]
  
  tags = {
    Name        = "${var.project_name}-aws-private-${count.index + 1}"
    Environment = var.environment
    Type        = "Private"
  }
}

# AWS Public Subnets
resource "aws_subnet" "aws_public" {
  count                   = length(var.aws_availability_zones)
  vpc_id                  = aws_vpc.data_platform.id
  cidr_block              = cidrsubnet(var.aws_vpc_cidr, 8, count.index + 101)
  availability_zone       = var.aws_availability_zones[count.index]
  map_public_ip_on_launch = true
  
  tags = {
    Name        = "${var.project_name}-aws-public-${count.index + 1}"
    Environment = var.environment
    Type        = "Public"
  }
}

# AWS Internet Gateway
resource "aws_internet_gateway" "aws_igw" {
  vpc_id = aws_vpc.data_platform.id
  
  tags = {
    Name        = "${var.project_name}-aws-igw"
    Environment = var.environment
  }
}

# AWS NAT Gateway for private subnet internet access
resource "aws_eip" "nat_gateway" {
  count  = length(aws_subnet.aws_public)
  domain = "vpc"
  
  tags = {
    Name = "${var.project_name}-nat-eip-${count.index + 1}"
  }
  
  depends_on = [aws_internet_gateway.aws_igw]
}

resource "aws_nat_gateway" "aws_nat" {
  count         = length(aws_subnet.aws_public)
  allocation_id = aws_eip.nat_gateway[count.index].id
  subnet_id     = aws_subnet.aws_public[count.index].id
  
  tags = {
    Name = "${var.project_name}-nat-${count.index + 1}"
  }
}

# Transit Gateway VPC Attachment
resource "aws_ec2_transit_gateway_vpc_attachment" "aws_attachment" {
  subnet_ids         = aws_subnet.aws_private[*].id
  transit_gateway_id = aws_ec2_transit_gateway.main.id
  vpc_id             = aws_vpc.data_platform.id
  
  tags = {
    Name = "${var.project_name}-tgw-attachment"
  }
}

# Azure Virtual Network
resource "azurerm_virtual_network" "data_platform" {
  name                = "${var.project_name}-azure-vnet"
  address_space       = [var.azure_vnet_cidr]
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Azure Subnets
resource "azurerm_subnet" "azure_private" {
  count                = length(var.azure_subnets)
  name                 = "${var.project_name}-azure-private-${count.index + 1}"
  resource_group_name  = var.azure_resource_group_name
  virtual_network_name = azurerm_virtual_network.data_platform.name
  address_prefixes     = [var.azure_subnets[count.index]]
}

# Azure VPN Gateway for cross-cloud connectivity
resource "azurerm_subnet" "gateway" {
  name                 = "GatewaySubnet"
  resource_group_name  = var.azure_resource_group_name
  virtual_network_name = azurerm_virtual_network.data_platform.name
  address_prefixes     = [var.azure_gateway_subnet]
}

resource "azurerm_public_ip" "vpn_gateway" {
  name                = "${var.project_name}-vpn-gateway-ip"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  allocation_method   = "Dynamic"
}

resource "azurerm_virtual_network_gateway" "vpn_gateway" {
  name                = "${var.project_name}-vpn-gateway"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  type     = "Vpn"
  vpn_type = "RouteBased"
  
  active_active = false
  enable_bgp    = false
  sku           = "VpnGw1"
  
  ip_configuration {
    name                          = "vnetGatewayConfig"
    public_ip_address_id          = azurerm_public_ip.vpn_gateway.id
    private_ip_address_allocation = "Dynamic"
    subnet_id                     = azurerm_subnet.gateway.id
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# GCP VPC Network
resource "google_compute_network" "data_platform" {
  name                    = "${var.project_name}-gcp-vpc"
  auto_create_subnetworks = false
  project                 = var.gcp_project_id
}

# GCP Subnets
resource "google_compute_subnetwork" "gcp_private" {
  count         = length(var.gcp_subnets)
  name          = "${var.project_name}-gcp-private-${count.index + 1}"
  ip_cidr_range = var.gcp_subnets[count.index]
  region        = var.gcp_region
  network       = google_compute_network.data_platform.id
  project       = var.gcp_project_id
  
  private_ip_google_access = true
  
  secondary_ip_range {
    range_name    = "pod-range"
    ip_cidr_range = cidrsubnet(var.gcp_subnets[count.index], 4, 1)
  }
  
  secondary_ip_range {
    range_name    = "service-range"
    ip_cidr_range = cidrsubnet(var.gcp_subnets[count.index], 4, 2)
  }
}

# GCP Cloud Router for VPN connectivity
resource "google_compute_router" "cloud_router" {
  name    = "${var.project_name}-cloud-router"
  region  = var.gcp_region
  network = google_compute_network.data_platform.id
  project = var.gcp_project_id
  
  bgp {
    asn = 64514
  }
}

# GCP VPN Gateway
resource "google_compute_vpn_gateway" "vpn_gateway" {
  name    = "${var.project_name}-vpn-gateway"
  network = google_compute_network.data_platform.id
  region  = var.gcp_region
  project = var.gcp_project_id
}

# Cross-Cloud VPN Tunnels (AWS-Azure)
resource "aws_vpn_gateway" "aws_vpn" {
  vpc_id = aws_vpc.data_platform.id
  
  tags = {
    Name = "${var.project_name}-aws-vpn-gateway"
  }
}

resource "aws_customer_gateway" "azure_customer_gateway" {
  bgp_asn    = 65000
  ip_address = azurerm_public_ip.vpn_gateway.ip_address
  type       = "ipsec.1"
  
  tags = {
    Name = "${var.project_name}-azure-customer-gateway"
  }
}

resource "aws_vpn_connection" "aws_to_azure" {
  vpn_gateway_id      = aws_vpn_gateway.aws_vpn.id
  customer_gateway_id = aws_customer_gateway.azure_customer_gateway.id
  type                = "ipsec.1"
  static_routes_only  = true
  
  tags = {
    Name = "${var.project_name}-aws-to-azure-vpn"
  }
}

# Network Security Groups and Firewall Rules
resource "aws_security_group" "multi_cloud_sg" {
  name_prefix = "${var.project_name}-multi-cloud-"
  vpc_id      = aws_vpc.data_platform.id
  
  # Allow internal multi-cloud communication
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr, var.azure_vnet_cidr, var.gcp_vpc_cidr]
  }
  
  # Allow HTTPS/TLS
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  # Allow HTTP for health checks
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr, var.azure_vnet_cidr, var.gcp_vpc_cidr]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${var.project_name}-multi-cloud-sg"
  }
}

resource "azurerm_network_security_group" "multi_cloud_nsg" {
  name                = "${var.project_name}-multi-cloud-nsg"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  security_rule {
    name                       = "AllowMultiCloudTraffic"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefixes    = [var.aws_vpc_cidr, var.azure_vnet_cidr, var.gcp_vpc_cidr]
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "google_compute_firewall" "multi_cloud_firewall" {
  name    = "${var.project_name}-multi-cloud-firewall"
  network = google_compute_network.data_platform.name
  project = var.gcp_project_id
  
  allow {
    protocol = "tcp"
    ports    = ["80", "443", "22", "3389"]
  }
  
  allow {
    protocol = "icmp"
  }
  
  source_ranges = [var.aws_vpc_cidr, var.azure_vnet_cidr, var.gcp_vpc_cidr]
  target_tags   = ["multi-cloud"]
}

# Load Balancer for intelligent traffic distribution
resource "aws_lb" "multi_cloud_alb" {
  name               = "${var.project_name}-multi-cloud-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.multi_cloud_sg.id]
  subnets            = aws_subnet.aws_public[*].id
  
  enable_deletion_protection = var.enable_deletion_protection
  
  tags = {
    Name        = "${var.project_name}-multi-cloud-alb"
    Environment = var.environment
  }
}

# Network monitoring and performance optimization
resource "aws_cloudwatch_log_group" "network_monitoring" {
  name              = "/aws/networkmonitoring/${var.project_name}"
  retention_in_days = 30
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# VPC Flow Logs for network analysis
resource "aws_flow_log" "vpc_flow_log" {
  iam_role_arn    = aws_iam_role.flow_log_role.arn
  log_destination = aws_cloudwatch_log_group.network_monitoring.arn
  traffic_type    = "ALL"
  vpc_id          = aws_vpc.data_platform.id
  
  tags = {
    Name = "${var.project_name}-vpc-flow-logs"
  }
}

# IAM Role for VPC Flow Logs
resource "aws_iam_role" "flow_log_role" {
  name = "${var.project_name}-flow-log-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "vpc-flow-logs.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "flow_log_policy" {
  name = "${var.project_name}-flow-log-policy"
  role = aws_iam_role.flow_log_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Cross-cloud bandwidth optimization
resource "aws_dx_gateway" "dx_gateway" {
  count = var.enable_direct_connect ? 1 : 0
  name  = "${var.project_name}-dx-gateway"
}

# Route tables for optimized traffic flow
resource "aws_route_table" "private_rt" {
  count  = length(aws_subnet.aws_private)
  vpc_id = aws_vpc.data_platform.id
  
  route {
    cidr_block = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.aws_nat[count.index].id
  }
  
  route {
    cidr_block         = var.azure_vnet_cidr
    transit_gateway_id = aws_ec2_transit_gateway.main.id
  }
  
  route {
    cidr_block         = var.gcp_vpc_cidr
    transit_gateway_id = aws_ec2_transit_gateway.main.id
  }
  
  tags = {
    Name = "${var.project_name}-private-rt-${count.index + 1}"
  }
}

resource "aws_route_table_association" "private_rt_association" {
  count          = length(aws_subnet.aws_private)
  subnet_id      = aws_subnet.aws_private[count.index].id
  route_table_id = aws_route_table.private_rt[count.index].id
}

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.data_platform.id
  
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.aws_igw.id
  }
  
  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public_rt_association" {
  count          = length(aws_subnet.aws_public)
  subnet_id      = aws_subnet.aws_public[count.index].id
  route_table_id = aws_route_table.public_rt.id
}