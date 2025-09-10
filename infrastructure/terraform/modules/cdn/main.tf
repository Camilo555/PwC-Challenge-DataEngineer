# BMAD Platform - Global CDN Infrastructure for Mobile Optimization (Story 4.1)
# Multi-cloud CDN with edge computing and mobile-first performance optimization

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

locals {
  common_tags = var.tags
  
  # Mobile optimization configurations
  mobile_optimizations = {
    image_formats = ["webp", "avif", "jpg", "png"]
    compression_levels = {
      images = 85
      css    = 95
      js     = 95
    }
    cache_rules = {
      api_dynamic  = 300    # 5 minutes
      api_static   = 3600   # 1 hour
      images       = 86400  # 24 hours
      css_js       = 2592000 # 30 days
      fonts        = 31536000 # 1 year
    }
  }
  
  # Global edge locations for mobile users
  edge_locations = {
    us_east_1      = { region = "us-east-1", priority = 1 }
    us_west_1      = { region = "us-west-1", priority = 1 }
    eu_west_1      = { region = "eu-west-1", priority = 2 }
    ap_southeast_1 = { region = "ap-southeast-1", priority = 2 }
    ap_northeast_1 = { region = "ap-northeast-1", priority = 3 }
    sa_east_1      = { region = "sa-east-1", priority = 3 }
  }
}

# AWS CloudFront Distribution for Global CDN
resource "aws_cloudfront_distribution" "bmad_mobile_cdn" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  
  # Multiple origins for multi-cloud failover
  dynamic "origin" {
    for_each = compact([
      var.aws_origin_domain,
      var.azure_origin_domain,
      var.gcp_origin_domain
    ])
    
    content {
      domain_name = origin.value
      origin_id   = "origin-${origin.key + 1}"
      
      custom_origin_config {
        http_port              = 80
        https_port             = 443
        origin_protocol_policy = "https-only"
        origin_ssl_protocols   = ["TLSv1.2"]
        origin_keepalive_timeout = 60
        origin_read_timeout    = 60
      }
      
      # Mobile-specific headers
      custom_header {
        name  = "X-Mobile-Optimized"
        value = "true"
      }
      
      custom_header {
        name  = "X-CDN-Provider"
        value = "aws-cloudfront"
      }
    }
  }
  
  enabled             = true
  is_ipv6_enabled     = true
  comment             = "BMAD Platform Mobile-Optimized CDN"
  default_root_object = "index.html"
  
  # Mobile-optimized aliases
  aliases = [
    "mobile.${var.domain_name}",
    "app.${var.domain_name}",
    "api-mobile.${var.domain_name}"
  ]
  
  # Default cache behavior for mobile apps
  default_cache_behavior {
    allowed_methods        = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods         = ["GET", "HEAD", "OPTIONS"]
    target_origin_id       = "origin-1"
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    
    # Mobile-optimized headers
    forwarded_values {
      query_string = true
      headers = [
        "User-Agent",
        "CloudFront-Device-Type",
        "CloudFront-Is-Mobile-Viewer",
        "CloudFront-Is-Tablet-Viewer",
        "Accept",
        "Accept-Encoding",
        "Accept-Language"
      ]
      
      cookies {
        forward = "whitelist"
        whitelisted_names = [
          "session_id",
          "user_preferences",
          "mobile_app_version"
        ]
      }
    }
    
    min_ttl     = 0
    default_ttl = local.mobile_optimizations.cache_rules.api_dynamic
    max_ttl     = local.mobile_optimizations.cache_rules.api_static
    
    # Lambda@Edge functions for mobile optimization
    lambda_function_association {
      event_type   = "viewer-request"
      lambda_arn   = aws_lambda_function.mobile_device_detection[0].qualified_arn
      include_body = false
    }
    
    lambda_function_association {
      event_type   = "origin-response"
      lambda_arn   = aws_lambda_function.mobile_image_optimization[0].qualified_arn
      include_body = false
    }
  }
  
  # API cache behavior - optimized for mobile API calls
  ordered_cache_behavior {
    path_pattern           = "/api/mobile/*"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD", "OPTIONS"]
    target_origin_id       = "origin-1"
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    
    forwarded_values {
      query_string = true
      headers = [
        "Authorization",
        "Content-Type",
        "User-Agent",
        "X-API-Version",
        "X-Mobile-App-Version",
        "CloudFront-Device-Type"
      ]
      
      cookies {
        forward = "none"
      }
    }
    
    min_ttl     = 0
    default_ttl = local.mobile_optimizations.cache_rules.api_dynamic
    max_ttl     = local.mobile_optimizations.cache_rules.api_static
  }
  
  # Static assets cache behavior - aggressive caching for mobile
  ordered_cache_behavior {
    path_pattern           = "/static/*"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "origin-1"
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    
    forwarded_values {
      query_string = false
      headers      = ["CloudFront-Device-Type"]
      
      cookies {
        forward = "none"
      }
    }
    
    min_ttl     = local.mobile_optimizations.cache_rules.images
    default_ttl = local.mobile_optimizations.cache_rules.css_js
    max_ttl     = local.mobile_optimizations.cache_rules.fonts
    
    # Image optimization for mobile
    lambda_function_association {
      event_type   = "origin-response"
      lambda_arn   = aws_lambda_function.mobile_image_optimization[0].qualified_arn
      include_body = false
    }
  }
  
  # Images cache behavior - WebP/AVIF optimization for mobile
  ordered_cache_behavior {
    path_pattern           = "/images/*"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "origin-1"
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    
    forwarded_values {
      query_string = true  # For dynamic image resizing
      headers = [
        "Accept",
        "CloudFront-Device-Type",
        "CloudFront-Viewer-Country"
      ]
      
      cookies {
        forward = "none"
      }
    }
    
    min_ttl     = local.mobile_optimizations.cache_rules.images
    default_ttl = local.mobile_optimizations.cache_rules.images
    max_ttl     = local.mobile_optimizations.cache_rules.images
    
    # Advanced image optimization
    lambda_function_association {
      event_type   = "viewer-request"
      lambda_arn   = aws_lambda_function.mobile_image_resizer[0].qualified_arn
      include_body = false
    }
  }
  
  # Price class for global distribution
  price_class = "PriceClass_All"
  
  # Geographic restrictions
  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }
  
  # SSL/TLS configuration
  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate.mobile_cert[0].arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
  
  # Custom error pages for mobile apps
  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/mobile-error.html"
  }
  
  custom_error_response {
    error_code         = 403
    response_code      = 200
    response_page_path = "/mobile-maintenance.html"
  }
  
  # Web Application Firewall
  web_acl_id = aws_wafv2_web_acl.mobile_waf[0].arn
  
  tags = merge(local.common_tags, {
    Purpose = "Mobile CDN"
    Story   = "4.1"
  })
}

# Mobile Device Detection Lambda@Edge
resource "aws_lambda_function" "mobile_device_detection" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  
  filename         = "mobile_device_detection.zip"
  function_name    = "${var.project_name}-mobile-device-detection"
  role            = aws_iam_role.lambda_edge_role[0].arn
  handler         = "index.handler"
  source_code_hash = data.archive_file.mobile_device_detection_zip[0].output_base64sha256
  runtime         = "nodejs18.x"
  timeout         = 5
  publish         = true
  
  tags = local.common_tags
}

# Mobile Image Optimization Lambda@Edge
resource "aws_lambda_function" "mobile_image_optimization" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  
  filename         = "mobile_image_optimization.zip"
  function_name    = "${var.project_name}-mobile-image-optimization"
  role            = aws_iam_role.lambda_edge_role[0].arn
  handler         = "index.handler"
  source_code_hash = data.archive_file.mobile_image_optimization_zip[0].output_base64sha256
  runtime         = "nodejs18.x"
  timeout         = 30
  memory_size      = 512
  publish         = true
  
  tags = local.common_tags
}

# Mobile Image Resizer Lambda@Edge
resource "aws_lambda_function" "mobile_image_resizer" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  
  filename         = "mobile_image_resizer.zip"
  function_name    = "${var.project_name}-mobile-image-resizer"
  role            = aws_iam_role.lambda_edge_role[0].arn
  handler         = "index.handler"
  source_code_hash = data.archive_file.mobile_image_resizer_zip[0].output_base64sha256
  runtime         = "nodejs18.x"
  timeout         = 30
  memory_size      = 1024
  publish         = true
  
  tags = local.common_tags
}

# Lambda@Edge IAM Role
resource "aws_iam_role" "lambda_edge_role" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  name  = "${var.project_name}-lambda-edge-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com",
            "edgelambda.amazonaws.com"
          ]
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# Lambda@Edge IAM Policy
resource "aws_iam_role_policy_attachment" "lambda_edge_basic" {
  count      = var.deploy_aws_cloudfront ? 1 : 0
  role       = aws_iam_role.lambda_edge_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ACM Certificate for Mobile Domains
resource "aws_acm_certificate" "mobile_cert" {
  count           = var.deploy_aws_cloudfront ? 1 : 0
  domain_name     = "mobile.${var.domain_name}"
  validation_method = "DNS"
  
  subject_alternative_names = [
    "app.${var.domain_name}",
    "api-mobile.${var.domain_name}",
    "*.mobile.${var.domain_name}"
  ]
  
  lifecycle {
    create_before_destroy = true
  }
  
  tags = local.common_tags
}

# WAF for Mobile Security
resource "aws_wafv2_web_acl" "mobile_waf" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  name  = "${var.project_name}-mobile-waf"
  scope = "CLOUDFRONT"
  
  default_action {
    allow {}
  }
  
  # Rate limiting for mobile API
  rule {
    name     = "RateLimitMobileAPI"
    priority = 1
    
    action {
      block {}
    }
    
    statement {
      rate_based_statement {
        limit          = 2000  # Requests per 5 minutes
        aggregate_key_type = "IP"
        
        scope_down_statement {
          byte_match_statement {
            search_string = "/api/mobile/"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "LOWERCASE"
            }
            positional_constraint = "STARTS_WITH"
          }
        }
      }
    }
    
    visibility_config {
      sampled_requests_enabled   = true
      cloudwatch_metrics_enabled = true
      metric_name               = "MobileAPIRateLimit"
    }
  }
  
  # Mobile app protection
  rule {
    name     = "MobileAppProtection"
    priority = 2
    
    action {
      allow {}
    }
    
    statement {
      and_statement {
        statement {
          byte_match_statement {
            search_string = "BMAD-Mobile-App"
            field_to_match {
              single_header {
                name = "user-agent"
              }
            }
            text_transformation {
              priority = 0
              type     = "LOWERCASE"
            }
            positional_constraint = "CONTAINS"
          }
        }
        
        statement {
          ip_set_reference_statement {
            arn = aws_wafv2_ip_set.mobile_whitelist[0].arn
          }
        }
      }
    }
    
    visibility_config {
      sampled_requests_enabled   = true
      cloudwatch_metrics_enabled = true
      metric_name               = "MobileAppAccess"
    }
  }
  
  tags = local.common_tags
}

# IP Whitelist for Mobile App
resource "aws_wafv2_ip_set" "mobile_whitelist" {
  count = var.deploy_aws_cloudfront ? 1 : 0
  name  = "${var.project_name}-mobile-whitelist"
  scope = "CLOUDFRONT"
  
  ip_address_version = "IPV4"
  
  # Add trusted IP ranges for mobile app access
  addresses = [
    "0.0.0.0/0"  # Allow all for now, restrict based on requirements
  ]
  
  tags = local.common_tags
}

# Azure CDN Profile for Multi-Cloud Redundancy
resource "azurerm_cdn_profile" "bmad_mobile_cdn" {
  count               = var.deploy_azure_cdn ? 1 : 0
  name                = "${var.project_name}-mobile-cdn"
  location            = "Global"
  resource_group_name = var.azure_resource_group_name
  sku                 = "Standard_Microsoft"
  
  tags = local.common_tags
}

# Azure CDN Endpoint for Mobile
resource "azurerm_cdn_endpoint" "mobile_endpoint" {
  count               = var.deploy_azure_cdn ? 1 : 0
  name                = "${var.project_name}-mobile"
  profile_name        = azurerm_cdn_profile.bmad_mobile_cdn[0].name
  location            = azurerm_cdn_profile.bmad_mobile_cdn[0].location
  resource_group_name = var.azure_resource_group_name
  
  origin_host_header = var.azure_origin_domain
  
  origin {
    name      = "mobile-origin"
    host_name = var.azure_origin_domain
  }
  
  # Mobile optimization rules
  delivery_rule {
    name  = "MobileImageOptimization"
    order = 1
    
    request_header_condition {
      selector         = "Accept"
      operator         = "Contains"
      match_values     = ["image/webp", "image/avif"]
      negate_condition = false
    }
    
    modify_response_header_action {
      action = "Append"
      name   = "Vary"
      value  = "Accept"
    }
  }
  
  delivery_rule {
    name  = "MobileCompression"
    order = 2
    
    request_header_condition {
      selector         = "User-Agent"
      operator         = "Contains"
      match_values     = ["Mobile", "Android", "iPhone", "iPad"]
      negate_condition = false
    }
    
    modify_response_header_action {
      action = "Append"
      name   = "Content-Encoding"
      value  = "gzip"
    }
  }
  
  # Cache rules for mobile content
  global_delivery_rule {
    cache_expiration_action {
      behavior = "Override"
      duration = "1.00:00:00"  # 1 day for mobile assets
    }
    
    cache_key_query_string_action {
      behavior   = "IncludeSpecified"
      parameters = "width,height,quality,format"
    }
  }
  
  tags = local.common_tags
}

# Google Cloud CDN for Global Reach
resource "google_compute_backend_service" "mobile_backend" {
  count       = var.deploy_gcp_cdn ? 1 : 0
  name        = "${var.project_name}-mobile-backend"
  description = "Backend service for mobile CDN"
  
  enable_cdn = true
  
  backend {
    group = var.gcp_instance_group
  }
  
  cdn_policy {
    cache_mode        = "CACHE_ALL_STATIC"
    default_ttl       = local.mobile_optimizations.cache_rules.api_static
    max_ttl          = local.mobile_optimizations.cache_rules.fonts
    client_ttl       = local.mobile_optimizations.cache_rules.images
    negative_caching = true
    
    cache_key_policy {
      include_host         = true
      include_protocol     = true
      include_query_string = true
      query_string_whitelist = [
        "width", "height", "quality", "format", "device"
      ]
    }
  }
  
  # Health check for mobile backend
  health_checks = [google_compute_health_check.mobile_health[0].id]
  
  # Mobile-optimized load balancing
  load_balancing_scheme = "EXTERNAL"
  protocol             = "HTTP"
  
  # Session affinity for mobile apps
  session_affinity = "CLIENT_IP"
  
  # Connection draining timeout
  timeout_sec = 30
}

# Health Check for Mobile Services
resource "google_compute_health_check" "mobile_health" {
  count = var.deploy_gcp_cdn ? 1 : 0
  name  = "${var.project_name}-mobile-health"
  
  timeout_sec        = 5
  check_interval_sec = 10
  
  http_health_check {
    port         = 80
    request_path = "/health/mobile"
  }
}

# Global Load Balancer for Mobile CDN
resource "google_compute_global_forwarding_rule" "mobile_forwarding_rule" {
  count      = var.deploy_gcp_cdn ? 1 : 0
  name       = "${var.project_name}-mobile-forwarding-rule"
  target     = google_compute_target_http_proxy.mobile_proxy[0].id
  port_range = "80"
}

resource "google_compute_target_http_proxy" "mobile_proxy" {
  count   = var.deploy_gcp_cdn ? 1 : 0
  name    = "${var.project_name}-mobile-proxy"
  url_map = google_compute_url_map.mobile_url_map[0].id
}

resource "google_compute_url_map" "mobile_url_map" {
  count           = var.deploy_gcp_cdn ? 1 : 0
  name            = "${var.project_name}-mobile-url-map"
  default_service = google_compute_backend_service.mobile_backend[0].id
  
  # Mobile API routing
  path_matcher {
    name            = "mobile-api"
    default_service = google_compute_backend_service.mobile_backend[0].id
    
    path_rule {
      paths   = ["/api/mobile/*"]
      service = google_compute_backend_service.mobile_backend[0].id
    }
    
    path_rule {
      paths   = ["/static/*", "/images/*"]
      service = google_compute_backend_service.mobile_backend[0].id
    }
  }
  
  host_rule {
    hosts        = ["mobile.${var.domain_name}"]
    path_matcher = "mobile-api"
  }
}

# Cloudflare for Global Edge and DDoS Protection
resource "cloudflare_zone" "mobile_zone" {
  count = var.deploy_cloudflare ? 1 : 0
  zone  = "mobile.${var.domain_name}"
  plan  = "pro"
}

resource "cloudflare_zone_settings_override" "mobile_settings" {
  count   = var.deploy_cloudflare ? 1 : 0
  zone_id = cloudflare_zone.mobile_zone[0].id
  
  settings {
    # Mobile optimizations
    mobile_redirect {
      status           = "on"
      mobile_subdomain = "m"
      strip_uri        = false
    }
    
    # Image optimization
    polish = "lossless"
    webp   = "on"
    
    # Performance optimizations
    minify {
      css  = "on"
      js   = "on"
      html = "on"
    }
    
    # Security
    security_level = "medium"
    
    # Caching
    browser_cache_ttl = local.mobile_optimizations.cache_rules.images
    
    # SSL
    ssl                      = "strict"
    always_use_https        = "on"
    automatic_https_rewrites = "on"
    
    # Performance
    brotli = "on"
  }
}

# Mobile-specific page rules
resource "cloudflare_page_rule" "mobile_api_cache" {
  count   = var.deploy_cloudflare ? 1 : 0
  zone_id = cloudflare_zone.mobile_zone[0].id
  target  = "mobile.${var.domain_name}/api/mobile/*"
  
  actions {
    cache_level = "bypass"  # Don't cache API responses
    
    # Add mobile headers
    browser_cache_ttl = local.mobile_optimizations.cache_rules.api_dynamic
  }
}

resource "cloudflare_page_rule" "mobile_static_cache" {
  count   = var.deploy_cloudflare ? 1 : 0
  zone_id = cloudflare_zone.mobile_zone[0].id
  target  = "mobile.${var.domain_name}/static/*"
  
  actions {
    cache_level       = "cache_everything"
    edge_cache_ttl    = local.mobile_optimizations.cache_rules.css_js
    browser_cache_ttl = local.mobile_optimizations.cache_rules.css_js
  }
}

# Archive files for Lambda@Edge functions
data "archive_file" "mobile_device_detection_zip" {
  count       = var.deploy_aws_cloudfront ? 1 : 0
  type        = "zip"
  output_path = "mobile_device_detection.zip"
  
  source {
    content = templatefile("${path.module}/lambda/mobile-device-detection.js", {
      project_name = var.project_name
    })
    filename = "index.js"
  }
}

data "archive_file" "mobile_image_optimization_zip" {
  count       = var.deploy_aws_cloudfront ? 1 : 0
  type        = "zip"
  output_path = "mobile_image_optimization.zip"
  
  source {
    content = templatefile("${path.module}/lambda/mobile-image-optimization.js", {
      project_name = var.project_name
    })
    filename = "index.js"
  }
}

data "archive_file" "mobile_image_resizer_zip" {
  count       = var.deploy_aws_cloudfront ? 1 : 0
  type        = "zip"
  output_path = "mobile_image_resizer.zip"
  
  source {
    content = templatefile("${path.module}/lambda/mobile-image-resizer.js", {
      project_name = var.project_name
    })
    filename = "index.js"
  }
}