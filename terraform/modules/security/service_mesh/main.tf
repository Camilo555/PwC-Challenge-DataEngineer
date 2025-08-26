# =============================================================================
# SERVICE MESH SECURITY SUBMODULE - ISTIO IMPLEMENTATION
# Zero Trust service-to-service communication with mTLS
# =============================================================================

terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# =============================================================================
# LOCAL VARIABLES
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Istio configuration
  istio_namespace = "istio-system"
  istio_gateway_namespace = "istio-ingress"
  
  # Service mesh security policies
  security_policies = {
    default_deny_all = var.default_deny_policy
    mtls_strict      = var.enable_mtls
    workload_identity = var.workload_identity
    authorization_enabled = var.enable_authorization
  }
  
  # mTLS configuration
  mtls_mode = var.enable_mtls ? "STRICT" : "PERMISSIVE"
  
  common_labels = merge(var.labels, {
    "app.kubernetes.io/managed-by" = "terraform"
    "security.istio.io/tlsMode"    = local.mtls_mode
  })
}

# =============================================================================
# ISTIO INSTALLATION
# =============================================================================

# Create Istio system namespace
resource "kubernetes_namespace" "istio_system" {
  metadata {
    name = local.istio_namespace
    labels = merge(local.common_labels, {
      name = local.istio_namespace
      "istio-injection" = "disabled"
    })
  }
}

# Create Istio ingress namespace
resource "kubernetes_namespace" "istio_ingress" {
  metadata {
    name = local.istio_gateway_namespace
    labels = merge(local.common_labels, {
      name = local.istio_gateway_namespace
      "istio-injection" = "enabled"
    })
  }
}

# Istio Base (CRDs and core components)
resource "helm_release" "istio_base" {
  name       = "istio-base"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "base"
  version    = var.istio_version
  namespace  = local.istio_namespace
  
  depends_on = [kubernetes_namespace.istio_system]
  
  set {
    name  = "defaultRevision"
    value = "default"
  }
  
  values = [yamlencode({
    global = {
      meshID = "${local.name_prefix}-mesh"
      network = var.network_name
      
      # Multi-cluster configuration
      meshNetworks = {
        (var.network_name) = {
          endpoints = [{
            fromRegistry = "Kubernetes"
          }]
          gateways = [{
            service = "istio-eastwestgateway.${local.istio_gateway_namespace}.svc.cluster.local"
            port = 15443
          }]
        }
      }
    }
  })]
}

# Istio Control Plane (Istiod)
resource "helm_release" "istiod" {
  name       = "istiod"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "istiod"
  version    = var.istio_version
  namespace  = local.istio_namespace
  
  depends_on = [helm_release.istio_base]
  
  values = [yamlencode({
    global = {
      meshID = "${local.name_prefix}-mesh"
      network = var.network_name
    }
    
    pilot = {
      env = {
        # Enable workload identity
        ENABLE_WORKLOAD_IDENTITY = var.workload_identity
        
        # Security settings
        PILOT_ENABLE_WORKLOAD_ENTRY_AUTOREGISTRATION = true
        PILOT_ENABLE_CROSS_CLUSTER_WORKLOAD_ENTRY = true
        
        # mTLS settings
        PILOT_ENABLE_ALPN_FILTER = true
      }
      
      # Resource configuration
      resources = {
        requests = {
          cpu    = var.istiod_resources.requests.cpu
          memory = var.istiod_resources.requests.memory
        }
        limits = {
          cpu    = var.istiod_resources.limits.cpu
          memory = var.istiod_resources.limits.memory
        }
      }
    }
    
    # Security configuration
    security = {
      # Custom CA configuration
      selfSigned = false
      customCACert = var.intermediate_ca_cert
      customCAKey  = var.intermediate_ca_key
    }
    
    # Telemetry configuration
    telemetry = {
      v2 = {
        enabled = var.enable_telemetry
        
        prometheus = {
          configOverride = {
            metric_relabeling_configs = [
              {
                source_labels = ["__name__"]
                regex = "istio_.*"
                target_label = "service_mesh"
                replacement = "istio"
              }
            ]
          }
        }
      }
    }
  })]
}

# Istio Gateway (Ingress)
resource "helm_release" "istio_gateway" {
  name       = "istio-gateway"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "gateway"
  version    = var.istio_version
  namespace  = local.istio_gateway_namespace
  
  depends_on = [
    helm_release.istiod,
    kubernetes_namespace.istio_ingress
  ]
  
  values = [yamlencode({
    name = "istio-ingressgateway"
    
    service = {
      type = var.gateway_service_type
      ports = [
        {
          name = "http2"
          port = 80
          protocol = "TCP"
          targetPort = 8080
        },
        {
          name = "https"
          port = 443
          protocol = "TCP"
          targetPort = 8443
        },
        {
          name = "tls"
          port = 15443
          protocol = "TCP"
          targetPort = 15443
        }
      ]
      
      annotations = var.gateway_service_annotations
    }
    
    # Resource configuration
    resources = {
      requests = {
        cpu    = var.gateway_resources.requests.cpu
        memory = var.gateway_resources.requests.memory
      }
      limits = {
        cpu    = var.gateway_resources.limits.cpu
        memory = var.gateway_resources.limits.memory
      }
    }
    
    # Security context
    securityContext = {
      runAsUser = 1337
      runAsGroup = 1337
      runAsNonRoot = true
      capabilities = {
        drop = ["ALL"]
      }
      readOnlyRootFilesystem = true
    }
    
    # Auto-scaling
    autoscaling = {
      enabled = true
      minReplicas = var.gateway_min_replicas
      maxReplicas = var.gateway_max_replicas
      targetCPUUtilizationPercentage = 80
    }
  })]
}

# East-West Gateway (Multi-cluster communication)
resource "helm_release" "istio_eastwest_gateway" {
  count = var.enable_multicluster ? 1 : 0
  
  name       = "istio-eastwestgateway"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "gateway"
  version    = var.istio_version
  namespace  = local.istio_gateway_namespace
  
  depends_on = [
    helm_release.istiod,
    kubernetes_namespace.istio_ingress
  ]
  
  values = [yamlencode({
    name = "istio-eastwestgateway"
    
    service = {
      type = "LoadBalancer"
      ports = [
        {
          name = "tls"
          port = 15443
          protocol = "TCP"
          targetPort = 15443
        }
      ]
    }
    
    env = {
      ISTIO_META_ROUTER_MODE = "sni-dnat"
      ISTIO_META_REQUESTED_NETWORK_VIEW = var.network_name
    }
  })]
}

# =============================================================================
# SECURITY POLICIES
# =============================================================================

# Default deny-all authorization policy
resource "kubernetes_manifest" "default_deny_policy" {
  count = var.default_deny_policy ? 1 : 0
  
  manifest = {
    apiVersion = "security.istio.io/v1beta1"
    kind       = "AuthorizationPolicy"
    metadata = {
      name      = "default-deny"
      namespace = "istio-system"
      labels    = local.common_labels
    }
    spec = {}  # Empty spec denies all requests
  }
  
  depends_on = [helm_release.istiod]
}

# Mesh-wide mTLS policy
resource "kubernetes_manifest" "mesh_mtls_policy" {
  count = var.enable_mtls ? 1 : 0
  
  manifest = {
    apiVersion = "security.istio.io/v1beta1"
    kind       = "PeerAuthentication"
    metadata = {
      name      = "default"
      namespace = "istio-system"
      labels    = local.common_labels
    }
    spec = {
      mtls = {
        mode = local.mtls_mode
      }
    }
  }
  
  depends_on = [helm_release.istiod]
}

# Workload-specific authorization policies
resource "kubernetes_manifest" "workload_authorization_policies" {
  for_each = var.authorization_policies
  
  manifest = {
    apiVersion = "security.istio.io/v1beta1"
    kind       = "AuthorizationPolicy"
    metadata = {
      name      = each.key
      namespace = each.value.namespace
      labels    = local.common_labels
    }
    spec = {
      selector = {
        matchLabels = each.value.selector
      }
      rules = each.value.rules
    }
  }
  
  depends_on = [helm_release.istiod]
}

# Security-focused destination rules
resource "kubernetes_manifest" "security_destination_rules" {
  for_each = var.destination_rules
  
  manifest = {
    apiVersion = "networking.istio.io/v1beta1"
    kind       = "DestinationRule"
    metadata = {
      name      = each.key
      namespace = each.value.namespace
      labels    = local.common_labels
    }
    spec = {
      host = each.value.host
      trafficPolicy = merge(
        each.value.traffic_policy,
        var.enable_mtls ? {
          tls = {
            mode = "ISTIO_MUTUAL"
          }
        } : {}
      )
      portLevelSettings = each.value.port_level_settings
    }
  }
  
  depends_on = [helm_release.istiod]
}

# =============================================================================
# OBSERVABILITY AND MONITORING
# =============================================================================

# Telemetry configuration for security metrics
resource "kubernetes_manifest" "security_telemetry" {
  count = var.enable_telemetry ? 1 : 0
  
  manifest = {
    apiVersion = "telemetry.istio.io/v1alpha1"
    kind       = "Telemetry"
    metadata = {
      name      = "security-metrics"
      namespace = "istio-system"
      labels    = local.common_labels
    }
    spec = {
      metrics = [
        {
          providers = [
            {
              name = "prometheus"
            }
          ]
          overrides = [
            {
              match = {
                metric = "ALL_METRICS"
              }
              tagOverrides = {
                source_workload = {
                  operation = "UPSERT"
                  value = "%{SOURCE_WORKLOAD}"
                }
                destination_service_name = {
                  operation = "UPSERT"
                  value = "%{DESTINATION_SERVICE_NAME}"
                }
                security_policy = {
                  operation = "UPSERT"
                  value = "%{SECURITY_POLICY}"
                }
              }
            }
          ]
        }
      ]
      
      accessLogging = [
        {
          providers = [
            {
              name = "otel"
            }
          ]
          filter = {
            expression = 'response.code >= 400'
          }
        }
      ]
    }
  }
  
  depends_on = [helm_release.istiod]
}

# Service monitor for Prometheus integration
resource "kubernetes_manifest" "istio_service_monitor" {
  count = var.enable_telemetry && var.enable_prometheus ? 1 : 0
  
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "istio-security-metrics"
      namespace = local.istio_namespace
      labels = merge(local.common_labels, {
        "app.kubernetes.io/name" = "istio-proxy"
      })
    }
    spec = {
      selector = {
        matchLabels = {
          app = "istiod"
        }
      }
      endpoints = [
        {
          port = "http-monitoring"
          interval = "30s"
          path = "/stats/prometheus"
        }
      ]
    }
  }
  
  depends_on = [helm_release.istiod]
}

# =============================================================================
# GATEWAY CONFIGURATION
# =============================================================================

# Main gateway for external traffic
resource "kubernetes_manifest" "main_gateway" {
  manifest = {
    apiVersion = "networking.istio.io/v1beta1"
    kind       = "Gateway"
    metadata = {
      name      = "main-gateway"
      namespace = local.istio_gateway_namespace
      labels    = local.common_labels
    }
    spec = {
      selector = {
        istio = "ingressgateway"
      }
      servers = concat(
        [
          {
            port = {
              number = 80
              name = "http"
              protocol = "HTTP"
            }
            hosts = var.gateway_hosts
            tls = {
              httpsRedirect = true
            }
          }
        ],
        var.enable_https ? [
          {
            port = {
              number = 443
              name = "https"
              protocol = "HTTPS"
            }
            hosts = var.gateway_hosts
            tls = {
              mode = "SIMPLE"
              credentialName = var.tls_secret_name
            }
          }
        ] : []
      )
    }
  }
  
  depends_on = [helm_release.istio_gateway]
}

# East-West gateway for multi-cluster communication
resource "kubernetes_manifest" "eastwest_gateway" {
  count = var.enable_multicluster ? 1 : 0
  
  manifest = {
    apiVersion = "networking.istio.io/v1beta1"
    kind       = "Gateway"
    metadata = {
      name      = "cross-network-gateway"
      namespace = local.istio_gateway_namespace
      labels    = local.common_labels
    }
    spec = {
      selector = {
        istio = "eastwestgateway"
      }
      servers = [
        {
          port = {
            number = 15443
            name = "tls"
            protocol = "TLS"
          }
          tls = {
            mode = "ISTIO_MUTUAL"
          }
          hosts = ["*.local"]
        }
      ]
    }
  }
  
  depends_on = [helm_release.istio_eastwest_gateway]
}

# =============================================================================
# SECURITY SCANNING AND COMPLIANCE
# =============================================================================

# Network policies for additional security
resource "kubernetes_network_policy" "istio_system_policy" {
  metadata {
    name      = "istio-system-network-policy"
    namespace = local.istio_namespace
    labels    = local.common_labels
  }
  
  spec {
    pod_selector {
      match_labels = {
        app = "istiod"
      }
    }
    
    policy_types = ["Ingress", "Egress"]
    
    ingress {
      from {
        namespace_selector {
          match_labels = {
            name = local.istio_gateway_namespace
          }
        }
      }
      
      ports {
        port     = "15010"
        protocol = "TCP"
      }
      
      ports {
        port     = "15011"
        protocol = "TCP"
      }
    }
    
    egress {
      to {}  # Allow all egress for control plane
    }
  }
  
  depends_on = [kubernetes_namespace.istio_system]
}

# Pod security policy for Istio components
resource "kubernetes_manifest" "istio_pod_security_policy" {
  count = var.enable_pod_security_policy ? 1 : 0
  
  manifest = {
    apiVersion = "policy/v1beta1"
    kind       = "PodSecurityPolicy"
    metadata = {
      name = "istio-security-policy"
      labels = local.common_labels
    }
    spec = {
      privileged = false
      allowPrivilegeEscalation = false
      requiredDropCapabilities = ["ALL"]
      volumes = [
        "configMap",
        "emptyDir",
        "projected",
        "secret",
        "downwardAPI",
        "persistentVolumeClaim"
      ]
      runAsUser = {
        rule = "MustRunAsNonRoot"
      }
      seLinux = {
        rule = "RunAsAny"
      }
      fsGroup = {
        rule = "RunAsAny"
      }
    }
  }
  
  depends_on = [kubernetes_namespace.istio_system]
}

# =============================================================================
# BACKUP AND DISASTER RECOVERY
# =============================================================================

# Backup Istio configuration
resource "kubernetes_manifest" "istio_config_backup" {
  count = var.enable_config_backup ? 1 : 0
  
  manifest = {
    apiVersion = "batch/v1"
    kind       = "CronJob"
    metadata = {
      name      = "istio-config-backup"
      namespace = local.istio_namespace
      labels    = local.common_labels
    }
    spec = {
      schedule = var.backup_schedule
      jobTemplate = {
        spec = {
          template = {
            spec = {
              containers = [
                {
                  name = "backup"
                  image = "bitnami/kubectl:latest"
                  command = [
                    "sh", "-c",
                    <<-EOF
                      kubectl get --all-namespaces -o yaml \
                        gateways,virtualservices,destinationrules,serviceentries,sidecars,authorizationpolicies,peerauthentications,requestauthentications,telemetries \
                        > /backup/istio-config-$(date +%Y%m%d-%H%M%S).yaml
                    EOF
                  ]
                  volumeMounts = [
                    {
                      name = "backup-volume"
                      mountPath = "/backup"
                    }
                  ]
                }
              ]
              volumes = [
                {
                  name = "backup-volume"
                  persistentVolumeClaim = {
                    claimName = "istio-backup-pvc"
                  }
                }
              ]
              restartPolicy = "OnFailure"
            }
          }
        }
      }
    }
  }
  
  depends_on = [helm_release.istiod]
}