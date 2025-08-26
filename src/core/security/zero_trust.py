"""
Zero Trust Security Architecture Implementation
Provides comprehensive Zero Trust security framework with micro-segmentation, 
continuous verification, and policy enforcement.
"""
import json
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from ipaddress import AddressValueError, IPv4Address, IPv6Address, ip_address
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import Request
from pydantic import BaseModel, Field, field_validator

from core.logging import get_logger
from core.security.advanced_security import SecurityEventManager, ThreatLevel, SecurityEventType

logger = get_logger(__name__)


class TrustLevel(Enum):
    """Trust levels for Zero Trust assessment"""
    UNTRUSTED = "untrusted"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERIFIED = "verified"


class AccessDecision(Enum):
    """Access control decisions"""
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_MFA = "require_mfa"
    REQUIRE_ADDITIONAL_AUTH = "require_additional_auth"
    MONITOR = "monitor"


class ResourceSensitivity(Enum):
    """Resource sensitivity levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class NetworkZone(Enum):
    """Network security zones"""
    UNTRUSTED = "untrusted"
    DMZ = "dmz"
    INTERNAL = "internal"
    SECURE = "secure"
    MANAGEMENT = "management"


@dataclass
class Identity:
    """Identity representation in Zero Trust model"""
    id: str
    type: str  # user, service, device
    name: str
    roles: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    trust_score: float = 0.0
    last_verified: Optional[datetime] = None
    mfa_enabled: bool = False
    device_compliance: bool = False
    location_verified: bool = False


@dataclass
class Resource:
    """Resource representation in Zero Trust model"""
    id: str
    type: str  # api, data, service, application
    name: str
    sensitivity: ResourceSensitivity
    zone: NetworkZone
    required_trust_level: TrustLevel
    data_classification: List[str] = field(default_factory=list)
    access_patterns: Dict[str, Any] = field(default_factory=dict)
    encryption_required: bool = True
    audit_required: bool = True


@dataclass
class PolicyRule:
    """Zero Trust policy rule"""
    id: str
    name: str
    condition: Dict[str, Any]
    action: AccessDecision
    priority: int = 100
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AccessContext:
    """Context for access decision"""
    identity: Identity
    resource: Resource
    request_time: datetime
    source_ip: str
    device_info: Dict[str, Any] = field(default_factory=dict)
    location_info: Dict[str, Any] = field(default_factory=dict)
    risk_factors: List[str] = field(default_factory=list)
    session_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AccessDecisionResult:
    """Result of access decision"""
    decision: AccessDecision
    confidence: float
    reasons: List[str]
    required_actions: List[str] = field(default_factory=list)
    monitoring_flags: List[str] = field(default_factory=list)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TrustScoreCalculator:
    """Calculates dynamic trust scores for identities"""
    
    def __init__(self):
        self.base_scores = {
            "user": 50.0,
            "service": 70.0,
            "device": 40.0
        }
        self.factor_weights = {
            "mfa_enabled": 15.0,
            "device_compliance": 10.0,
            "location_verified": 8.0,
            "recent_activity": 5.0,
            "behavior_pattern": 7.0,
            "risk_indicators": -20.0,
            "time_since_auth": -10.0
        }
    
    def calculate_trust_score(self, identity: Identity, context: AccessContext) -> float:
        """Calculate dynamic trust score for an identity"""
        base_score = self.base_scores.get(identity.type, 30.0)
        
        # Apply trust factors
        if identity.mfa_enabled:
            base_score += self.factor_weights["mfa_enabled"]
        
        if identity.device_compliance:
            base_score += self.factor_weights["device_compliance"]
        
        if identity.location_verified:
            base_score += self.factor_weights["location_verified"]
        
        # Time-based factors
        if identity.last_verified:
            time_diff = (datetime.now() - identity.last_verified).total_seconds()
            if time_diff > 3600:  # More than 1 hour
                base_score += self.factor_weights["time_since_auth"] * (time_diff / 3600)
        
        # Risk factors
        risk_penalty = len(context.risk_factors) * abs(self.factor_weights["risk_indicators"]) / 2
        base_score -= risk_penalty
        
        # Normalize to 0-100 range
        trust_score = max(0.0, min(100.0, base_score))
        
        return trust_score
    
    def get_trust_level(self, trust_score: float) -> TrustLevel:
        """Convert trust score to trust level"""
        if trust_score >= 85:
            return TrustLevel.VERIFIED
        elif trust_score >= 70:
            return TrustLevel.HIGH
        elif trust_score >= 50:
            return TrustLevel.MEDIUM
        elif trust_score >= 30:
            return TrustLevel.LOW
        else:
            return TrustLevel.UNTRUSTED


class MicroSegmentationEngine:
    """Network micro-segmentation engine"""
    
    def __init__(self):
        self.network_policies: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.zone_definitions = {
            NetworkZone.UNTRUSTED: {
                "allowed_ports": [80, 443],
                "protocols": ["HTTP", "HTTPS"],
                "trust_required": TrustLevel.UNTRUSTED
            },
            NetworkZone.DMZ: {
                "allowed_ports": [80, 443, 22, 8080],
                "protocols": ["HTTP", "HTTPS", "SSH"],
                "trust_required": TrustLevel.LOW
            },
            NetworkZone.INTERNAL: {
                "allowed_ports": list(range(1024, 65535)),
                "protocols": ["HTTP", "HTTPS", "SSH", "RDP", "TCP", "UDP"],
                "trust_required": TrustLevel.MEDIUM
            },
            NetworkZone.SECURE: {
                "allowed_ports": [443, 22, 3389],
                "protocols": ["HTTPS", "SSH", "RDP"],
                "trust_required": TrustLevel.HIGH
            },
            NetworkZone.MANAGEMENT: {
                "allowed_ports": [22, 443, 161, 162],
                "protocols": ["SSH", "HTTPS", "SNMP"],
                "trust_required": TrustLevel.VERIFIED
            }
        }
    
    def classify_network_zone(self, ip_address: str, port: int = None) -> NetworkZone:
        """Classify network zone based on IP address"""
        try:
            ip = ip_address(ip_address)
            
            # Private networks - generally internal
            if ip.is_private:
                if ip in ip_address("10.0.0.0/16"):  # Management network
                    return NetworkZone.MANAGEMENT
                elif ip in ip_address("192.168.100.0/24"):  # Secure network
                    return NetworkZone.SECURE
                else:
                    return NetworkZone.INTERNAL
            
            # Loopback
            elif ip.is_loopback:
                return NetworkZone.SECURE
            
            # Public IPs
            else:
                return NetworkZone.UNTRUSTED
                
        except (AddressValueError, ValueError):
            return NetworkZone.UNTRUSTED
    
    def check_network_access(self, source_zone: NetworkZone, dest_zone: NetworkZone, 
                           port: int, protocol: str, trust_level: TrustLevel) -> bool:
        """Check if network access is allowed based on micro-segmentation rules"""
        
        # Get destination zone requirements
        dest_requirements = self.zone_definitions.get(dest_zone, {})
        
        # Check trust level requirement
        required_trust = dest_requirements.get("trust_required", TrustLevel.HIGH)
        if trust_level.value < required_trust.value:
            return False
        
        # Check port access
        allowed_ports = dest_requirements.get("allowed_ports", [])
        if port and port not in allowed_ports:
            return False
        
        # Check protocol
        allowed_protocols = dest_requirements.get("protocols", [])
        if protocol and protocol.upper() not in allowed_protocols:
            return False
        
        # Zone-to-zone access matrix
        access_matrix = {
            (NetworkZone.UNTRUSTED, NetworkZone.DMZ): True,
            (NetworkZone.UNTRUSTED, NetworkZone.INTERNAL): False,
            (NetworkZone.UNTRUSTED, NetworkZone.SECURE): False,
            (NetworkZone.UNTRUSTED, NetworkZone.MANAGEMENT): False,
            
            (NetworkZone.DMZ, NetworkZone.INTERNAL): True,
            (NetworkZone.DMZ, NetworkZone.SECURE): False,
            (NetworkZone.DMZ, NetworkZone.MANAGEMENT): False,
            
            (NetworkZone.INTERNAL, NetworkZone.SECURE): True,
            (NetworkZone.INTERNAL, NetworkZone.MANAGEMENT): False,
            
            (NetworkZone.SECURE, NetworkZone.MANAGEMENT): True,
        }
        
        return access_matrix.get((source_zone, dest_zone), False)


class PolicyEngine:
    """Zero Trust policy evaluation engine"""
    
    def __init__(self):
        self.policies: List[PolicyRule] = []
        self.trust_calculator = TrustScoreCalculator()
        self.microsegmentation = MicroSegmentationEngine()
        self.decision_cache: Dict[str, Tuple[AccessDecisionResult, float]] = {}
        self.cache_ttl = 300  # 5 minutes
    
    def add_policy(self, policy: PolicyRule) -> None:
        """Add a policy rule"""
        self.policies.append(policy)
        # Sort by priority (lower number = higher priority)
        self.policies.sort(key=lambda p: p.priority)
    
    def evaluate_access(self, context: AccessContext) -> AccessDecisionResult:
        """Evaluate access request against Zero Trust policies"""
        
        # Check cache first
        cache_key = self._generate_cache_key(context)
        if cache_key in self.decision_cache:
            cached_result, timestamp = self.decision_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_result
        
        # Calculate trust score
        trust_score = self.trust_calculator.calculate_trust_score(
            context.identity, context
        )
        trust_level = self.trust_calculator.get_trust_level(trust_score)
        
        # Update identity trust score
        context.identity.trust_score = trust_score
        
        # Evaluate policies
        decision_result = self._evaluate_policies(context, trust_level)
        
        # Apply network micro-segmentation
        network_decision = self._check_network_access(context, trust_level)
        if network_decision.decision == AccessDecision.DENY:
            decision_result = network_decision
        
        # Cache the result
        self.decision_cache[cache_key] = (decision_result, time.time())
        
        return decision_result
    
    def _generate_cache_key(self, context: AccessContext) -> str:
        """Generate cache key for access context"""
        key_components = [
            context.identity.id,
            context.resource.id,
            context.source_ip,
            str(int(context.request_time.timestamp() / 60))  # Minute precision
        ]
        return "|".join(key_components)
    
    def _evaluate_policies(self, context: AccessContext, trust_level: TrustLevel) -> AccessDecisionResult:
        """Evaluate policies against context"""
        
        reasons = []
        confidence = 0.8  # Base confidence
        required_actions = []
        monitoring_flags = []
        
        # Check resource sensitivity vs trust level
        if context.resource.required_trust_level.value > trust_level.value:
            if trust_level == TrustLevel.UNTRUSTED:
                return AccessDecisionResult(
                    decision=AccessDecision.DENY,
                    confidence=0.95,
                    reasons=["Untrusted identity cannot access resource"],
                    metadata={"trust_score": context.identity.trust_score}
                )
            else:
                required_actions.append("additional_authentication")
                return AccessDecisionResult(
                    decision=AccessDecision.REQUIRE_ADDITIONAL_AUTH,
                    confidence=0.9,
                    reasons=[f"Trust level {trust_level.value} insufficient for {context.resource.sensitivity.value} resource"],
                    required_actions=required_actions,
                    metadata={"trust_score": context.identity.trust_score}
                )
        
        # Evaluate custom policies
        for policy in self.policies:
            if not policy.enabled:
                continue
            
            if self._match_policy_condition(policy.condition, context):
                if policy.action == AccessDecision.DENY:
                    return AccessDecisionResult(
                        decision=AccessDecision.DENY,
                        confidence=0.95,
                        reasons=[f"Denied by policy: {policy.name}"],
                        metadata={"policy_id": policy.id, "trust_score": context.identity.trust_score}
                    )
                elif policy.action == AccessDecision.REQUIRE_MFA:
                    if not context.identity.mfa_enabled:
                        return AccessDecisionResult(
                            decision=AccessDecision.REQUIRE_MFA,
                            confidence=0.9,
                            reasons=[f"MFA required by policy: {policy.name}"],
                            required_actions=["mfa_verification"],
                            metadata={"policy_id": policy.id, "trust_score": context.identity.trust_score}
                        )
        
        # Check for risk factors
        if context.risk_factors:
            monitoring_flags.extend(context.risk_factors)
            if len(context.risk_factors) > 2:
                confidence -= 0.2
                return AccessDecisionResult(
                    decision=AccessDecision.MONITOR,
                    confidence=confidence,
                    reasons=["Multiple risk factors detected"],
                    monitoring_flags=monitoring_flags,
                    metadata={"trust_score": context.identity.trust_score}
                )
        
        # Default allow with monitoring for high-value resources
        if context.resource.sensitivity in [ResourceSensitivity.RESTRICTED, ResourceSensitivity.TOP_SECRET]:
            monitoring_flags.append("high_value_resource_access")
        
        return AccessDecisionResult(
            decision=AccessDecision.ALLOW,
            confidence=confidence,
            reasons=["Access granted based on trust level and policies"],
            monitoring_flags=monitoring_flags,
            expires_at=datetime.now() + timedelta(hours=1),
            metadata={"trust_score": context.identity.trust_score}
        )
    
    def _match_policy_condition(self, condition: Dict[str, Any], context: AccessContext) -> bool:
        """Check if policy condition matches context"""
        
        # Check identity conditions
        if "identity_type" in condition:
            if context.identity.type not in condition["identity_type"]:
                return False
        
        if "roles" in condition:
            if not any(role in context.identity.roles for role in condition["roles"]):
                return False
        
        # Check resource conditions
        if "resource_type" in condition:
            if context.resource.type not in condition["resource_type"]:
                return False
        
        if "resource_sensitivity" in condition:
            if context.resource.sensitivity.value not in condition["resource_sensitivity"]:
                return False
        
        # Check time conditions
        if "time_range" in condition:
            current_hour = context.request_time.hour
            allowed_hours = condition["time_range"]
            if current_hour not in range(allowed_hours[0], allowed_hours[1]):
                return False
        
        # Check location conditions
        if "allowed_countries" in condition:
            user_country = context.location_info.get("country")
            if user_country not in condition["allowed_countries"]:
                return False
        
        return True
    
    def _check_network_access(self, context: AccessContext, trust_level: TrustLevel) -> AccessDecisionResult:
        """Check network-level access using micro-segmentation"""
        
        # Determine network zones
        source_zone = self.microsegmentation.classify_network_zone(context.source_ip)
        dest_zone = context.resource.zone
        
        # Extract port and protocol from context
        port = context.device_info.get("port", 443)
        protocol = context.device_info.get("protocol", "HTTPS")
        
        # Check network access
        network_allowed = self.microsegmentation.check_network_access(
            source_zone, dest_zone, port, protocol, trust_level
        )
        
        if not network_allowed:
            return AccessDecisionResult(
                decision=AccessDecision.DENY,
                confidence=0.95,
                reasons=[f"Network access denied: {source_zone.value} to {dest_zone.value}"],
                metadata={
                    "source_zone": source_zone.value,
                    "dest_zone": dest_zone.value,
                    "port": port,
                    "protocol": protocol
                }
            )
        
        return AccessDecisionResult(
            decision=AccessDecision.ALLOW,
            confidence=0.8,
            reasons=["Network access allowed"],
            metadata={"network_check": "passed"}
        )


class ContinuousVerificationEngine:
    """Continuous verification and monitoring engine"""
    
    def __init__(self, security_manager: SecurityEventManager):
        self.security_manager = security_manager
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.verification_intervals = {
            TrustLevel.VERIFIED: timedelta(hours=4),
            TrustLevel.HIGH: timedelta(hours=2),
            TrustLevel.MEDIUM: timedelta(hours=1),
            TrustLevel.LOW: timedelta(minutes=30),
            TrustLevel.UNTRUSTED: timedelta(minutes=5)
        }
    
    def start_session_monitoring(self, session_id: str, context: AccessContext) -> None:
        """Start monitoring a user session"""
        self.active_sessions[session_id] = {
            "context": context,
            "start_time": datetime.now(),
            "last_verification": datetime.now(),
            "activity_count": 0,
            "risk_score": 0.0,
            "anomalies": []
        }
    
    def update_session_activity(self, session_id: str, activity: Dict[str, Any]) -> None:
        """Update session with new activity"""
        if session_id not in self.active_sessions:
            return
        
        session = self.active_sessions[session_id]
        session["activity_count"] += 1
        session["last_activity"] = datetime.now()
        
        # Analyze for anomalies
        anomalies = self._detect_anomalies(session, activity)
        if anomalies:
            session["anomalies"].extend(anomalies)
            session["risk_score"] += len(anomalies) * 10
            
            # Report security events
            for anomaly in anomalies:
                self.security_manager.process_security_event(
                    event_type=SecurityEventType.ANOMALOUS_BEHAVIOR,
                    source_ip=session["context"].source_ip,
                    user_id=session["context"].identity.id,
                    details={"anomaly": anomaly, "session_id": session_id}
                )
    
    def requires_reverification(self, session_id: str, trust_level: TrustLevel) -> bool:
        """Check if session requires re-verification"""
        if session_id not in self.active_sessions:
            return True
        
        session = self.active_sessions[session_id]
        last_verification = session["last_verification"]
        interval = self.verification_intervals.get(trust_level, timedelta(minutes=30))
        
        return datetime.now() - last_verification > interval
    
    def _detect_anomalies(self, session: Dict[str, Any], activity: Dict[str, Any]) -> List[str]:
        """Detect behavioral anomalies in session"""
        anomalies = []
        
        # Check for unusual activity patterns
        if session["activity_count"] > 100:  # High activity
            anomalies.append("high_activity_rate")
        
        # Check for geo-location changes
        if "location" in activity:
            prev_location = session["context"].location_info.get("country")
            new_location = activity["location"].get("country")
            if prev_location and new_location and prev_location != new_location:
                anomalies.append("location_change")
        
        # Check for privilege escalation attempts
        if "privilege_change" in activity:
            anomalies.append("privilege_escalation_attempt")
        
        # Check for unusual resource access patterns
        if "resource_access" in activity:
            resource_type = activity["resource_access"].get("type")
            if resource_type in ["administrative", "sensitive_data"]:
                anomalies.append("sensitive_resource_access")
        
        return anomalies


class ZeroTrustOrchestrator:
    """Main orchestrator for Zero Trust security architecture"""
    
    def __init__(self, security_manager: SecurityEventManager):
        self.security_manager = security_manager
        self.policy_engine = PolicyEngine()
        self.continuous_verification = ContinuousVerificationEngine(security_manager)
        
        # Load default policies
        self._load_default_policies()
    
    def _load_default_policies(self) -> None:
        """Load default Zero Trust policies"""
        
        # Administrative access policy
        admin_policy = PolicyRule(
            id="admin-access-policy",
            name="Administrative Access Control",
            condition={
                "roles": ["admin", "superuser"],
                "resource_type": ["administrative", "management"]
            },
            action=AccessDecision.REQUIRE_MFA,
            priority=10
        )
        self.policy_engine.add_policy(admin_policy)
        
        # Sensitive data access policy
        sensitive_policy = PolicyRule(
            id="sensitive-data-policy",
            name="Sensitive Data Protection",
            condition={
                "resource_sensitivity": ["restricted", "top_secret"]
            },
            action=AccessDecision.REQUIRE_ADDITIONAL_AUTH,
            priority=20
        )
        self.policy_engine.add_policy(sensitive_policy)
        
        # Time-based access policy
        time_policy = PolicyRule(
            id="business-hours-policy",
            name="Business Hours Access",
            condition={
                "time_range": [9, 17],  # 9 AM to 5 PM
                "resource_sensitivity": ["confidential", "restricted"]
            },
            action=AccessDecision.ALLOW,
            priority=30
        )
        self.policy_engine.add_policy(time_policy)
        
        # Geographic restriction policy
        geo_policy = PolicyRule(
            id="geo-restriction-policy",
            name="Geographic Access Control",
            condition={
                "allowed_countries": ["US", "CA", "GB", "DE"],
                "resource_sensitivity": ["restricted", "top_secret"]
            },
            action=AccessDecision.DENY,
            priority=15
        )
        self.policy_engine.add_policy(geo_policy)
    
    def evaluate_access_request(self, request: Request, identity: Identity, 
                              resource: Resource) -> AccessDecisionResult:
        """Evaluate access request using Zero Trust principles"""
        
        # Extract context from request
        context = self._extract_context(request, identity, resource)
        
        # Add risk factors based on request analysis
        risk_factors = self._analyze_risk_factors(request, identity)
        context.risk_factors.extend(risk_factors)
        
        # Evaluate access using policy engine
        decision = self.policy_engine.evaluate_access(context)
        
        # Log access decision
        self._log_access_decision(context, decision)
        
        # Start continuous monitoring if access is granted
        if decision.decision == AccessDecision.ALLOW:
            session_id = str(uuid.uuid4())
            self.continuous_verification.start_session_monitoring(session_id, context)
            decision.metadata["session_id"] = session_id
        
        return decision
    
    def _extract_context(self, request: Request, identity: Identity, 
                        resource: Resource) -> AccessContext:
        """Extract access context from request"""
        
        # Get client IP
        client_ip = request.client.host if request.client else "unknown"
        x_forwarded_for = request.headers.get("x-forwarded-for")
        if x_forwarded_for:
            client_ip = x_forwarded_for.split(",")[0].strip()
        
        # Extract device information
        device_info = {
            "user_agent": request.headers.get("user-agent", ""),
            "accept_language": request.headers.get("accept-language", ""),
            "port": getattr(request.url, "port", 443),
            "protocol": request.url.scheme.upper() if request.url.scheme else "HTTPS"
        }
        
        # Extract location information (would typically come from GeoIP service)
        location_info = {
            "ip": client_ip,
            "country": "US",  # Placeholder - would use GeoIP
            "city": "Unknown",
            "timezone": "UTC"
        }
        
        return AccessContext(
            identity=identity,
            resource=resource,
            request_time=datetime.now(),
            source_ip=client_ip,
            device_info=device_info,
            location_info=location_info,
            session_info={"request_id": str(uuid.uuid4())}
        )
    
    def _analyze_risk_factors(self, request: Request, identity: Identity) -> List[str]:
        """Analyze request for risk factors"""
        risk_factors = []
        
        # Check for suspicious user agents
        user_agent = request.headers.get("user-agent", "").lower()
        suspicious_agents = ["curl", "wget", "python", "bot", "scanner", "crawler"]
        if any(agent in user_agent for agent in suspicious_agents):
            risk_factors.append("suspicious_user_agent")
        
        # Check for unusual request patterns
        if request.method in ["DELETE", "PUT", "PATCH"]:
            risk_factors.append("destructive_operation")
        
        # Check for admin-related paths
        path = str(request.url.path).lower()
        admin_paths = ["/admin", "/management", "/config", "/system"]
        if any(admin_path in path for admin_path in admin_paths):
            risk_factors.append("administrative_path")
        
        # Check identity risk factors
        if not identity.mfa_enabled:
            risk_factors.append("no_mfa")
        
        if not identity.device_compliance:
            risk_factors.append("non_compliant_device")
        
        if identity.last_verified:
            hours_since_auth = (datetime.now() - identity.last_verified).total_seconds() / 3600
            if hours_since_auth > 8:  # More than 8 hours
                risk_factors.append("stale_authentication")
        
        return risk_factors
    
    def _log_access_decision(self, context: AccessContext, decision: AccessDecisionResult) -> None:
        """Log access decision for audit and monitoring"""
        
        # Determine threat level based on decision
        if decision.decision == AccessDecision.DENY:
            threat_level = ThreatLevel.HIGH
        elif decision.decision in [AccessDecision.REQUIRE_MFA, AccessDecision.REQUIRE_ADDITIONAL_AUTH]:
            threat_level = ThreatLevel.MEDIUM
        elif decision.decision == AccessDecision.MONITOR:
            threat_level = ThreatLevel.LOW
        else:
            threat_level = ThreatLevel.INFO
        
        # Create security event
        event_details = {
            "identity_id": context.identity.id,
            "identity_type": context.identity.type,
            "resource_id": context.resource.id,
            "resource_type": context.resource.type,
            "resource_sensitivity": context.resource.sensitivity.value,
            "decision": decision.decision.value,
            "confidence": decision.confidence,
            "reasons": decision.reasons,
            "trust_score": context.identity.trust_score,
            "risk_factors": context.risk_factors,
            "required_actions": decision.required_actions,
            "monitoring_flags": decision.monitoring_flags
        }
        
        # Log through security manager
        logger.info(
            f"Zero Trust access decision: {decision.decision.value} for {context.identity.id} "
            f"accessing {context.resource.id} (confidence: {decision.confidence:.2f})",
            extra={
                "event_type": "zero_trust_access_decision",
                "user_id": context.identity.id,
                "resource_id": context.resource.id,
                "decision": decision.decision.value,
                "trust_score": context.identity.trust_score,
                "details": event_details
            }
        )
    
    def update_session_activity(self, session_id: str, activity: Dict[str, Any]) -> None:
        """Update session with new activity for continuous monitoring"""
        self.continuous_verification.update_session_activity(session_id, activity)
    
    def get_trust_dashboard(self) -> Dict[str, Any]:
        """Get Zero Trust dashboard data"""
        
        # Get active sessions count
        active_sessions = len(self.continuous_verification.active_sessions)
        
        # Get policy statistics
        total_policies = len(self.policy_engine.policies)
        enabled_policies = len([p for p in self.policy_engine.policies if p.enabled])
        
        # Calculate trust level distribution from cache
        trust_distribution = defaultdict(int)
        for cache_key, (decision, _) in self.policy_engine.decision_cache.items():
            trust_score = decision.metadata.get("trust_score", 0)
            trust_level = self.policy_engine.trust_calculator.get_trust_level(trust_score)
            trust_distribution[trust_level.value] += 1
        
        return {
            "active_sessions": active_sessions,
            "total_policies": total_policies,
            "enabled_policies": enabled_policies,
            "trust_level_distribution": dict(trust_distribution),
            "cached_decisions": len(self.policy_engine.decision_cache),
            "policy_engine_status": "active",
            "continuous_verification_status": "active",
            "microsegmentation_zones": len(self.policy_engine.microsegmentation.zone_definitions)
        }


# Global Zero Trust orchestrator instance
_zero_trust_orchestrator: Optional[ZeroTrustOrchestrator] = None


def get_zero_trust_orchestrator() -> ZeroTrustOrchestrator:
    """Get global Zero Trust orchestrator instance"""
    global _zero_trust_orchestrator
    if _zero_trust_orchestrator is None:
        from core.security.advanced_security import get_security_manager
        security_manager = get_security_manager()
        _zero_trust_orchestrator = ZeroTrustOrchestrator(security_manager)
    return _zero_trust_orchestrator
