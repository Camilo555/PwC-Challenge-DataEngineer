"""
Advanced Security Features: Audit Logging and Threat Detection
Provides comprehensive security monitoring, audit trails, and threat detection capabilities.
"""
import asyncio
import hashlib
import json
import re
import socket
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from ipaddress import AddressValueError, IPv4Address, IPv6Address
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
import threading
import uuid

from fastapi import Request
import geoip2.database
import geoip2.errors

from core.config.security_config import SecurityConfig
from core.logging import get_logger


class SecurityEventType(Enum):
    """Types of security events"""
    AUTHENTICATION_SUCCESS = "auth_success"
    AUTHENTICATION_FAILURE = "auth_failure"
    AUTHORIZATION_FAILURE = "authz_failure"
    SUSPICIOUS_LOGIN = "suspicious_login"
    BRUTE_FORCE_ATTEMPT = "brute_force"
    SQL_INJECTION_ATTEMPT = "sql_injection"
    XSS_ATTEMPT = "xss_attempt"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    DATA_ACCESS_VIOLATION = "data_access_violation"
    CONFIGURATION_CHANGE = "config_change"
    SYSTEM_COMPROMISE = "system_compromise"
    ANOMALOUS_BEHAVIOR = "anomalous_behavior"


class ThreatLevel(Enum):
    """Threat severity levels"""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ActionType(Enum):
    """Types of user/system actions"""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    LOGIN = "login"
    LOGOUT = "logout"
    DOWNLOAD = "download"
    UPLOAD = "upload"
    EXPORT = "export"
    IMPORT = "import"
    CONFIGURE = "configure"


@dataclass
class SecurityEvent:
    """Security event record"""
    event_id: str
    event_type: SecurityEventType
    threat_level: ThreatLevel
    timestamp: datetime
    source_ip: str
    user_id: Optional[str]
    session_id: Optional[str]
    user_agent: Optional[str]
    resource_accessed: Optional[str]
    action_performed: Optional[ActionType]
    success: bool
    details: Dict[str, Any]
    risk_score: float
    geolocation: Optional[Dict[str, str]] = None
    blocked: bool = False
    remediation_taken: Optional[str] = None


@dataclass
class AuditLogEntry:
    """Comprehensive audit log entry"""
    audit_id: str
    timestamp: datetime
    user_id: Optional[str]
    session_id: Optional[str]
    source_ip: str
    user_agent: Optional[str]
    action: ActionType
    resource_type: str
    resource_id: Optional[str]
    old_value: Optional[Dict[str, Any]]
    new_value: Optional[Dict[str, Any]]
    success: bool
    error_message: Optional[str]
    request_id: Optional[str]
    correlation_id: Optional[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ThreatIndicator:
    """Threat indicator for detection"""
    indicator_id: str
    indicator_type: str  # ip, user_agent, pattern, etc.
    indicator_value: str
    threat_level: ThreatLevel
    description: str
    created_at: datetime
    expires_at: Optional[datetime]
    source: str
    confidence_score: float


class GeoLocationService:
    """Geolocation service for IP addresses"""
    
    def __init__(self, geoip_db_path: Optional[Path] = None):
        self.geoip_db_path = geoip_db_path
        self.reader = None
        self.logger = get_logger(__name__)
        
        if self.geoip_db_path and self.geoip_db_path.exists():
            try:
                self.reader = geoip2.database.Reader(str(self.geoip_db_path))
            except Exception as e:
                self.logger.warning(f"Failed to load GeoIP database: {e}")
    
    def get_location(self, ip_address: str) -> Optional[Dict[str, str]]:
        """Get geolocation information for IP address"""
        if not self.reader:
            return None
        
        try:
            # Validate IP address
            try:
                ip_obj = IPv4Address(ip_address)
            except AddressValueError:
                try:
                    ip_obj = IPv6Address(ip_address)
                except AddressValueError:
                    return None
            
            # Skip private/local addresses
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                return {
                    'country': 'Local',
                    'city': 'Private Network',
                    'region': 'N/A',
                    'timezone': 'N/A'
                }
            
            response = self.reader.city(ip_address)
            
            return {
                'country': response.country.name or 'Unknown',
                'country_code': response.country.iso_code or 'XX',
                'city': response.city.name or 'Unknown',
                'region': response.subdivisions.most_specific.name or 'Unknown',
                'timezone': response.location.time_zone or 'Unknown',
                'latitude': float(response.location.latitude) if response.location.latitude else 0.0,
                'longitude': float(response.location.longitude) if response.location.longitude else 0.0
            }
            
        except (geoip2.errors.AddressNotFoundError, Exception) as e:
            self.logger.debug(f"Geolocation lookup failed for {ip_address}: {e}")
            return None
    
    def __del__(self):
        if self.reader:
            self.reader.close()


class ThreatDetectionEngine:
    """Advanced threat detection and analysis"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.threat_indicators: Dict[str, ThreatIndicator] = {}
        self.ip_reputation_cache: Dict[str, Tuple[float, datetime]] = {}
        self.behavioral_profiles: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        
        # Load threat indicators
        self._load_threat_indicators()
        
        # Behavioral analysis windows
        self.login_attempts: Dict[str, List[datetime]] = defaultdict(list)
        self.failed_attempts: Dict[str, List[datetime]] = defaultdict(list)
        self.request_patterns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    def _load_threat_indicators(self):
        """Load threat indicators from various sources"""
        # Common SQL injection patterns
        sql_injection_patterns = [
            r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC)\b)",
            r"(\b(UNION|OR|AND)\s+\d+\s*=\s*\d+)",
            r"('|\"|;|--|\|\/\*)",
            r"(\bxp_cmdshell\b|\bsp_executesql\b)"
        ]
        
        for i, pattern in enumerate(sql_injection_patterns):
            self.threat_indicators[f"sql_injection_{i}"] = ThreatIndicator(
                indicator_id=f"sql_injection_{i}",
                indicator_type="regex_pattern",
                indicator_value=pattern,
                threat_level=ThreatLevel.HIGH,
                description=f"SQL injection pattern {i+1}",
                created_at=datetime.now(),
                expires_at=None,
                source="built_in",
                confidence_score=0.9
            )
        
        # XSS patterns
        xss_patterns = [
            r"(<script[^>]*>.*?</script>)",
            r"(javascript:|vbscript:|onload=|onerror=)",
            r"(<iframe[^>]*>|<object[^>]*>|<embed[^>]*>)",
            r"(eval\s*\(|setTimeout\s*\(|setInterval\s*\()"
        ]
        
        for i, pattern in enumerate(xss_patterns):
            self.threat_indicators[f"xss_{i}"] = ThreatIndicator(
                indicator_id=f"xss_{i}",
                indicator_type="regex_pattern",
                indicator_value=pattern,
                threat_level=ThreatLevel.MEDIUM,
                description=f"XSS pattern {i+1}",
                created_at=datetime.now(),
                expires_at=None,
                source="built_in",
                confidence_score=0.8
            )
        
        # Suspicious user agents
        suspicious_agents = [
            "sqlmap", "havij", "nmap", "nikto", "burpsuite",
            "w3af", "acunetix", "nessus", "openvas"
        ]
        
        for agent in suspicious_agents:
            self.threat_indicators[f"ua_{agent}"] = ThreatIndicator(
                indicator_id=f"ua_{agent}",
                indicator_type="user_agent",
                indicator_value=agent.lower(),
                threat_level=ThreatLevel.HIGH,
                description=f"Suspicious user agent: {agent}",
                created_at=datetime.now(),
                expires_at=None,
                source="built_in",
                confidence_score=0.95
            )
    
    def analyze_request(
        self, 
        request_data: Dict[str, Any],
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[SecurityEvent]:
        """Analyze incoming request for threats"""
        
        events = []
        source_ip = request_data.get('source_ip', 'unknown')
        user_agent = request_data.get('user_agent', '')
        url_path = request_data.get('path', '')
        query_params = request_data.get('query_params', {})
        body = request_data.get('body', '')
        
        # Check for SQL injection
        sql_events = self._detect_sql_injection(
            source_ip, user_id, session_id, user_agent, 
            url_path, query_params, body
        )
        events.extend(sql_events)
        
        # Check for XSS
        xss_events = self._detect_xss(
            source_ip, user_id, session_id, user_agent, 
            url_path, query_params, body
        )
        events.extend(xss_events)
        
        # Check user agent
        ua_events = self._detect_suspicious_user_agent(
            source_ip, user_id, session_id, user_agent
        )
        events.extend(ua_events)
        
        # Behavioral analysis
        behavioral_events = self._analyze_behavioral_patterns(
            source_ip, user_id, session_id, request_data
        )
        events.extend(behavioral_events)
        
        return events
    
    def _detect_sql_injection(
        self, 
        source_ip: str, 
        user_id: Optional[str],
        session_id: Optional[str],
        user_agent: str,
        url_path: str, 
        query_params: Dict[str, Any], 
        body: str
    ) -> List[SecurityEvent]:
        """Detect SQL injection attempts"""
        
        events = []
        
        # Combine all input sources
        all_input = f"{url_path} {json.dumps(query_params)} {body}"
        
        for indicator_id, indicator in self.threat_indicators.items():
            if indicator.indicator_type == "regex_pattern" and "sql_injection" in indicator_id:
                if re.search(indicator.indicator_value, all_input, re.IGNORECASE):
                    event = SecurityEvent(
                        event_id=str(uuid.uuid4()),
                        event_type=SecurityEventType.SQL_INJECTION_ATTEMPT,
                        threat_level=indicator.threat_level,
                        timestamp=datetime.now(),
                        source_ip=source_ip,
                        user_id=user_id,
                        session_id=session_id,
                        user_agent=user_agent,
                        resource_accessed=url_path,
                        action_performed=ActionType.READ,
                        success=False,
                        details={
                            'pattern_matched': indicator.indicator_value,
                            'input_analyzed': all_input[:500],  # Truncate for storage
                            'indicator_id': indicator_id
                        },
                        risk_score=indicator.confidence_score * 10,
                        blocked=True
                    )
                    events.append(event)
        
        return events
    
    def _detect_xss(
        self, 
        source_ip: str, 
        user_id: Optional[str],
        session_id: Optional[str],
        user_agent: str,
        url_path: str, 
        query_params: Dict[str, Any], 
        body: str
    ) -> List[SecurityEvent]:
        """Detect XSS attempts"""
        
        events = []
        
        # Combine all input sources
        all_input = f"{url_path} {json.dumps(query_params)} {body}"
        
        for indicator_id, indicator in self.threat_indicators.items():
            if indicator.indicator_type == "regex_pattern" and "xss" in indicator_id:
                if re.search(indicator.indicator_value, all_input, re.IGNORECASE):
                    event = SecurityEvent(
                        event_id=str(uuid.uuid4()),
                        event_type=SecurityEventType.XSS_ATTEMPT,
                        threat_level=indicator.threat_level,
                        timestamp=datetime.now(),
                        source_ip=source_ip,
                        user_id=user_id,
                        session_id=session_id,
                        user_agent=user_agent,
                        resource_accessed=url_path,
                        action_performed=ActionType.READ,
                        success=False,
                        details={
                            'pattern_matched': indicator.indicator_value,
                            'input_analyzed': all_input[:500],
                            'indicator_id': indicator_id
                        },
                        risk_score=indicator.confidence_score * 8,
                        blocked=True
                    )
                    events.append(event)
        
        return events
    
    def _detect_suspicious_user_agent(
        self, 
        source_ip: str, 
        user_id: Optional[str],
        session_id: Optional[str],
        user_agent: str
    ) -> List[SecurityEvent]:
        """Detect suspicious user agents"""
        
        events = []
        
        for indicator_id, indicator in self.threat_indicators.items():
            if indicator.indicator_type == "user_agent":
                if indicator.indicator_value in user_agent.lower():
                    event = SecurityEvent(
                        event_id=str(uuid.uuid4()),
                        event_type=SecurityEventType.SUSPICIOUS_LOGIN,
                        threat_level=indicator.threat_level,
                        timestamp=datetime.now(),
                        source_ip=source_ip,
                        user_id=user_id,
                        session_id=session_id,
                        user_agent=user_agent,
                        resource_accessed=None,
                        action_performed=ActionType.LOGIN,
                        success=False,
                        details={
                            'suspicious_agent': indicator.indicator_value,
                            'full_user_agent': user_agent,
                            'indicator_id': indicator_id
                        },
                        risk_score=indicator.confidence_score * 9,
                        blocked=True
                    )
                    events.append(event)
        
        return events
    
    def _analyze_behavioral_patterns(
        self, 
        source_ip: str, 
        user_id: Optional[str],
        session_id: Optional[str],
        request_data: Dict[str, Any]
    ) -> List[SecurityEvent]:
        """Analyze behavioral patterns for anomalies"""
        
        events = []
        now = datetime.now()
        
        # Track request patterns
        with self.lock:
            # Store request pattern
            pattern = {
                'timestamp': now,
                'path': request_data.get('path'),
                'method': request_data.get('method'),
                'user_agent': request_data.get('user_agent'),
                'size': len(str(request_data))
            }
            
            self.request_patterns[source_ip].append(pattern)
            
            # Keep only last hour of data
            cutoff_time = now - timedelta(hours=1)
            self.request_patterns[source_ip] = [
                p for p in self.request_patterns[source_ip] 
                if p['timestamp'] >= cutoff_time
            ]
        
        # Analyze patterns
        recent_requests = self.request_patterns.get(source_ip, [])
        
        # Check for excessive requests (potential DoS)
        if len(recent_requests) > 1000:  # More than 1000 requests per hour
            event = SecurityEvent(
                event_id=str(uuid.uuid4()),
                event_type=SecurityEventType.RATE_LIMIT_EXCEEDED,
                threat_level=ThreatLevel.MEDIUM,
                timestamp=now,
                source_ip=source_ip,
                user_id=user_id,
                session_id=session_id,
                user_agent=request_data.get('user_agent'),
                resource_accessed=None,
                action_performed=ActionType.READ,
                success=False,
                details={
                    'requests_per_hour': len(recent_requests),
                    'threshold_exceeded': True
                },
                risk_score=6.0,
                blocked=False
            )
            events.append(event)
        
        # Check for scanning behavior (many different paths)
        if len(recent_requests) > 50:
            unique_paths = len(set(r['path'] for r in recent_requests))
            path_diversity = unique_paths / len(recent_requests)
            
            if path_diversity > 0.8:  # High path diversity suggests scanning
                event = SecurityEvent(
                    event_id=str(uuid.uuid4()),
                    event_type=SecurityEventType.ANOMALOUS_BEHAVIOR,
                    threat_level=ThreatLevel.MEDIUM,
                    timestamp=now,
                    source_ip=source_ip,
                    user_id=user_id,
                    session_id=session_id,
                    user_agent=request_data.get('user_agent'),
                    resource_accessed=None,
                    action_performed=ActionType.READ,
                    success=False,
                    details={
                        'total_requests': len(recent_requests),
                        'unique_paths': unique_paths,
                        'path_diversity': path_diversity,
                        'scanning_detected': True
                    },
                    risk_score=7.0,
                    blocked=False
                )
                events.append(event)
        
        return events
    
    def analyze_login_attempt(
        self, 
        source_ip: str,
        user_id: Optional[str],
        success: bool,
        user_agent: Optional[str] = None
    ) -> List[SecurityEvent]:
        """Analyze login attempts for suspicious patterns"""
        
        events = []
        now = datetime.now()
        
        with self.lock:
            if not success:
                self.failed_attempts[source_ip].append(now)
                if user_id:
                    self.failed_attempts[f"user_{user_id}"].append(now)
            
            self.login_attempts[source_ip].append(now)
            if user_id:
                self.login_attempts[f"user_{user_id}"].append(now)
            
            # Clean old attempts (keep last 24 hours)
            cutoff_time = now - timedelta(hours=24)
            for key in list(self.failed_attempts.keys()):
                self.failed_attempts[key] = [
                    attempt for attempt in self.failed_attempts[key]
                    if attempt >= cutoff_time
                ]
            
            for key in list(self.login_attempts.keys()):
                self.login_attempts[key] = [
                    attempt for attempt in self.login_attempts[key]
                    if attempt >= cutoff_time
                ]
        
        # Check for brute force attempts
        recent_failures = len(self.failed_attempts.get(source_ip, []))
        recent_user_failures = len(self.failed_attempts.get(f"user_{user_id}", [])) if user_id else 0
        
        if recent_failures >= 10 or recent_user_failures >= 5:
            event = SecurityEvent(
                event_id=str(uuid.uuid4()),
                event_type=SecurityEventType.BRUTE_FORCE_ATTEMPT,
                threat_level=ThreatLevel.HIGH,
                timestamp=now,
                source_ip=source_ip,
                user_id=user_id,
                session_id=None,
                user_agent=user_agent,
                resource_accessed="/login",
                action_performed=ActionType.LOGIN,
                success=success,
                details={
                    'failed_attempts_ip': recent_failures,
                    'failed_attempts_user': recent_user_failures,
                    'time_window': '24 hours'
                },
                risk_score=8.0,
                blocked=True
            )
            events.append(event)
        
        return events


class AuditLogger:
    """Comprehensive audit logging system"""
    
    def __init__(self, log_file_path: Optional[Path] = None):
        self.log_file_path = log_file_path or Path("audit_logs.jsonl")
        self.logger = get_logger(__name__)
        self.audit_buffer: deque = deque(maxlen=10000)  # Buffer recent entries
        self.lock = threading.Lock()
        
        # Ensure log directory exists
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log_audit_event(
        self,
        user_id: Optional[str],
        action: ActionType,
        resource_type: str,
        resource_id: Optional[str] = None,
        source_ip: str = "unknown",
        session_id: Optional[str] = None,
        user_agent: Optional[str] = None,
        old_value: Optional[Dict[str, Any]] = None,
        new_value: Optional[Dict[str, Any]] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        request_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        metadata: Dict[str, Any] = None
    ) -> str:
        """Log an audit event"""
        
        audit_entry = AuditLogEntry(
            audit_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            user_id=user_id,
            session_id=session_id,
            source_ip=source_ip,
            user_agent=user_agent,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            old_value=old_value,
            new_value=new_value,
            success=success,
            error_message=error_message,
            request_id=request_id,
            correlation_id=correlation_id,
            metadata=metadata or {}
        )
        
        # Add to buffer
        with self.lock:
            self.audit_buffer.append(audit_entry)
        
        # Write to file
        self._write_audit_entry(audit_entry)
        
        return audit_entry.audit_id
    
    def _write_audit_entry(self, entry: AuditLogEntry):
        """Write audit entry to file"""
        try:
            # Convert to JSON
            entry_dict = {
                'audit_id': entry.audit_id,
                'timestamp': entry.timestamp.isoformat(),
                'user_id': entry.user_id,
                'session_id': entry.session_id,
                'source_ip': entry.source_ip,
                'user_agent': entry.user_agent,
                'action': entry.action.value,
                'resource_type': entry.resource_type,
                'resource_id': entry.resource_id,
                'old_value': entry.old_value,
                'new_value': entry.new_value,
                'success': entry.success,
                'error_message': entry.error_message,
                'request_id': entry.request_id,
                'correlation_id': entry.correlation_id,
                'metadata': entry.metadata
            }
            
            # Write to file (JSONL format)
            with open(self.log_file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry_dict) + '\n')
                
        except Exception as e:
            self.logger.error(f"Failed to write audit entry: {e}")
    
    def search_audit_logs(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user_id: Optional[str] = None,
        action: Optional[ActionType] = None,
        resource_type: Optional[str] = None,
        source_ip: Optional[str] = None,
        limit: int = 100
    ) -> List[AuditLogEntry]:
        """Search audit logs with filters"""
        
        results = []
        
        with self.lock:
            for entry in reversed(self.audit_buffer):
                # Apply filters
                if start_time and entry.timestamp < start_time:
                    continue
                if end_time and entry.timestamp > end_time:
                    continue
                if user_id and entry.user_id != user_id:
                    continue
                if action and entry.action != action:
                    continue
                if resource_type and entry.resource_type != resource_type:
                    continue
                if source_ip and entry.source_ip != source_ip:
                    continue
                
                results.append(entry)
                
                if len(results) >= limit:
                    break
        
        return results
    
    def get_audit_statistics(self) -> Dict[str, Any]:
        """Get audit log statistics"""
        
        with self.lock:
            entries = list(self.audit_buffer)
        
        if not entries:
            return {}
        
        # Calculate statistics
        total_entries = len(entries)
        success_rate = sum(1 for e in entries if e.success) / total_entries
        
        # Action distribution
        action_counts = defaultdict(int)
        for entry in entries:
            action_counts[entry.action.value] += 1
        
        # User activity
        user_activity = defaultdict(int)
        for entry in entries:
            if entry.user_id:
                user_activity[entry.user_id] += 1
        
        # Resource access
        resource_access = defaultdict(int)
        for entry in entries:
            resource_access[entry.resource_type] += 1
        
        # Time range
        timestamps = [e.timestamp for e in entries]
        time_range = {
            'earliest': min(timestamps).isoformat(),
            'latest': max(timestamps).isoformat()
        }
        
        return {
            'total_entries': total_entries,
            'success_rate': success_rate,
            'time_range': time_range,
            'action_distribution': dict(action_counts),
            'top_users': dict(sorted(user_activity.items(), key=lambda x: x[1], reverse=True)[:10]),
            'resource_access': dict(resource_access)
        }


class SecurityEventManager:
    """Centralized security event management"""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.logger = get_logger(__name__)
        self.threat_detector = ThreatDetectionEngine()
        self.audit_logger = AuditLogger()
        self.geolocation = GeoLocationService()
        
        self.security_events: deque = deque(maxlen=50000)
        self.active_threats: Dict[str, List[SecurityEvent]] = defaultdict(list)
        self.blocked_ips: Set[str] = set()
        self.lock = threading.Lock()
        
        # Start background threat analysis
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background tasks for threat analysis"""
        def cleanup_task():
            while True:
                try:
                    self._cleanup_old_events()
                    self._update_threat_status()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    self.logger.error(f"Background task error: {e}")
        
        import threading
        cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        cleanup_thread.start()
    
    def process_request(
        self, 
        request: Request,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[SecurityEvent]:
        """Process incoming request for security threats"""
        
        # Extract request data
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "")
        
        request_data = {
            'source_ip': client_ip,
            'user_agent': user_agent,
            'method': request.method,
            'path': str(request.url.path),
            'query_params': dict(request.query_params),
            'headers': dict(request.headers)
        }
        
        # Analyze for threats
        events = self.threat_detector.analyze_request(request_data, user_id, session_id)
        
        # Add geolocation data
        for event in events:
            if event.source_ip and event.source_ip != "unknown":
                event.geolocation = self.geolocation.get_location(event.source_ip)
        
        # Store events
        with self.lock:
            for event in events:
                self.security_events.append(event)
                
                if event.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                    self.active_threats[event.source_ip].append(event)
                    
                    if event.blocked:
                        self.blocked_ips.add(event.source_ip)
        
        # Log high-severity events
        for event in events:
            if event.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                self.logger.warning(f"Security threat detected: {event.event_type.value} from {event.source_ip}")
        
        return events
    
    def log_authentication_event(
        self,
        user_id: str,
        success: bool,
        source_ip: str,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Dict[str, Any] = None
    ) -> List[SecurityEvent]:
        """Log authentication event and analyze for threats"""
        
        # Create audit log entry
        self.audit_logger.log_audit_event(
            user_id=user_id,
            action=ActionType.LOGIN,
            resource_type="authentication",
            source_ip=source_ip,
            session_id=session_id,
            user_agent=user_agent,
            success=success,
            metadata=details or {}
        )
        
        # Analyze for threats
        events = self.threat_detector.analyze_login_attempt(
            source_ip=source_ip,
            user_id=user_id,
            success=success,
            user_agent=user_agent
        )
        
        # Add geolocation
        for event in events:
            event.geolocation = self.geolocation.get_location(source_ip)
        
        # Store events
        with self.lock:
            for event in events:
                self.security_events.append(event)
                
                if event.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                    self.active_threats[source_ip].append(event)
                    
                    if event.blocked:
                        self.blocked_ips.add(source_ip)
        
        return events
    
    def is_ip_blocked(self, ip_address: str) -> bool:
        """Check if IP address is blocked"""
        with self.lock:
            return ip_address in self.blocked_ips
    
    def unblock_ip(self, ip_address: str, reason: str = "Manual unblock") -> bool:
        """Unblock an IP address"""
        with self.lock:
            if ip_address in self.blocked_ips:
                self.blocked_ips.remove(ip_address)
                
                # Log the unblock action
                self.audit_logger.log_audit_event(
                    user_id="system",
                    action=ActionType.UPDATE,
                    resource_type="ip_blocklist",
                    resource_id=ip_address,
                    old_value={"blocked": True},
                    new_value={"blocked": False},
                    metadata={"reason": reason}
                )
                
                return True
        
        return False
    
    def get_security_dashboard(self) -> Dict[str, Any]:
        """Get security dashboard data"""
        
        with self.lock:
            events = list(self.security_events)
            active_threats = dict(self.active_threats)
            blocked_ips = set(self.blocked_ips)
        
        if not events:
            return {'message': 'No security events recorded'}
        
        # Calculate statistics
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        recent_events = [e for e in events if e.timestamp >= last_24h]
        
        # Threat level distribution
        threat_levels = defaultdict(int)
        for event in recent_events:
            threat_levels[event.threat_level.value] += 1
        
        # Event type distribution
        event_types = defaultdict(int)
        for event in recent_events:
            event_types[event.event_type.value] += 1
        
        # Top source IPs
        source_ips = defaultdict(int)
        for event in recent_events:
            source_ips[event.source_ip] += 1
        
        return {
            'total_events_24h': len(recent_events),
            'active_threats': len(active_threats),
            'blocked_ips': len(blocked_ips),
            'threat_level_distribution': dict(threat_levels),
            'event_type_distribution': dict(event_types),
            'top_source_ips': dict(sorted(source_ips.items(), key=lambda x: x[1], reverse=True)[:10]),
            'recent_high_severity_events': [
                {
                    'event_id': e.event_id,
                    'type': e.event_type.value,
                    'threat_level': e.threat_level.value,
                    'source_ip': e.source_ip,
                    'timestamp': e.timestamp.isoformat(),
                    'blocked': e.blocked
                }
                for e in recent_events
                if e.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]
            ][:20]
        }
    
    def _cleanup_old_events(self):
        """Clean up old security events"""
        cutoff_time = datetime.now() - timedelta(days=7)
        
        with self.lock:
            # Clean security events (deque handles this automatically with maxlen)
            
            # Clean active threats
            for ip in list(self.active_threats.keys()):
                self.active_threats[ip] = [
                    event for event in self.active_threats[ip]
                    if event.timestamp >= cutoff_time
                ]
                if not self.active_threats[ip]:
                    del self.active_threats[ip]
    
    def _update_threat_status(self):
        """Update threat status and auto-unblock IPs if appropriate"""
        now = datetime.now()
        auto_unblock_time = now - timedelta(hours=24)  # Auto-unblock after 24 hours
        
        with self.lock:
            # Check for IPs to auto-unblock
            ips_to_unblock = set()
            
            for ip in self.blocked_ips:
                recent_threats = self.active_threats.get(ip, [])
                if recent_threats:
                    latest_threat = max(recent_threats, key=lambda x: x.timestamp)
                    if latest_threat.timestamp < auto_unblock_time:
                        ips_to_unblock.add(ip)
            
            # Auto-unblock IPs
            for ip in ips_to_unblock:
                self.blocked_ips.remove(ip)
                self.logger.info(f"Auto-unblocked IP {ip} after 24 hours")


# Global security event manager instance
_security_manager: Optional[SecurityEventManager] = None


def get_security_manager() -> SecurityEventManager:
    """Get global security event manager instance"""
    global _security_manager
    if _security_manager is None:
        from core.config.unified_config import get_unified_config
        config = get_unified_config()
        _security_manager = SecurityEventManager(config.security)
    return _security_manager


# Decorator for automatic audit logging
def audit_action(
    action: ActionType,
    resource_type: str,
    extract_resource_id: Callable = None,
    log_args: bool = False,
    log_result: bool = False
):
    """Decorator for automatic audit logging"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Extract context information
            user_id = kwargs.get('user_id') or getattr(args[0], 'user_id', None) if args else None
            resource_id = extract_resource_id(*args, **kwargs) if extract_resource_id else None
            
            # Store old value for updates/deletes
            old_value = None
            if action in [ActionType.UPDATE, ActionType.DELETE] and log_args:
                old_value = {'args': str(args)[:500], 'kwargs': str(kwargs)[:500]}
            
            start_time = time.time()
            success = True
            error_message = None
            result = None
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error_message = str(e)
                raise
            finally:
                # Log audit event
                audit_logger = get_security_manager().audit_logger
                
                new_value = None
                if log_result and result:
                    new_value = {'result': str(result)[:500]}
                elif log_args:
                    new_value = {'args': str(args)[:500], 'kwargs': str(kwargs)[:500]}
                
                audit_logger.log_audit_event(
                    user_id=user_id,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    old_value=old_value,
                    new_value=new_value,
                    success=success,
                    error_message=error_message,
                    metadata={
                        'function': func.__name__,
                        'execution_time_ms': (time.time() - start_time) * 1000
                    }
                )
        
        return wrapper
    return decorator