"""
Comprehensive Audit Trail System
================================

Enterprise-grade audit trail system with change tracking, user attribution,
and compliance features for data governance and regulatory requirements.

Features:
- Complete change tracking for all database operations
- User attribution and session correlation
- Automatic data classification and sensitivity detection
- Compliance reporting (GDPR, SOX, PCI-DSS)
- Real-time audit event streaming
- Advanced forensic analysis capabilities
- Data retention and archival policies
"""
from __future__ import annotations

import asyncio
import json
import hashlib
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from sqlalchemy import text, event, inspect
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class AuditEventType(str, Enum):
    """Types of audit events."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    EXPORT = "export"
    IMPORT = "import"
    CONFIGURATION_CHANGE = "configuration_change"
    SECURITY_EVENT = "security_event"
    COMPLIANCE_EVENT = "compliance_event"


class DataClassification(str, Enum):
    """Data sensitivity classifications."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"  # Personally Identifiable Information
    PHI = "phi"  # Protected Health Information
    PCI = "pci"  # Payment Card Industry data


class AuditStatus(str, Enum):
    """Audit event processing status."""
    PENDING = "pending"
    PROCESSED = "processed"
    ARCHIVED = "archived"
    FAILED = "failed"


@dataclass
class AuditEvent:
    """Comprehensive audit event record."""
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: AuditEventType = AuditEventType.READ
    timestamp: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

    # Resource information
    resource_type: str = ""  # table, api_endpoint, file, etc.
    resource_id: Optional[str] = None
    resource_name: Optional[str] = None

    # Change tracking
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    changed_fields: List[str] = field(default_factory=list)

    # Context and metadata
    operation: str = ""  # SQL operation, API method, etc.
    affected_rows: int = 0
    data_classification: DataClassification = DataClassification.INTERNAL
    compliance_flags: List[str] = field(default_factory=list)

    # Request context
    request_id: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    status_code: Optional[int] = None

    # Security context
    risk_score: float = 0.0
    sensitive_data_accessed: bool = False
    geo_location: Optional[str] = None

    # Processing metadata
    status: AuditStatus = AuditStatus.PENDING
    checksum: Optional[str] = None
    retention_date: Optional[datetime] = None


@dataclass
class AuditConfiguration:
    """Configuration for audit trail system."""
    enable_database_auditing: bool = True
    enable_api_auditing: bool = True
    enable_file_auditing: bool = True
    enable_real_time_streaming: bool = True

    # Data classification rules
    pii_patterns: List[str] = field(default_factory=lambda: [
        r'\b\d{3}-\d{2}-\d{4}\b',  # SSN
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Email
        r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'  # Credit card
    ])

    # Retention policies
    default_retention_days: int = 2555  # 7 years
    pii_retention_days: int = 1095  # 3 years
    security_event_retention_days: int = 3650  # 10 years

    # Compliance settings
    enable_gdpr_compliance: bool = True
    enable_sox_compliance: bool = True
    enable_pci_compliance: bool = True

    # Performance settings
    batch_size: int = 1000
    flush_interval_seconds: int = 30
    max_memory_events: int = 10000


class ComprehensiveAuditTrailManager:
    """
    Enterprise audit trail manager with advanced tracking capabilities.

    Provides comprehensive audit logging, change tracking, and compliance
    features for enterprise data governance requirements.
    """

    def __init__(self, engine: AsyncEngine, config: Optional[AuditConfiguration] = None):
        self.engine = engine
        self.config = config or AuditConfiguration()

        # Event storage and processing
        self.pending_events: List[AuditEvent] = []
        self.event_buffer_lock = asyncio.Lock()
        self.processing_task: Optional[asyncio.Task] = None

        # Session tracking
        self.active_sessions: Dict[str, Dict[str, Any]] = {}

        # Data classification cache
        self.classification_cache: Dict[str, DataClassification] = {}

        # Metrics and monitoring
        self.metrics = {
            "events_processed": 0,
            "events_failed": 0,
            "compliance_violations": 0,
            "pii_access_events": 0,
            "security_events": 0,
            "last_processing_time": None
        }

    async def initialize(self):
        """Initialize the audit trail system."""
        try:
            # Create audit schema and tables
            await self._create_audit_schema()

            # Load data classification rules
            await self._load_classification_rules()

            # Start background processing
            if self.config.enable_real_time_streaming:
                self.processing_task = asyncio.create_task(self._event_processing_loop())

            # Register database event listeners
            if self.config.enable_database_auditing:
                await self._register_database_listeners()

            logger.info("Comprehensive audit trail system initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize audit trail system: {e}")
            raise

    async def _create_audit_schema(self):
        """Create comprehensive audit schema."""
        schema_sql = """
        -- Main audit events table
        CREATE TABLE IF NOT EXISTS audit_events (
            event_id VARCHAR(36) PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            ip_address INET,
            user_agent TEXT,

            -- Resource information
            resource_type VARCHAR(100) NOT NULL,
            resource_id VARCHAR(255),
            resource_name VARCHAR(255),

            -- Change tracking
            old_values JSONB,
            new_values JSONB,
            changed_fields TEXT[],

            -- Context and metadata
            operation VARCHAR(100) NOT NULL,
            affected_rows INTEGER DEFAULT 0,
            data_classification VARCHAR(50) DEFAULT 'internal',
            compliance_flags TEXT[] DEFAULT '{}',

            -- Request context
            request_id VARCHAR(255),
            endpoint VARCHAR(255),
            method VARCHAR(20),
            status_code INTEGER,

            -- Security context
            risk_score DECIMAL(3,2) DEFAULT 0.0,
            sensitive_data_accessed BOOLEAN DEFAULT FALSE,
            geo_location VARCHAR(255),

            -- Processing metadata
            status VARCHAR(20) DEFAULT 'pending',
            checksum VARCHAR(64),
            retention_date DATE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP
        );

        -- User sessions tracking
        CREATE TABLE IF NOT EXISTS audit_user_sessions (
            session_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            ip_address INET,
            user_agent TEXT,
            login_time TIMESTAMP NOT NULL,
            last_activity TIMESTAMP NOT NULL,
            logout_time TIMESTAMP,
            session_duration_minutes INTEGER,
            events_count INTEGER DEFAULT 0,
            risk_events_count INTEGER DEFAULT 0,
            geo_location VARCHAR(255),
            device_fingerprint VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Data access patterns
        CREATE TABLE IF NOT EXISTS audit_data_access_patterns (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            resource_type VARCHAR(100) NOT NULL,
            access_pattern VARCHAR(50), -- normal, unusual, suspicious
            frequency_score DECIMAL(5,2),
            time_pattern_score DECIMAL(5,2),
            volume_score DECIMAL(5,2),
            risk_score DECIMAL(5,2),
            first_access TIMESTAMP NOT NULL,
            last_access TIMESTAMP NOT NULL,
            access_count INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Compliance violations
        CREATE TABLE IF NOT EXISTS audit_compliance_violations (
            violation_id VARCHAR(36) PRIMARY KEY,
            event_id VARCHAR(36) REFERENCES audit_events(event_id),
            compliance_type VARCHAR(50) NOT NULL, -- GDPR, SOX, PCI, etc.
            violation_type VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL, -- low, medium, high, critical
            description TEXT NOT NULL,
            affected_data_types TEXT[],
            remediation_required BOOLEAN DEFAULT TRUE,
            remediation_status VARCHAR(50) DEFAULT 'pending',
            remediation_notes TEXT,
            reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP
        );

        -- Audit configuration and rules
        CREATE TABLE IF NOT EXISTS audit_configuration (
            config_key VARCHAR(100) PRIMARY KEY,
            config_value JSONB NOT NULL,
            config_type VARCHAR(50) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create comprehensive indexes
        CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_events_user_id ON audit_events(user_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON audit_events(resource_type, resource_id);
        CREATE INDEX IF NOT EXISTS idx_audit_events_event_type ON audit_events(event_type, timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_events_session ON audit_events(session_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_events_compliance ON audit_events(data_classification, compliance_flags);
        CREATE INDEX IF NOT EXISTS idx_audit_events_risk ON audit_events(risk_score DESC, timestamp);

        CREATE INDEX IF NOT EXISTS idx_audit_sessions_user ON audit_user_sessions(user_id, login_time);
        CREATE INDEX IF NOT EXISTS idx_audit_sessions_activity ON audit_user_sessions(last_activity);

        CREATE INDEX IF NOT EXISTS idx_audit_patterns_user_resource ON audit_data_access_patterns(user_id, resource_type);
        CREATE INDEX IF NOT EXISTS idx_audit_patterns_risk ON audit_data_access_patterns(risk_score DESC);

        CREATE INDEX IF NOT EXISTS idx_audit_violations_type ON audit_compliance_violations(compliance_type, severity);
        CREATE INDEX IF NOT EXISTS idx_audit_violations_status ON audit_compliance_violations(remediation_status, reported_at);

        -- Create partitioning for audit_events (monthly partitions)
        SELECT create_range_partition('audit_events', 'timestamp', 'monthly');
        """

        async with self.engine.begin() as conn:
            await conn.execute(text(schema_sql))

    async def _load_classification_rules(self):
        """Load data classification rules and patterns."""
        try:
            # Load from database if available
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT config_key, config_value
                    FROM audit_configuration
                    WHERE config_type = 'data_classification'
                """))

                for row in result:
                    key = row[0]
                    value = json.loads(row[1]) if isinstance(row[1], str) else row[1]

                    if key == 'pii_patterns':
                        self.config.pii_patterns.extend(value)
                    elif key == 'classification_mappings':
                        self.classification_cache.update(value)

        except Exception as e:
            logger.warning(f"Could not load classification rules from database: {e}")

        # Set default classifications
        default_classifications = {
            'users': DataClassification.PII,
            'customers': DataClassification.PII,
            'transactions': DataClassification.CONFIDENTIAL,
            'payments': DataClassification.PCI,
            'sales': DataClassification.CONFIDENTIAL,
            'products': DataClassification.INTERNAL,
            'public_data': DataClassification.PUBLIC
        }

        for table, classification in default_classifications.items():
            if table not in self.classification_cache:
                self.classification_cache[table] = classification

    async def log_event(self, event: AuditEvent):
        """Log a comprehensive audit event."""
        try:
            # Enrich event with additional context
            await self._enrich_audit_event(event)

            # Classify data sensitivity
            event.data_classification = self._classify_data(event.resource_type, event.new_values or event.old_values)

            # Calculate risk score
            event.risk_score = await self._calculate_risk_score(event)

            # Check compliance violations
            violations = await self._check_compliance_violations(event)
            if violations:
                event.compliance_flags.extend([v['type'] for v in violations])
                await self._log_compliance_violations(event.event_id, violations)

            # Generate checksum
            event.checksum = self._generate_event_checksum(event)

            # Set retention date
            event.retention_date = self._calculate_retention_date(event)

            # Add to processing queue
            async with self.event_buffer_lock:
                self.pending_events.append(event)

                # Flush if buffer is full
                if len(self.pending_events) >= self.config.batch_size:
                    await self._flush_events()

            # Update session tracking
            if event.session_id:
                await self._update_session_tracking(event)

            # Update access patterns
            if event.user_id and event.resource_type:
                await self._update_access_patterns(event)

        except Exception as e:
            logger.error(f"Failed to log audit event: {e}")
            self.metrics["events_failed"] += 1

    async def _enrich_audit_event(self, event: AuditEvent):
        """Enrich audit event with additional context."""
        # Add geo-location if IP address available
        if event.ip_address and not event.geo_location:
            event.geo_location = await self._get_geo_location(event.ip_address)

        # Detect sensitive data access
        if event.new_values or event.old_values:
            event.sensitive_data_accessed = self._contains_sensitive_data(
                event.new_values or event.old_values
            )

        # Set default operation if not specified
        if not event.operation and event.event_type:
            event.operation = event.event_type.upper()

    def _classify_data(self, resource_type: str, data: Optional[Dict[str, Any]]) -> DataClassification:
        """Classify data sensitivity level."""
        # Check cache first
        if resource_type in self.classification_cache:
            return self.classification_cache[resource_type]

        # Default classification
        base_classification = DataClassification.INTERNAL

        # Check for PII patterns
        if data and self._contains_pii_data(data):
            base_classification = DataClassification.PII

        # Check for financial/payment data
        if data and self._contains_financial_data(data):
            base_classification = DataClassification.PCI

        # Cache the result
        self.classification_cache[resource_type] = base_classification
        return base_classification

    def _contains_pii_data(self, data: Dict[str, Any]) -> bool:
        """Check if data contains PII patterns."""
        import re

        data_str = json.dumps(data, default=str).lower()

        for pattern in self.config.pii_patterns:
            if re.search(pattern, data_str):
                return True

        # Check for common PII field names
        pii_fields = ['ssn', 'social_security', 'email', 'phone', 'address', 'credit_card', 'passport']
        for field in pii_fields:
            if field in data_str:
                return True

        return False

    def _contains_financial_data(self, data: Dict[str, Any]) -> bool:
        """Check if data contains financial information."""
        financial_fields = ['card_number', 'account_number', 'routing_number', 'payment', 'transaction', 'amount']
        data_str = json.dumps(data, default=str).lower()

        return any(field in data_str for field in financial_fields)

    def _contains_sensitive_data(self, data: Dict[str, Any]) -> bool:
        """Check if data contains any sensitive information."""
        return self._contains_pii_data(data) or self._contains_financial_data(data)

    async def _calculate_risk_score(self, event: AuditEvent) -> float:
        """Calculate risk score for the event."""
        score = 0.0

        # Base scores by event type
        risk_weights = {
            AuditEventType.DELETE: 0.8,
            AuditEventType.UPDATE: 0.6,
            AuditEventType.EXPORT: 0.7,
            AuditEventType.ACCESS_DENIED: 0.9,
            AuditEventType.SECURITY_EVENT: 1.0,
            AuditEventType.CREATE: 0.3,
            AuditEventType.READ: 0.1
        }

        score += risk_weights.get(event.event_type, 0.1)

        # Data classification multiplier
        classification_multipliers = {
            DataClassification.PUBLIC: 0.1,
            DataClassification.INTERNAL: 0.3,
            DataClassification.CONFIDENTIAL: 0.7,
            DataClassification.RESTRICTED: 0.9,
            DataClassification.PII: 0.8,
            DataClassification.PCI: 1.0
        }

        score *= classification_multipliers.get(event.data_classification, 0.5)

        # Volume factor
        if event.affected_rows > 100:
            score += 0.2
        elif event.affected_rows > 1000:
            score += 0.4
        elif event.affected_rows > 10000:
            score += 0.6

        # Time-based factors
        hour = event.timestamp.hour
        if hour < 6 or hour > 22:  # Outside business hours
            score += 0.2

        # Weekend factor
        if event.timestamp.weekday() >= 5:  # Weekend
            score += 0.1

        return min(score, 1.0)  # Cap at 1.0

    async def _check_compliance_violations(self, event: AuditEvent) -> List[Dict[str, Any]]:
        """Check for compliance violations."""
        violations = []

        # GDPR violations
        if self.config.enable_gdpr_compliance:
            gdpr_violations = await self._check_gdpr_violations(event)
            violations.extend(gdpr_violations)

        # SOX violations
        if self.config.enable_sox_compliance:
            sox_violations = await self._check_sox_violations(event)
            violations.extend(sox_violations)

        # PCI violations
        if self.config.enable_pci_compliance:
            pci_violations = await self._check_pci_violations(event)
            violations.extend(pci_violations)

        return violations

    async def _check_gdpr_violations(self, event: AuditEvent) -> List[Dict[str, Any]]:
        """Check for GDPR compliance violations."""
        violations = []

        # Check for PII access without consent
        if (event.data_classification == DataClassification.PII and
            event.event_type in [AuditEventType.READ, AuditEventType.EXPORT] and
            'gdpr_consent' not in event.compliance_flags):

            violations.append({
                'type': 'GDPR_UNAUTHORIZED_PII_ACCESS',
                'severity': 'high',
                'description': f'PII data accessed without GDPR consent: {event.resource_type}'
            })

        # Check for data retention violations
        if event.retention_date and event.retention_date < datetime.now().date():
            violations.append({
                'type': 'GDPR_DATA_RETENTION_VIOLATION',
                'severity': 'medium',
                'description': f'Data accessed beyond retention period: {event.resource_type}'
            })

        return violations

    async def _check_sox_violations(self, event: AuditEvent) -> List[Dict[str, Any]]:
        """Check for SOX compliance violations."""
        violations = []

        # Check for financial data modifications without approval
        if (event.data_classification == DataClassification.CONFIDENTIAL and
            event.event_type in [AuditEventType.UPDATE, AuditEventType.DELETE] and
            event.resource_type in ['transactions', 'sales', 'financials']):

            violations.append({
                'type': 'SOX_UNAUTHORIZED_FINANCIAL_MODIFICATION',
                'severity': 'critical',
                'description': f'Financial data modified without proper authorization: {event.resource_type}'
            })

        return violations

    async def _check_pci_violations(self, event: AuditEvent) -> List[Dict[str, Any]]:
        """Check for PCI compliance violations."""
        violations = []

        # Check for payment data exposure
        if (event.data_classification == DataClassification.PCI and
            event.event_type == AuditEventType.READ and
            not event.compliance_flags):

            violations.append({
                'type': 'PCI_PAYMENT_DATA_EXPOSURE',
                'severity': 'critical',
                'description': f'Payment card data accessed without proper safeguards: {event.resource_type}'
            })

        return violations

    async def _log_compliance_violations(self, event_id: str, violations: List[Dict[str, Any]]):
        """Log compliance violations."""
        try:
            async with self.engine.begin() as conn:
                for violation in violations:
                    violation_id = str(uuid4())
                    await conn.execute(text("""
                        INSERT INTO audit_compliance_violations
                        (violation_id, event_id, compliance_type, violation_type,
                         severity, description, affected_data_types)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """), violation_id, event_id, violation['type'].split('_')[0],
                         violation['type'], violation['severity'], violation['description'], [])

            self.metrics["compliance_violations"] += len(violations)

        except Exception as e:
            logger.error(f"Failed to log compliance violations: {e}")

    def _generate_event_checksum(self, event: AuditEvent) -> str:
        """Generate checksum for event integrity."""
        event_data = f"{event.event_id}{event.timestamp}{event.user_id}{event.operation}{event.resource_type}"
        return hashlib.sha256(event_data.encode()).hexdigest()

    def _calculate_retention_date(self, event: AuditEvent) -> datetime.date:
        """Calculate retention date based on data classification."""
        retention_days = self.config.default_retention_days

        if event.data_classification == DataClassification.PII:
            retention_days = self.config.pii_retention_days
        elif event.event_type == AuditEventType.SECURITY_EVENT:
            retention_days = self.config.security_event_retention_days

        return (event.timestamp + timedelta(days=retention_days)).date()

    async def _flush_events(self):
        """Flush pending events to database."""
        if not self.pending_events:
            return

        events_to_process = self.pending_events.copy()
        self.pending_events.clear()

        try:
            async with self.engine.begin() as conn:
                for event in events_to_process:
                    await conn.execute(text("""
                        INSERT INTO audit_events (
                            event_id, event_type, timestamp, user_id, session_id,
                            ip_address, user_agent, resource_type, resource_id, resource_name,
                            old_values, new_values, changed_fields, operation, affected_rows,
                            data_classification, compliance_flags, request_id, endpoint, method,
                            status_code, risk_score, sensitive_data_accessed, geo_location,
                            status, checksum, retention_date
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25, $26, $27
                        )
                    """),
                    event.event_id, event.event_type.value, event.timestamp,
                    event.user_id, event.session_id, event.ip_address, event.user_agent,
                    event.resource_type, event.resource_id, event.resource_name,
                    event.old_values, event.new_values, event.changed_fields,
                    event.operation, event.affected_rows, event.data_classification.value,
                    event.compliance_flags, event.request_id, event.endpoint, event.method,
                    event.status_code, event.risk_score, event.sensitive_data_accessed,
                    event.geo_location, event.status.value, event.checksum, event.retention_date
                    )

            self.metrics["events_processed"] += len(events_to_process)
            self.metrics["last_processing_time"] = datetime.now()

        except Exception as e:
            logger.error(f"Failed to flush audit events: {e}")
            self.metrics["events_failed"] += len(events_to_process)

    async def _event_processing_loop(self):
        """Background event processing loop."""
        while True:
            try:
                await asyncio.sleep(self.config.flush_interval_seconds)

                async with self.event_buffer_lock:
                    if self.pending_events:
                        await self._flush_events()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processing loop: {e}")

    async def get_audit_report(self,
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None,
                              user_id: Optional[str] = None,
                              resource_type: Optional[str] = None,
                              include_compliance: bool = True) -> Dict[str, Any]:
        """Generate comprehensive audit report."""

        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()

        try:
            async with self.engine.begin() as conn:
                # Base event statistics
                events_query = """
                    SELECT
                        event_type,
                        data_classification,
                        COUNT(*) as event_count,
                        AVG(risk_score) as avg_risk_score,
                        SUM(CASE WHEN sensitive_data_accessed THEN 1 ELSE 0 END) as sensitive_access_count
                    FROM audit_events
                    WHERE timestamp BETWEEN $1 AND $2
                """
                params = [start_date, end_date]

                if user_id:
                    events_query += " AND user_id = $3"
                    params.append(user_id)

                if resource_type:
                    param_num = len(params) + 1
                    events_query += f" AND resource_type = ${param_num}"
                    params.append(resource_type)

                events_query += " GROUP BY event_type, data_classification ORDER BY event_count DESC"

                events_result = await conn.execute(text(events_query), *params)
                events_data = [dict(row._mapping) for row in events_result]

                # Compliance violations
                violations_data = []
                if include_compliance:
                    violations_query = """
                        SELECT
                            compliance_type,
                            violation_type,
                            severity,
                            COUNT(*) as violation_count,
                            COUNT(CASE WHEN remediation_status = 'resolved' THEN 1 END) as resolved_count
                        FROM audit_compliance_violations v
                        JOIN audit_events e ON v.event_id = e.event_id
                        WHERE e.timestamp BETWEEN $1 AND $2
                        GROUP BY compliance_type, violation_type, severity
                        ORDER BY violation_count DESC
                    """

                    violations_result = await conn.execute(text(violations_query), start_date, end_date)
                    violations_data = [dict(row._mapping) for row in violations_result]

                # User activity summary
                user_activity_query = """
                    SELECT
                        user_id,
                        COUNT(*) as total_events,
                        COUNT(DISTINCT resource_type) as resources_accessed,
                        AVG(risk_score) as avg_risk_score,
                        MAX(timestamp) as last_activity
                    FROM audit_events
                    WHERE timestamp BETWEEN $1 AND $2 AND user_id IS NOT NULL
                    GROUP BY user_id
                    ORDER BY total_events DESC
                    LIMIT 20
                """

                user_result = await conn.execute(text(user_activity_query), start_date, end_date)
                user_data = [dict(row._mapping) for row in user_result]

                return {
                    "report_period": {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat()
                    },
                    "summary": {
                        "total_events": sum(row["event_count"] for row in events_data),
                        "total_violations": sum(row["violation_count"] for row in violations_data),
                        "unique_users": len(user_data),
                        "avg_risk_score": sum(row.get("avg_risk_score", 0) for row in events_data) / len(events_data) if events_data else 0
                    },
                    "events_by_type": events_data,
                    "compliance_violations": violations_data,
                    "top_users": user_data,
                    "system_metrics": self.metrics,
                    "generated_at": datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Failed to generate audit report: {e}")
            return {"error": str(e)}

    async def close(self):
        """Close the audit trail manager."""
        try:
            # Cancel background processing
            if self.processing_task:
                self.processing_task.cancel()
                try:
                    await self.processing_task
                except asyncio.CancelledError:
                    pass

            # Flush remaining events
            async with self.event_buffer_lock:
                if self.pending_events:
                    await self._flush_events()

            logger.info("Audit trail manager closed successfully")

        except Exception as e:
            logger.warning(f"Error closing audit trail manager: {e}")

    async def _get_geo_location(self, ip_address: str) -> Optional[str]:
        """Get geo-location for IP address (placeholder implementation)."""
        # In production, integrate with GeoIP service
        return f"Location for {ip_address}"

    async def _update_session_tracking(self, event: AuditEvent):
        """Update session tracking information."""
        # Implementation for session tracking
        pass

    async def _update_access_patterns(self, event: AuditEvent):
        """Update access pattern analysis."""
        # Implementation for access pattern analysis
        pass

    async def _register_database_listeners(self):
        """Register database event listeners."""
        # Implementation for database event listening
        pass


# Factory function
def create_audit_trail_manager(engine: AsyncEngine, config: Optional[AuditConfiguration] = None) -> ComprehensiveAuditTrailManager:
    """Create audit trail manager instance."""
    return ComprehensiveAuditTrailManager(engine, config)


# Context manager for audit trail
@asynccontextmanager
async def audit_context(manager: ComprehensiveAuditTrailManager,
                       user_id: str,
                       session_id: str,
                       request_id: Optional[str] = None):
    """Context manager for automatic audit trail."""
    # Store context for automatic event logging
    context = {
        'user_id': user_id,
        'session_id': session_id,
        'request_id': request_id,
        'start_time': datetime.now()
    }

    try:
        yield context
    finally:
        # Log context completion
        duration = (datetime.now() - context['start_time']).total_seconds()

        event = AuditEvent(
            event_type=AuditEventType.ACCESS_GRANTED,
            user_id=user_id,
            session_id=session_id,
            request_id=request_id,
            operation="CONTEXT_COMPLETE",
            resource_type="session",
            new_values={"duration_seconds": duration}
        )

        await manager.log_event(event)