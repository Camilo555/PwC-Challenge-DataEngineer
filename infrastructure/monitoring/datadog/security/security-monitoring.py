#!/usr/bin/env python3
"""
Advanced Security Monitoring and Threat Detection
Provides comprehensive security monitoring, threat detection, and compliance monitoring
"""

import json
import os
import re
import logging
import hashlib
import subprocess
import requests
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import threading
import time
from datadog import api, initialize
import psutil
import socket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SecurityEvent:
    """Security event data structure"""
    event_id: str
    timestamp: datetime
    event_type: str
    severity: str
    source_ip: str
    destination_ip: str
    source_port: int
    destination_port: int
    protocol: str
    action: str
    description: str
    user_agent: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    threat_indicators: Optional[List[str]] = None

@dataclass
class ComplianceCheck:
    """Compliance check result"""
    check_id: str
    framework: str
    control_id: str
    description: str
    status: str
    severity: str
    remediation: str
    evidence: Optional[str] = None

@dataclass
class ThreatIntelligence:
    """Threat intelligence data"""
    indicator: str
    indicator_type: str
    threat_type: str
    confidence: float
    source: str
    first_seen: datetime
    last_seen: datetime
    tags: List[str]

class SecurityMonitor:
    """Advanced security monitoring and threat detection"""
    
    def __init__(self, config: Dict):
        """Initialize security monitor"""
        self.config = config
        self.running = False
        
        # Initialize DataDog
        initialize(
            api_key=config.get('datadog_api_key'),
            app_key=config.get('datadog_app_key'),
            api_host=config.get('datadog_host', 'https://api.datadoghq.com')
        )
        
        # Security monitoring settings
        self.monitor_network_traffic = config.get('monitor_network_traffic', True)
        self.monitor_file_integrity = config.get('monitor_file_integrity', True)
        self.monitor_user_activity = config.get('monitor_user_activity', True)
        self.monitor_process_activity = config.get('monitor_process_activity', True)
        
        # Threat intelligence sources
        self.threat_feeds = {
            'malware_domains': config.get('malware_domain_feed_url', ''),
            'malicious_ips': config.get('malicious_ip_feed_url', ''),
            'suspicious_urls': config.get('suspicious_url_feed_url', '')
        }
        
        # Compliance frameworks to monitor
        self.compliance_frameworks = [
            'SOC2',
            'GDPR',
            'HIPAA',
            'PCI_DSS',
            'ISO_27001',
            'NIST_CSF'
        ]
        
        # Security event patterns
        self.threat_patterns = {
            'sql_injection': [
                r"(?i)(union|select|insert|delete|update|drop|create|alter).*(\s|%20|%09|%0a|%0d)",
                r"(?i)'.*or.*'.*=.*'",
                r"(?i)1.*=.*1",
                r"(?i)(exec|execute|sp_|xp_)"
            ],
            'xss_attack': [
                r"(?i)<script.*>",
                r"(?i)javascript:",
                r"(?i)on\w+\s*=",
                r"(?i)(alert|confirm|prompt)\("
            ],
            'directory_traversal': [
                r"\.\.[\\/]",
                r"[\\/]etc[\\/]passwd",
                r"[\\/]windows[\\/]system32",
                r"file://"
            ],
            'command_injection': [
                r"(?i)(;|&&|\|\||\|).*(\s|%20)(cat|ls|pwd|whoami|id|uname)",
                r"(?i)(`|$\()",
                r"(?i)(wget|curl|nc|netcat).*http"
            ]
        }
        
        # File integrity monitoring paths
        self.fim_paths = [
            '/etc/passwd',
            '/etc/shadow',
            '/etc/hosts',
            '/etc/ssh/sshd_config',
            '/etc/sudoers',
            '/var/log/',
            '/opt/datadog-agent/',
            '/etc/kubernetes/',
            '/opt/kafka/config/',
            '/etc/postgresql/',
            '/etc/redis/'
        ]
        
        # Initialize components
        self.threat_intel_cache = {}
        self.security_events = []
        self.file_hashes = {}
        
    def start_monitoring(self):
        """Start security monitoring"""
        logger.info("Starting security monitoring")
        self.running = True
        
        # Start monitoring threads
        if self.monitor_network_traffic:
            threading.Thread(target=self._monitor_network_traffic, daemon=True).start()
        
        if self.monitor_file_integrity:
            threading.Thread(target=self._monitor_file_integrity, daemon=True).start()
        
        if self.monitor_user_activity:
            threading.Thread(target=self._monitor_user_activity, daemon=True).start()
        
        if self.monitor_process_activity:
            threading.Thread(target=self._monitor_process_activity, daemon=True).start()
        
        # Start compliance monitoring
        threading.Thread(target=self._monitor_compliance, daemon=True).start()
        
        # Start threat intelligence updates
        threading.Thread(target=self._update_threat_intelligence, daemon=True).start()
        
        # Start metrics reporting
        threading.Thread(target=self._report_security_metrics, daemon=True).start()
    
    def stop_monitoring(self):
        """Stop security monitoring"""
        logger.info("Stopping security monitoring")
        self.running = False
    
    def _monitor_network_traffic(self):
        """Monitor network traffic for threats"""
        logger.info("Starting network traffic monitoring")
        
        while self.running:
            try:
                # Monitor network connections
                connections = psutil.net_connections(kind='inet')
                
                for conn in connections:
                    if conn.status == 'ESTABLISHED':
                        # Check against threat intelligence
                        if conn.raddr:
                            remote_ip = conn.raddr.ip
                            if self._is_malicious_ip(remote_ip):
                                self._create_security_event(
                                    event_type='malicious_connection',
                                    severity='HIGH',
                                    source_ip=conn.laddr.ip if conn.laddr else '',
                                    destination_ip=remote_ip,
                                    source_port=conn.laddr.port if conn.laddr else 0,
                                    destination_port=conn.raddr.port,
                                    protocol='TCP',
                                    action='DETECTED',
                                    description=f'Connection to known malicious IP: {remote_ip}'
                                )
                
                # Monitor for suspicious port activities
                self._detect_port_scanning()
                
                # Monitor DNS queries (if available)
                self._monitor_dns_queries()
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring network traffic: {e}")
                time.sleep(60)
    
    def _monitor_file_integrity(self):
        """Monitor file integrity"""
        logger.info("Starting file integrity monitoring")
        
        # Initialize file hashes
        self._initialize_file_hashes()
        
        while self.running:
            try:
                for file_path in self.fim_paths:
                    if os.path.exists(file_path):
                        current_hash = self._calculate_file_hash(file_path)
                        stored_hash = self.file_hashes.get(file_path)
                        
                        if stored_hash and current_hash != stored_hash:
                            self._create_security_event(
                                event_type='file_integrity_violation',
                                severity='MEDIUM',
                                source_ip='localhost',
                                destination_ip='localhost',
                                source_port=0,
                                destination_port=0,
                                protocol='FILE',
                                action='MODIFIED',
                                description=f'File integrity violation detected: {file_path}'
                            )
                        
                        self.file_hashes[file_path] = current_hash
                
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error monitoring file integrity: {e}")
                time.sleep(600)
    
    def _monitor_user_activity(self):
        """Monitor user activity for anomalies"""
        logger.info("Starting user activity monitoring")
        
        while self.running:
            try:
                # Monitor login attempts
                self._monitor_login_attempts()
                
                # Monitor sudo usage
                self._monitor_sudo_usage()
                
                # Monitor SSH connections
                self._monitor_ssh_connections()
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error monitoring user activity: {e}")
                time.sleep(120)
    
    def _monitor_process_activity(self):
        """Monitor process activity for threats"""
        logger.info("Starting process activity monitoring")
        
        while self.running:
            try:
                # Get running processes
                processes = list(psutil.process_iter(['pid', 'name', 'cmdline', 'username']))
                
                for proc in processes:
                    try:
                        proc_info = proc.info
                        cmdline = ' '.join(proc_info.get('cmdline', []))
                        
                        # Check for suspicious processes
                        if self._is_suspicious_process(proc_info['name'], cmdline):
                            self._create_security_event(
                                event_type='suspicious_process',
                                severity='HIGH',
                                source_ip='localhost',
                                destination_ip='localhost',
                                source_port=0,
                                destination_port=0,
                                protocol='PROCESS',
                                action='DETECTED',
                                description=f'Suspicious process detected: {proc_info["name"]} (PID: {proc_info["pid"]})',
                                user_id=proc_info.get('username')
                            )
                        
                        # Check for crypto mining indicators
                        if self._is_crypto_mining_process(proc_info['name'], cmdline):
                            self._create_security_event(
                                event_type='crypto_mining',
                                severity='HIGH',
                                source_ip='localhost',
                                destination_ip='localhost',
                                source_port=0,
                                destination_port=0,
                                protocol='PROCESS',
                                action='DETECTED',
                                description=f'Potential crypto mining process: {proc_info["name"]}',
                                user_id=proc_info.get('username')
                            )
                        
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                time.sleep(120)  # Check every 2 minutes
                
            except Exception as e:
                logger.error(f"Error monitoring process activity: {e}")
                time.sleep(180)
    
    def _monitor_compliance(self):
        """Monitor compliance across multiple frameworks"""
        logger.info("Starting compliance monitoring")
        
        while self.running:
            try:
                compliance_results = []
                
                for framework in self.compliance_frameworks:
                    framework_results = self._run_compliance_checks(framework)
                    compliance_results.extend(framework_results)
                
                # Send compliance metrics to DataDog
                self._send_compliance_metrics(compliance_results)
                
                # Generate compliance alerts
                self._generate_compliance_alerts(compliance_results)
                
                time.sleep(3600)  # Check every hour
                
            except Exception as e:
                logger.error(f"Error monitoring compliance: {e}")
                time.sleep(1800)
    
    def _run_compliance_checks(self, framework: str) -> List[ComplianceCheck]:
        """Run compliance checks for specific framework"""
        checks = []
        
        if framework == 'SOC2':
            checks.extend(self._run_soc2_checks())
        elif framework == 'GDPR':
            checks.extend(self._run_gdpr_checks())
        elif framework == 'HIPAA':
            checks.extend(self._run_hipaa_checks())
        elif framework == 'PCI_DSS':
            checks.extend(self._run_pci_dss_checks())
        elif framework == 'ISO_27001':
            checks.extend(self._run_iso27001_checks())
        elif framework == 'NIST_CSF':
            checks.extend(self._run_nist_csf_checks())
        
        return checks
    
    def _run_soc2_checks(self) -> List[ComplianceCheck]:
        """Run SOC2 compliance checks"""
        checks = []
        
        # CC6.1 - Logical and physical access controls
        checks.append(ComplianceCheck(
            check_id='SOC2_CC6_1',
            framework='SOC2',
            control_id='CC6.1',
            description='Logical and physical access controls',
            status=self._check_access_controls(),
            severity='HIGH',
            remediation='Implement proper access controls'
        ))
        
        # CC6.7 - Data transmission security
        checks.append(ComplianceCheck(
            check_id='SOC2_CC6_7',
            framework='SOC2',
            control_id='CC6.7',
            description='Data transmission security',
            status=self._check_encryption_in_transit(),
            severity='HIGH',
            remediation='Enable encryption for data in transit'
        ))
        
        return checks
    
    def _run_gdpr_checks(self) -> List[ComplianceCheck]:
        """Run GDPR compliance checks"""
        checks = []
        
        # Article 32 - Security of processing
        checks.append(ComplianceCheck(
            check_id='GDPR_ART32',
            framework='GDPR',
            control_id='Article 32',
            description='Security of processing',
            status=self._check_data_encryption(),
            severity='HIGH',
            remediation='Implement appropriate technical and organizational measures'
        ))
        
        return checks
    
    def _run_hipaa_checks(self) -> List[ComplianceCheck]:
        """Run HIPAA compliance checks"""
        checks = []
        
        # 164.312(a)(1) - Access control
        checks.append(ComplianceCheck(
            check_id='HIPAA_164_312_A1',
            framework='HIPAA',
            control_id='164.312(a)(1)',
            description='Access control',
            status=self._check_access_controls(),
            severity='HIGH',
            remediation='Implement access control measures'
        ))
        
        return checks
    
    def _run_pci_dss_checks(self) -> List[ComplianceCheck]:
        """Run PCI DSS compliance checks"""
        checks = []
        
        # Requirement 4 - Encrypt transmission of cardholder data
        checks.append(ComplianceCheck(
            check_id='PCI_DSS_REQ4',
            framework='PCI_DSS',
            control_id='Requirement 4',
            description='Encrypt transmission of cardholder data',
            status=self._check_encryption_in_transit(),
            severity='HIGH',
            remediation='Enable strong cryptography and security protocols'
        ))
        
        return checks
    
    def _run_iso27001_checks(self) -> List[ComplianceCheck]:
        """Run ISO 27001 compliance checks"""
        checks = []
        
        # A.9.1.1 - Access control policy
        checks.append(ComplianceCheck(
            check_id='ISO27001_A911',
            framework='ISO_27001',
            control_id='A.9.1.1',
            description='Access control policy',
            status=self._check_access_controls(),
            severity='MEDIUM',
            remediation='Establish and maintain access control policy'
        ))
        
        return checks
    
    def _run_nist_csf_checks(self) -> List[ComplianceCheck]:
        """Run NIST CSF compliance checks"""
        checks = []
        
        # PR.AC-1 - Identity and credential management
        checks.append(ComplianceCheck(
            check_id='NIST_CSF_PRAC1',
            framework='NIST_CSF',
            control_id='PR.AC-1',
            description='Identity and credential management',
            status=self._check_identity_management(),
            severity='HIGH',
            remediation='Implement identity and credential management processes'
        ))
        
        return checks
    
    def _check_access_controls(self) -> str:
        """Check access control implementation"""
        try:
            # Check for proper file permissions
            sensitive_files = ['/etc/passwd', '/etc/shadow', '/etc/sudoers']
            for file_path in sensitive_files:
                if os.path.exists(file_path):
                    file_stat = os.stat(file_path)
                    # Check if file has appropriate permissions
                    if file_stat.st_mode & 0o077:  # World or group readable
                        return 'NON_COMPLIANT'
            
            return 'COMPLIANT'
        except Exception:
            return 'UNKNOWN'
    
    def _check_encryption_in_transit(self) -> str:
        """Check encryption in transit"""
        try:
            # Check for TLS/SSL configurations
            connections = psutil.net_connections(kind='inet')
            unencrypted_count = 0
            
            for conn in connections:
                if conn.status == 'LISTEN':
                    # Check for common unencrypted ports
                    if conn.laddr.port in [80, 21, 23, 25, 53, 110, 143]:
                        unencrypted_count += 1
            
            return 'COMPLIANT' if unencrypted_count == 0 else 'NON_COMPLIANT'
        except Exception:
            return 'UNKNOWN'
    
    def _check_data_encryption(self) -> str:
        """Check data encryption at rest"""
        try:
            # Check for encrypted file systems or databases
            # This is a simplified check - in practice, would need more sophisticated detection
            
            # Check for LUKS encrypted volumes
            result = subprocess.run(['lsblk', '-f'], capture_output=True, text=True)
            if 'crypto_LUKS' in result.stdout:
                return 'COMPLIANT'
            
            return 'PARTIAL'  # May have application-level encryption
        except Exception:
            return 'UNKNOWN'
    
    def _check_identity_management(self) -> str:
        """Check identity and credential management"""
        try:
            # Check password policy
            if os.path.exists('/etc/pam.d/common-password'):
                with open('/etc/pam.d/common-password', 'r') as f:
                    content = f.read()
                    if 'minlen=' in content and 'ucredit=' in content:
                        return 'COMPLIANT'
            
            return 'NON_COMPLIANT'
        except Exception:
            return 'UNKNOWN'
    
    def _update_threat_intelligence(self):
        """Update threat intelligence feeds"""
        logger.info("Starting threat intelligence updates")
        
        while self.running:
            try:
                for feed_name, feed_url in self.threat_feeds.items():
                    if feed_url:
                        self._update_threat_feed(feed_name, feed_url)
                
                time.sleep(3600)  # Update every hour
                
            except Exception as e:
                logger.error(f"Error updating threat intelligence: {e}")
                time.sleep(1800)
    
    def _update_threat_feed(self, feed_name: str, feed_url: str):
        """Update specific threat intelligence feed"""
        try:
            response = requests.get(feed_url, timeout=30)
            response.raise_for_status()
            
            # Parse threat indicators (assuming simple text format)
            indicators = response.text.strip().split('\n')
            
            for indicator in indicators:
                if indicator and not indicator.startswith('#'):
                    self.threat_intel_cache[indicator] = ThreatIntelligence(
                        indicator=indicator,
                        indicator_type=self._determine_indicator_type(indicator),
                        threat_type=feed_name,
                        confidence=0.8,
                        source=feed_url,
                        first_seen=datetime.now(),
                        last_seen=datetime.now(),
                        tags=[feed_name]
                    )
            
            logger.info(f"Updated threat feed {feed_name}: {len(indicators)} indicators")
            
        except Exception as e:
            logger.error(f"Error updating threat feed {feed_name}: {e}")
    
    def _determine_indicator_type(self, indicator: str) -> str:
        """Determine threat indicator type"""
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', indicator):
            return 'ip'
        elif '.' in indicator and not '/' in indicator:
            return 'domain'
        elif indicator.startswith('http'):
            return 'url'
        else:
            return 'unknown'
    
    def _is_malicious_ip(self, ip: str) -> bool:
        """Check if IP is in threat intelligence"""
        return ip in self.threat_intel_cache
    
    def _detect_port_scanning(self):
        """Detect port scanning activities"""
        # This would implement port scan detection logic
        # For now, this is a placeholder
        pass
    
    def _monitor_dns_queries(self):
        """Monitor DNS queries for malicious domains"""
        # This would implement DNS query monitoring
        # For now, this is a placeholder
        pass
    
    def _initialize_file_hashes(self):
        """Initialize file integrity monitoring hashes"""
        for file_path in self.fim_paths:
            if os.path.exists(file_path):
                self.file_hashes[file_path] = self._calculate_file_hash(file_path)
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of file"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception:
            return ''
    
    def _monitor_login_attempts(self):
        """Monitor login attempts from logs"""
        # This would parse authentication logs for failed login attempts
        pass
    
    def _monitor_sudo_usage(self):
        """Monitor sudo usage"""
        # This would parse sudo logs for privilege escalation attempts
        pass
    
    def _monitor_ssh_connections(self):
        """Monitor SSH connections"""
        # This would monitor SSH connection logs
        pass
    
    def _is_suspicious_process(self, process_name: str, cmdline: str) -> bool:
        """Check if process is suspicious"""
        suspicious_patterns = [
            r'.*\.tmp$',
            r'/tmp/.*',
            r'.*base64.*',
            r'.*powershell.*',
            r'.*wget.*http.*',
            r'.*curl.*http.*'
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, cmdline, re.IGNORECASE):
                return True
        
        return False
    
    def _is_crypto_mining_process(self, process_name: str, cmdline: str) -> bool:
        """Check if process is crypto mining"""
        mining_indicators = [
            'xmrig', 'cpuminer', 'ccminer', 'cgminer', 'bfgminer',
            'stratum+tcp', 'mining', 'hashrate', 'pool.'
        ]
        
        for indicator in mining_indicators:
            if indicator in cmdline.lower() or indicator in process_name.lower():
                return True
        
        return False
    
    def _create_security_event(self, event_type: str, severity: str, source_ip: str, 
                              destination_ip: str, source_port: int, destination_port: int,
                              protocol: str, action: str, description: str, **kwargs):
        """Create security event"""
        event = SecurityEvent(
            event_id=hashlib.md5(f"{datetime.now().isoformat()}{description}".encode()).hexdigest(),
            timestamp=datetime.now(),
            event_type=event_type,
            severity=severity,
            source_ip=source_ip,
            destination_ip=destination_ip,
            source_port=source_port,
            destination_port=destination_port,
            protocol=protocol,
            action=action,
            description=description,
            **kwargs
        )
        
        self.security_events.append(event)
        
        # Send to DataDog immediately for high severity events
        if severity in ['HIGH', 'CRITICAL']:
            self._send_security_event_to_datadog(event)
    
    def _send_security_event_to_datadog(self, event: SecurityEvent):
        """Send security event to DataDog"""
        try:
            api.Event.create(
                title=f"Security Event: {event.event_type}",
                text=event.description,
                alert_type='error' if event.severity in ['HIGH', 'CRITICAL'] else 'warning',
                tags=[
                    f'event_type:{event.event_type}',
                    f'severity:{event.severity}',
                    f'source_ip:{event.source_ip}',
                    f'protocol:{event.protocol}',
                    'team:security',
                    'env:production'
                ],
                source_type_name='security-monitor'
            )
        except Exception as e:
            logger.error(f"Error sending security event to DataDog: {e}")
    
    def _send_compliance_metrics(self, compliance_results: List[ComplianceCheck]):
        """Send compliance metrics to DataDog"""
        try:
            timestamp = int(time.time())
            
            # Group results by framework and status
            framework_metrics = {}
            
            for check in compliance_results:
                framework = check.framework
                status = check.status
                
                if framework not in framework_metrics:
                    framework_metrics[framework] = {
                        'COMPLIANT': 0,
                        'NON_COMPLIANT': 0,
                        'PARTIAL': 0,
                        'UNKNOWN': 0
                    }
                
                framework_metrics[framework][status] += 1
            
            # Send metrics for each framework
            for framework, metrics in framework_metrics.items():
                total_checks = sum(metrics.values())
                compliance_percentage = (metrics['COMPLIANT'] / total_checks * 100) if total_checks > 0 else 0
                
                api.Metric.send(
                    metric='security.compliance.percentage',
                    points=[(timestamp, compliance_percentage)],
                    tags=[f'framework:{framework}', 'env:production', 'team:security']
                )
                
                for status, count in metrics.items():
                    api.Metric.send(
                        metric='security.compliance.checks',
                        points=[(timestamp, count)],
                        tags=[f'framework:{framework}', f'status:{status}', 'env:production', 'team:security']
                    )
                    
        except Exception as e:
            logger.error(f"Error sending compliance metrics: {e}")
    
    def _generate_compliance_alerts(self, compliance_results: List[ComplianceCheck]):
        """Generate compliance alerts"""
        non_compliant_high = [c for c in compliance_results 
                             if c.status == 'NON_COMPLIANT' and c.severity == 'HIGH']
        
        if non_compliant_high:
            try:
                api.Event.create(
                    title=f"High Severity Compliance Violations Detected",
                    text=f"Found {len(non_compliant_high)} high severity compliance violations",
                    alert_type='error',
                    tags=['compliance', 'violation', 'high-severity', 'team:security', 'env:production'],
                    source_type_name='compliance-monitor'
                )
            except Exception as e:
                logger.error(f"Error sending compliance alert: {e}")
    
    def _report_security_metrics(self):
        """Report security metrics to DataDog"""
        logger.info("Starting security metrics reporting")
        
        while self.running:
            try:
                timestamp = int(time.time())
                
                # Report security event counts by type and severity
                event_counts = {}
                severity_counts = {}
                
                for event in self.security_events:
                    event_type = event.event_type
                    severity = event.severity
                    
                    event_counts[event_type] = event_counts.get(event_type, 0) + 1
                    severity_counts[severity] = severity_counts.get(severity, 0) + 1
                
                # Send event type metrics
                for event_type, count in event_counts.items():
                    api.Metric.send(
                        metric='security.events.count',
                        points=[(timestamp, count)],
                        tags=[f'event_type:{event_type}', 'env:production', 'team:security']
                    )
                
                # Send severity metrics
                for severity, count in severity_counts.items():
                    api.Metric.send(
                        metric='security.events.severity',
                        points=[(timestamp, count)],
                        tags=[f'severity:{severity}', 'env:production', 'team:security']
                    )
                
                # Report threat intelligence metrics
                threat_intel_count = len(self.threat_intel_cache)
                api.Metric.send(
                    metric='security.threat_intelligence.indicators',
                    points=[(timestamp, threat_intel_count)],
                    tags=['env:production', 'team:security']
                )
                
                # Clear processed events (keep last hour)
                cutoff_time = datetime.now() - timedelta(hours=1)
                self.security_events = [e for e in self.security_events if e.timestamp > cutoff_time]
                
                time.sleep(300)  # Report every 5 minutes
                
            except Exception as e:
                logger.error(f"Error reporting security metrics: {e}")
                time.sleep(300)

def main():
    """Main execution function"""
    config = {
        'datadog_api_key': 'your-datadog-api-key',
        'datadog_app_key': 'your-datadog-app-key',
        'monitor_network_traffic': True,
        'monitor_file_integrity': True,
        'monitor_user_activity': True,
        'monitor_process_activity': True,
        'malware_domain_feed_url': 'https://example.com/malware-domains.txt',
        'malicious_ip_feed_url': 'https://example.com/malicious-ips.txt',
        'suspicious_url_feed_url': 'https://example.com/suspicious-urls.txt'
    }
    
    monitor = SecurityMonitor(config)
    
    try:
        monitor.start_monitoring()
        
        # Keep running
        while True:
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        monitor.stop_monitoring()
        return 0
    except Exception as e:
        logger.error(f"Security monitoring failed: {e}")
        monitor.stop_monitoring()
        return 1

if __name__ == '__main__':
    exit(main())