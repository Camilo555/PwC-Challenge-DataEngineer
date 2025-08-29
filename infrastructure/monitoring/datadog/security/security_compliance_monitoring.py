"""
DataDog Security and Compliance Monitoring
Comprehensive security monitoring with compliance frameworks (SOC2, GDPR, HIPAA)
"""

import logging
from typing import Dict, List, Any, Optional, Set
from datadog import initialize, statsd
from datadog.api.metrics import Metrics
from datadog.api.events import Events
from datetime import datetime, timedelta
import asyncio
import time
import json
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
import subprocess
import requests
import socket
import ssl
from urllib.parse import urlparse
from cryptography import x509
from cryptography.hazmat.backends import default_backend
import psutil
import os
import ipaddress

# Initialize DataDog
initialize(
    api_key="<your-api-key>",
    app_key="<your-app-key>",
    host_name="enterprise-data-platform"
)

logger = logging.getLogger(__name__)

class DataDogSecurityComplianceMonitor:
    """Comprehensive security and compliance monitoring for enterprise data platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # Monitoring configuration
        self.monitoring_interval = config.get('monitoring_interval', 300)  # 5 minutes
        self.compliance_scan_interval = config.get('compliance_scan_interval', 3600)  # 1 hour
        self.vulnerability_scan_interval = config.get('vulnerability_scan_interval', 21600)  # 6 hours
        
        # Compliance frameworks
        self.compliance_frameworks = config.get('compliance_frameworks', ['SOC2', 'GDPR', 'HIPAA'])
        
        # Security baselines and thresholds
        self.security_baselines = {
            'failed_login_threshold': 5,
            'privilege_escalation_threshold': 1,
            'suspicious_network_threshold': 100,
            'certificate_expiry_days': 30,
            'password_policy_violations': 0,
            'data_exfiltration_threshold': 1000000000,  # 1GB
            'unauthorized_access_threshold': 1
        }
        
        # Network security monitoring
        self.monitored_ports = config.get('monitored_ports', [22, 80, 443, 5432, 6379, 9200])
        self.allowed_networks = config.get('allowed_networks', [])
        self.blocked_ips = set()
        
        # File integrity monitoring
        self.critical_files = config.get('critical_files', [
            '/etc/passwd', '/etc/shadow', '/etc/ssh/sshd_config',
            '/etc/kubernetes/manifests', '/etc/docker/daemon.json'
        ])
        self.file_checksums = {}
        
        # Compliance controls mapping
        self.compliance_controls = self._setup_compliance_controls()
        
        # Threat intelligence
        self.threat_indicators = set()
        self.last_threat_update = 0
        
        # Audit logs
        self.audit_log_sources = config.get('audit_log_sources', [])
        
    def _setup_compliance_controls(self) -> Dict[str, Dict[str, Any]]:
        """Setup compliance controls mapping"""
        return {
            'SOC2': {
                'CC6.1': {  # Logical and Physical Access Controls
                    'description': 'Implement logical and physical access controls',
                    'controls': ['access_control', 'authentication', 'authorization'],
                    'metrics': ['failed_logins', 'unauthorized_access', 'privilege_changes']
                },
                'CC6.2': {  # System Access Management
                    'description': 'Prior to issuing system credentials and granting system access',
                    'controls': ['user_provisioning', 'access_reviews', 'role_based_access'],
                    'metrics': ['new_user_accounts', 'access_reviews_completed', 'role_assignments']
                },
                'CC6.3': {  # Network Access Controls
                    'description': 'Network access controls are implemented',
                    'controls': ['network_segmentation', 'firewall_rules', 'intrusion_detection'],
                    'metrics': ['network_anomalies', 'firewall_violations', 'intrusion_attempts']
                },
                'CC7.1': {  # Security Monitoring
                    'description': 'System monitoring controls are implemented',
                    'controls': ['log_monitoring', 'anomaly_detection', 'incident_response'],
                    'metrics': ['security_events', 'anomalies_detected', 'incidents_resolved']
                }
            },
            'GDPR': {
                'Art25': {  # Data Protection by Design and by Default
                    'description': 'Data protection by design and by default',
                    'controls': ['data_encryption', 'privacy_by_design', 'data_minimization'],
                    'metrics': ['encryption_coverage', 'data_classification', 'data_retention_compliance']
                },
                'Art32': {  # Security of Processing
                    'description': 'Security of processing',
                    'controls': ['encryption', 'access_controls', 'regular_testing'],
                    'metrics': ['encryption_status', 'access_violations', 'security_tests_performed']
                },
                'Art33': {  # Data Breach Notification
                    'description': 'Notification of a personal data breach',
                    'controls': ['breach_detection', 'incident_response', 'notification_procedures'],
                    'metrics': ['data_breaches_detected', 'breach_response_time', 'notifications_sent']
                }
            },
            'HIPAA': {
                '164.308': {  # Administrative Safeguards
                    'description': 'Administrative safeguards',
                    'controls': ['security_officer', 'workforce_training', 'access_management'],
                    'metrics': ['training_completed', 'policy_violations', 'access_certifications']
                },
                '164.310': {  # Physical Safeguards
                    'description': 'Physical safeguards',
                    'controls': ['facility_access', 'workstation_controls', 'device_media_controls'],
                    'metrics': ['physical_access_violations', 'device_tracking', 'media_disposal']
                },
                '164.312': {  # Technical Safeguards
                    'description': 'Technical safeguards',
                    'controls': ['access_control', 'audit_controls', 'integrity', 'transmission_security'],
                    'metrics': ['technical_violations', 'audit_log_completeness', 'transmission_encryption']
                }
            }
        }
    
    async def monitor_security_events(self) -> Dict[str, Any]:
        """Monitor security events and anomalies"""
        try:
            security_metrics = {
                'failed_logins': 0,
                'successful_logins': 0,
                'privilege_escalations': 0,
                'suspicious_network_connections': 0,
                'malware_detections': 0,
                'policy_violations': 0,
                'security_alerts': []
            }
            
            # Monitor authentication events
            auth_events = await self._monitor_authentication_events()
            security_metrics.update(auth_events)
            
            # Monitor network security
            network_events = await self._monitor_network_security()
            security_metrics.update(network_events)
            
            # Monitor file integrity
            file_integrity = await self._monitor_file_integrity()
            security_metrics.update(file_integrity)
            
            # Monitor process anomalies
            process_anomalies = await self._monitor_process_anomalies()
            security_metrics.update(process_anomalies)
            
            # Check certificate expiration
            cert_status = await self._monitor_certificate_expiration()
            security_metrics.update(cert_status)
            
            # Send security metrics
            await self._send_security_metrics(security_metrics)
            
            return security_metrics
            
        except Exception as e:
            logger.error(f"Error monitoring security events: {e}")
            statsd.increment('security.monitoring.error')
            return {}
    
    async def _monitor_authentication_events(self) -> Dict[str, Any]:
        """Monitor authentication events"""
        try:
            metrics = {
                'failed_logins': 0,
                'successful_logins': 0,
                'brute_force_attempts': 0,
                'password_policy_violations': 0
            }
            
            # Monitor system authentication logs (Linux)
            if os.path.exists('/var/log/auth.log'):
                with open('/var/log/auth.log', 'r') as f:
                    lines = f.readlines()[-1000:]  # Last 1000 lines
                    
                    for line in lines:
                        if 'Failed password' in line:
                            metrics['failed_logins'] += 1
                        elif 'Accepted password' in line or 'Accepted publickey' in line:
                            metrics['successful_logins'] += 1
                        elif 'authentication failure' in line:
                            metrics['failed_logins'] += 1
            
            # Check for brute force attacks
            if metrics['failed_logins'] > self.security_baselines['failed_login_threshold']:
                metrics['brute_force_attempts'] = 1
                
                # Send alert
                statsd.event(
                    title="Potential Brute Force Attack Detected",
                    text=f"Detected {metrics['failed_logins']} failed login attempts",
                    alert_type='error',
                    tags=['security:authentication', 'attack:brute_force']
                )
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error monitoring authentication events: {e}")
            return {}
    
    async def _monitor_network_security(self) -> Dict[str, Any]:
        """Monitor network security events"""
        try:
            metrics = {
                'suspicious_connections': 0,
                'port_scan_attempts': 0,
                'ddos_attempts': 0,
                'unauthorized_protocols': 0
            }
            
            # Get network connections
            connections = psutil.net_connections(kind='inet')
            
            suspicious_ips = set()
            connection_counts = {}
            
            for conn in connections:
                if conn.raddr:
                    remote_ip = conn.raddr.ip
                    remote_port = conn.raddr.port
                    
                    # Count connections per IP
                    connection_counts[remote_ip] = connection_counts.get(remote_ip, 0) + 1
                    
                    # Check for suspicious ports
                    if remote_port in [4444, 5555, 6666, 7777]:  # Common backdoor ports
                        suspicious_ips.add(remote_ip)
                        metrics['suspicious_connections'] += 1
                    
                    # Check against threat intelligence
                    if remote_ip in self.threat_indicators:
                        suspicious_ips.add(remote_ip)
                        metrics['suspicious_connections'] += 1
            
            # Check for potential DDoS
            for ip, count in connection_counts.items():
                if count > self.security_baselines['suspicious_network_threshold']:
                    metrics['ddos_attempts'] += 1
                    suspicious_ips.add(ip)
            
            # Monitor port scans
            metrics['port_scan_attempts'] = len([ip for ip, count in connection_counts.items() if count > 50])
            
            # Block suspicious IPs
            for ip in suspicious_ips:
                if ip not in self.blocked_ips:
                    await self._block_ip(ip)
                    self.blocked_ips.add(ip)
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error monitoring network security: {e}")
            return {}
    
    async def _monitor_file_integrity(self) -> Dict[str, Any]:
        """Monitor file integrity of critical files"""
        try:
            metrics = {
                'file_changes': 0,
                'unauthorized_modifications': 0,
                'integrity_violations': []
            }
            
            for file_path in self.critical_files:
                if os.path.exists(file_path):
                    try:
                        # Calculate file checksum
                        with open(file_path, 'rb') as f:
                            file_hash = hashlib.sha256(f.read()).hexdigest()
                        
                        # Check if file has changed
                        if file_path in self.file_checksums:
                            if self.file_checksums[file_path] != file_hash:
                                metrics['file_changes'] += 1
                                metrics['unauthorized_modifications'] += 1
                                metrics['integrity_violations'].append({
                                    'file': file_path,
                                    'old_hash': self.file_checksums[file_path],
                                    'new_hash': file_hash,
                                    'timestamp': datetime.now().isoformat()
                                })
                                
                                # Send alert
                                statsd.event(
                                    title=f"Critical File Modified - {file_path}",
                                    text=f"Unauthorized modification detected in {file_path}",
                                    alert_type='error',
                                    tags=['security:file_integrity', f'file:{file_path}']
                                )
                        
                        # Update checksum
                        self.file_checksums[file_path] = file_hash
                        
                    except Exception as file_error:
                        logger.warning(f"Error checking file {file_path}: {file_error}")
                        continue
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error monitoring file integrity: {e}")
            return {}
    
    async def _monitor_process_anomalies(self) -> Dict[str, Any]:
        """Monitor for suspicious processes"""
        try:
            metrics = {
                'suspicious_processes': 0,
                'privilege_escalations': 0,
                'anomalous_behaviors': []
            }
            
            # Known suspicious process patterns
            suspicious_patterns = [
                r'.*nc.*-l.*',  # Netcat listeners
                r'.*python.*-c.*',  # Python one-liners
                r'.*sh.*-i.*',  # Interactive shells
                r'.*wget.*\|.*sh',  # Download and execute
                r'.*curl.*\|.*sh',  # Download and execute
            ]
            
            processes = psutil.process_iter(['pid', 'name', 'cmdline', 'username', 'create_time'])
            
            for proc in processes:
                try:
                    proc_info = proc.info
                    cmdline = ' '.join(proc_info.get('cmdline', []))
                    
                    # Check for suspicious command patterns
                    for pattern in suspicious_patterns:
                        if re.search(pattern, cmdline, re.IGNORECASE):
                            metrics['suspicious_processes'] += 1
                            metrics['anomalous_behaviors'].append({
                                'pid': proc_info['pid'],
                                'name': proc_info['name'],
                                'cmdline': cmdline,
                                'username': proc_info['username'],
                                'pattern_matched': pattern
                            })
                            
                            # Send alert
                            statsd.event(
                                title=f"Suspicious Process Detected - {proc_info['name']}",
                                text=f"Process {proc_info['pid']} matches suspicious pattern: {pattern}",
                                alert_type='warning',
                                tags=['security:process_monitoring', f'pid:{proc_info["pid"]}']
                            )
                    
                    # Check for privilege escalation (processes running as root)
                    if proc_info.get('username') == 'root':
                        # Check if this is an expected root process
                        expected_root_processes = ['systemd', 'kthreadd', 'ksoftirqd', 'rcu_', 'watchdog']
                        if not any(exp in proc_info['name'] for exp in expected_root_processes):
                            # This could be a privilege escalation
                            if 'sudo' in cmdline or 'su -' in cmdline:
                                metrics['privilege_escalations'] += 1
                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error monitoring process anomalies: {e}")
            return {}
    
    async def _monitor_certificate_expiration(self) -> Dict[str, Any]:
        """Monitor SSL certificate expiration"""
        try:
            metrics = {
                'expiring_certificates': 0,
                'expired_certificates': 0,
                'certificate_details': []
            }
            
            # List of services to check
            services_to_check = self.config.get('ssl_services', [
                {'host': 'localhost', 'port': 443},
                {'host': 'api.company.com', 'port': 443}
            ])
            
            for service in services_to_check:
                try:
                    host = service['host']
                    port = service['port']
                    
                    # Get certificate
                    context = ssl.create_default_context()
                    
                    with socket.create_connection((host, port), timeout=10) as sock:
                        with context.wrap_socket(sock, server_hostname=host) as ssock:
                            cert_der = ssock.getpeercert(binary_form=True)
                            
                    # Parse certificate
                    cert = x509.load_der_x509_certificate(cert_der, default_backend())
                    
                    # Check expiration
                    expiry_date = cert.not_valid_after
                    days_until_expiry = (expiry_date - datetime.now()).days
                    
                    cert_info = {
                        'host': host,
                        'port': port,
                        'subject': cert.subject.rfc4514_string(),
                        'issuer': cert.issuer.rfc4514_string(),
                        'expiry_date': expiry_date.isoformat(),
                        'days_until_expiry': days_until_expiry
                    }
                    
                    metrics['certificate_details'].append(cert_info)
                    
                    if days_until_expiry < 0:
                        metrics['expired_certificates'] += 1
                        
                        # Send critical alert
                        statsd.event(
                            title=f"Certificate Expired - {host}:{port}",
                            text=f"SSL certificate for {host}:{port} expired {abs(days_until_expiry)} days ago",
                            alert_type='error',
                            tags=['security:certificates', f'host:{host}', f'port:{port}']
                        )
                    elif days_until_expiry <= self.security_baselines['certificate_expiry_days']:
                        metrics['expiring_certificates'] += 1
                        
                        # Send warning alert
                        statsd.event(
                            title=f"Certificate Expiring Soon - {host}:{port}",
                            text=f"SSL certificate for {host}:{port} expires in {days_until_expiry} days",
                            alert_type='warning',
                            tags=['security:certificates', f'host:{host}', f'port:{port}']
                        )
                
                except Exception as cert_error:
                    logger.warning(f"Error checking certificate for {service}: {cert_error}")
                    continue
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error monitoring certificate expiration: {e}")
            return {}
    
    async def _block_ip(self, ip: str):
        """Block suspicious IP address"""
        try:
            # Use iptables to block the IP (Linux)
            if os.name == 'posix':
                subprocess.run(['iptables', '-A', 'INPUT', '-s', ip, '-j', 'DROP'], 
                             check=True, capture_output=True)
                logger.info(f"Blocked IP address: {ip}")
                
                # Send event to DataDog
                statsd.event(
                    title=f"IP Address Blocked - {ip}",
                    text=f"Automatically blocked suspicious IP address: {ip}",
                    alert_type='info',
                    tags=['security:ip_blocking', f'ip:{ip}']
                )
            
        except Exception as e:
            logger.warning(f"Error blocking IP {ip}: {e}")
    
    async def monitor_compliance(self) -> Dict[str, Any]:
        """Monitor compliance controls"""
        try:
            compliance_metrics = {}
            
            for framework in self.compliance_frameworks:
                if framework in self.compliance_controls:
                    framework_metrics = await self._monitor_framework_compliance(framework)
                    compliance_metrics[framework] = framework_metrics
            
            # Send compliance metrics
            await self._send_compliance_metrics(compliance_metrics)
            
            return compliance_metrics
            
        except Exception as e:
            logger.error(f"Error monitoring compliance: {e}")
            statsd.increment('compliance.monitoring.error')
            return {}
    
    async def _monitor_framework_compliance(self, framework: str) -> Dict[str, Any]:
        """Monitor compliance for a specific framework"""
        try:
            framework_controls = self.compliance_controls[framework]
            compliance_status = {}
            
            for control_id, control_info in framework_controls.items():
                control_metrics = await self._evaluate_control(framework, control_id, control_info)
                compliance_status[control_id] = control_metrics
            
            return compliance_status
            
        except Exception as e:
            logger.warning(f"Error monitoring framework {framework}: {e}")
            return {}
    
    async def _evaluate_control(self, framework: str, control_id: str, control_info: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate a specific compliance control"""
        try:
            metrics = {
                'compliant': True,
                'score': 100,
                'violations': [],
                'last_assessed': datetime.now().isoformat()
            }
            
            # Evaluate based on control type
            controls = control_info.get('controls', [])
            
            for control in controls:
                control_result = await self._check_control_implementation(framework, control)
                
                if not control_result.get('compliant', True):
                    metrics['compliant'] = False
                    metrics['score'] -= 20  # Reduce score for each violation
                    metrics['violations'].extend(control_result.get('violations', []))
            
            # Ensure score doesn't go below 0
            metrics['score'] = max(0, metrics['score'])
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Error evaluating control {control_id}: {e}")
            return {'compliant': False, 'score': 0}
    
    async def _check_control_implementation(self, framework: str, control: str) -> Dict[str, Any]:
        """Check implementation of a specific control"""
        try:
            result = {'compliant': True, 'violations': []}
            
            # Access control checks
            if control == 'access_control':
                # Check for proper access controls
                if not await self._verify_access_controls():
                    result['compliant'] = False
                    result['violations'].append('Inadequate access controls detected')
            
            # Authentication checks
            elif control == 'authentication':
                if not await self._verify_authentication_controls():
                    result['compliant'] = False
                    result['violations'].append('Weak authentication controls')
            
            # Encryption checks
            elif control == 'encryption':
                if not await self._verify_encryption():
                    result['compliant'] = False
                    result['violations'].append('Data encryption not properly implemented')
            
            # Network security checks
            elif control == 'network_segmentation':
                if not await self._verify_network_segmentation():
                    result['compliant'] = False
                    result['violations'].append('Network segmentation inadequate')
            
            # Audit logging checks
            elif control == 'audit_controls':
                if not await self._verify_audit_logging():
                    result['compliant'] = False
                    result['violations'].append('Audit logging not comprehensive')
            
            return result
            
        except Exception as e:
            logger.warning(f"Error checking control {control}: {e}")
            return {'compliant': False, 'violations': [f'Error checking control: {e}']}
    
    async def _verify_access_controls(self) -> bool:
        """Verify access control implementation"""
        try:
            # Check if proper RBAC is in place
            # This would integrate with your access control system
            # For now, we'll do basic checks
            
            # Check for default passwords
            default_passwords = ['admin', 'password', '123456', 'root']
            # In real implementation, this would check actual user accounts
            
            return True  # Placeholder
            
        except Exception as e:
            logger.warning(f"Error verifying access controls: {e}")
            return False
    
    async def _verify_authentication_controls(self) -> bool:
        """Verify authentication control implementation"""
        try:
            # Check for multi-factor authentication
            # Check password policies
            # Check session management
            
            return True  # Placeholder
            
        except Exception as e:
            logger.warning(f"Error verifying authentication controls: {e}")
            return False
    
    async def _verify_encryption(self) -> bool:
        """Verify encryption implementation"""
        try:
            # Check data at rest encryption
            # Check data in transit encryption
            # Check key management
            
            return True  # Placeholder
            
        except Exception as e:
            logger.warning(f"Error verifying encryption: {e}")
            return False
    
    async def _verify_network_segmentation(self) -> bool:
        """Verify network segmentation"""
        try:
            # Check network policies
            # Check firewall rules
            # Check VPC/subnet configuration
            
            return True  # Placeholder
            
        except Exception as e:
            logger.warning(f"Error verifying network segmentation: {e}")
            return False
    
    async def _verify_audit_logging(self) -> bool:
        """Verify audit logging implementation"""
        try:
            # Check if audit logs are enabled
            # Check log retention policies
            # Check log integrity
            
            audit_log_paths = ['/var/log/audit/audit.log', '/var/log/auth.log', '/var/log/syslog']
            
            for log_path in audit_log_paths:
                if os.path.exists(log_path):
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error verifying audit logging: {e}")
            return False
    
    async def _send_security_metrics(self, metrics: Dict[str, Any]):
        """Send security metrics to DataDog"""
        try:
            base_tags = ['scope:security']
            
            # Authentication metrics
            statsd.gauge('security.authentication.failed_logins', metrics.get('failed_logins', 0), tags=base_tags)
            statsd.gauge('security.authentication.successful_logins', metrics.get('successful_logins', 0), tags=base_tags)
            statsd.gauge('security.authentication.brute_force_attempts', metrics.get('brute_force_attempts', 0), tags=base_tags)
            
            # Network security metrics
            statsd.gauge('security.network.suspicious_connections', metrics.get('suspicious_connections', 0), tags=base_tags)
            statsd.gauge('security.network.port_scan_attempts', metrics.get('port_scan_attempts', 0), tags=base_tags)
            statsd.gauge('security.network.ddos_attempts', metrics.get('ddos_attempts', 0), tags=base_tags)
            
            # File integrity metrics
            statsd.gauge('security.file_integrity.changes', metrics.get('file_changes', 0), tags=base_tags)
            statsd.gauge('security.file_integrity.violations', metrics.get('unauthorized_modifications', 0), tags=base_tags)
            
            # Process monitoring metrics
            statsd.gauge('security.processes.suspicious', metrics.get('suspicious_processes', 0), tags=base_tags)
            statsd.gauge('security.processes.privilege_escalations', metrics.get('privilege_escalations', 0), tags=base_tags)
            
            # Certificate metrics
            statsd.gauge('security.certificates.expiring', metrics.get('expiring_certificates', 0), tags=base_tags)
            statsd.gauge('security.certificates.expired', metrics.get('expired_certificates', 0), tags=base_tags)
            
        except Exception as e:
            logger.warning(f"Error sending security metrics: {e}")
    
    async def _send_compliance_metrics(self, compliance_metrics: Dict[str, Any]):
        """Send compliance metrics to DataDog"""
        try:
            for framework, framework_data in compliance_metrics.items():
                framework_tags = ['scope:compliance', f'framework:{framework.lower()}']
                
                total_controls = len(framework_data)
                compliant_controls = sum(1 for control in framework_data.values() if control.get('compliant', False))
                compliance_percentage = (compliant_controls / total_controls * 100) if total_controls > 0 else 0
                
                statsd.gauge('compliance.framework.score', compliance_percentage, tags=framework_tags)
                statsd.gauge('compliance.framework.controls_total', total_controls, tags=framework_tags)
                statsd.gauge('compliance.framework.controls_compliant', compliant_controls, tags=framework_tags)
                
                # Individual control metrics
                for control_id, control_data in framework_data.items():
                    control_tags = framework_tags + [f'control:{control_id}']
                    
                    statsd.gauge('compliance.control.score', control_data.get('score', 0), tags=control_tags)
                    statsd.gauge('compliance.control.violations', len(control_data.get('violations', [])), tags=control_tags)
                    statsd.gauge('compliance.control.compliant', 1 if control_data.get('compliant', False) else 0, tags=control_tags)
            
        except Exception as e:
            logger.warning(f"Error sending compliance metrics: {e}")
    
    async def run_monitoring_loop(self):
        """Run continuous security and compliance monitoring loop"""
        logger.info("Starting security and compliance monitoring loop")
        
        security_last_run = 0
        compliance_last_run = 0
        
        while True:
            try:
                current_time = time.time()
                
                # Security monitoring (every monitoring_interval)
                if current_time - security_last_run >= self.monitoring_interval:
                    security_metrics = await self.monitor_security_events()
                    security_last_run = current_time
                    
                    logger.info(f"Security monitoring cycle completed: "
                               f"{security_metrics.get('failed_logins', 0)} failed logins, "
                               f"{security_metrics.get('suspicious_connections', 0)} suspicious connections")
                
                # Compliance monitoring (every compliance_scan_interval)
                if current_time - compliance_last_run >= self.compliance_scan_interval:
                    compliance_metrics = await self.monitor_compliance()
                    compliance_last_run = current_time
                    
                    logger.info(f"Compliance monitoring cycle completed for frameworks: "
                               f"{list(compliance_metrics.keys())}")
                
                # Send health check metric
                statsd.gauge('security.monitoring.health', 1, tags=['status:healthy'])
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in security monitoring loop: {e}")
                statsd.increment('security.monitoring.loop_error')
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Configuration example
SECURITY_COMPLIANCE_CONFIG = {
    'monitoring_interval': 300,  # 5 minutes
    'compliance_scan_interval': 3600,  # 1 hour
    'vulnerability_scan_interval': 21600,  # 6 hours
    'compliance_frameworks': ['SOC2', 'GDPR', 'HIPAA'],
    'monitored_ports': [22, 80, 443, 5432, 6379, 9200, 9300],
    'allowed_networks': ['10.0.0.0/8', '192.168.0.0/16', '172.16.0.0/12'],
    'critical_files': [
        '/etc/passwd', '/etc/shadow', '/etc/ssh/sshd_config',
        '/etc/kubernetes/manifests', '/etc/docker/daemon.json',
        '/etc/security/limits.conf', '/etc/sudoers'
    ],
    'ssl_services': [
        {'host': 'api.company.com', 'port': 443},
        {'host': 'app.company.com', 'port': 443},
        {'host': 'admin.company.com', 'port': 443}
    ],
    'audit_log_sources': [
        '/var/log/audit/audit.log',
        '/var/log/auth.log',
        '/var/log/syslog',
        '/var/log/kubernetes/audit.log'
    ]
}

async def main():
    """Main entry point for security and compliance monitoring"""
    monitor = DataDogSecurityComplianceMonitor(SECURITY_COMPLIANCE_CONFIG)
    await monitor.run_monitoring_loop()

if __name__ == "__main__":
    asyncio.run(main())