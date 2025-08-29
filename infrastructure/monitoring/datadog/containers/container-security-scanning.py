#!/usr/bin/env python3
"""
Container Security Scanning and Monitoring Integration
Provides automated security scanning for Docker containers and images
"""

import json
import subprocess
import logging
import requests
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import docker
from datadog import api, initialize

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class VulnerabilityReport:
    """Container vulnerability report"""
    image_name: str
    image_tag: str
    scan_date: datetime
    critical_count: int
    high_count: int
    medium_count: int
    low_count: int
    total_count: int
    cve_list: List[str]
    compliance_status: str

@dataclass
class ContainerSecurityMetrics:
    """Container security metrics"""
    container_id: str
    container_name: str
    image_name: str
    security_score: float
    vulnerability_count: int
    compliance_violations: int
    last_scan_time: datetime
    risk_level: str

class ContainerSecurityScanner:
    """Container security scanning and monitoring"""
    
    def __init__(self, config: Dict):
        """Initialize container security scanner"""
        self.config = config
        self.docker_client = docker.from_env()
        
        # Initialize DataDog
        initialize(
            api_key=config.get('datadog_api_key'),
            app_key=config.get('datadog_app_key'),
            api_host=config.get('datadog_host', 'https://api.datadoghq.com')
        )
        
        # Security scanning tools configuration
        self.scanning_tools = {
            'trivy': config.get('trivy_enabled', True),
            'clair': config.get('clair_enabled', False),
            'anchore': config.get('anchore_enabled', False)
        }
        
        # Compliance frameworks
        self.compliance_frameworks = [
            'CIS Docker Benchmark',
            'NIST Container Security',
            'SOC2',
            'GDPR',
            'HIPAA'
        ]
        
    def scan_image_vulnerabilities(self, image_name: str, image_tag: str = 'latest') -> VulnerabilityReport:
        """Scan container image for vulnerabilities"""
        logger.info(f"Scanning image vulnerabilities: {image_name}:{image_tag}")
        
        full_image_name = f"{image_name}:{image_tag}"
        
        try:
            # Use Trivy for vulnerability scanning
            if self.scanning_tools.get('trivy'):
                return self._scan_with_trivy(full_image_name)
            
            # Fallback to other scanning tools
            elif self.scanning_tools.get('clair'):
                return self._scan_with_clair(full_image_name)
            
            elif self.scanning_tools.get('anchore'):
                return self._scan_with_anchore(full_image_name)
            
            else:
                logger.warning("No vulnerability scanning tools enabled")
                return self._create_empty_report(image_name, image_tag)
                
        except Exception as e:
            logger.error(f"Error scanning image {full_image_name}: {e}")
            return self._create_empty_report(image_name, image_tag)
    
    def _scan_with_trivy(self, image_name: str) -> VulnerabilityReport:
        """Scan image with Trivy"""
        try:
            # Run Trivy vulnerability scan
            cmd = [
                'trivy', 'image', 
                '--format', 'json',
                '--quiet',
                '--no-progress',
                image_name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                logger.error(f"Trivy scan failed: {result.stderr}")
                return self._create_empty_report(*image_name.split(':'))
            
            # Parse Trivy output
            scan_data = json.loads(result.stdout)
            return self._parse_trivy_results(scan_data, image_name)
            
        except subprocess.TimeoutExpired:
            logger.error(f"Trivy scan timeout for image: {image_name}")
            return self._create_empty_report(*image_name.split(':'))
        except Exception as e:
            logger.error(f"Trivy scan error: {e}")
            return self._create_empty_report(*image_name.split(':'))
    
    def _parse_trivy_results(self, scan_data: Dict, image_name: str) -> VulnerabilityReport:
        """Parse Trivy scan results"""
        image_name_parts = image_name.split(':')
        image_name = image_name_parts[0]
        image_tag = image_name_parts[1] if len(image_name_parts) > 1 else 'latest'
        
        critical_count = 0
        high_count = 0
        medium_count = 0
        low_count = 0
        cve_list = []
        
        # Parse vulnerability results
        results = scan_data.get('Results', [])
        for result in results:
            vulnerabilities = result.get('Vulnerabilities', [])
            for vuln in vulnerabilities:
                severity = vuln.get('Severity', 'UNKNOWN').upper()
                cve_id = vuln.get('VulnerabilityID', '')
                
                if cve_id:
                    cve_list.append(cve_id)
                
                if severity == 'CRITICAL':
                    critical_count += 1
                elif severity == 'HIGH':
                    high_count += 1
                elif severity == 'MEDIUM':
                    medium_count += 1
                elif severity == 'LOW':
                    low_count += 1
        
        total_count = critical_count + high_count + medium_count + low_count
        
        # Determine compliance status
        compliance_status = self._determine_compliance_status(critical_count, high_count)
        
        return VulnerabilityReport(
            image_name=image_name,
            image_tag=image_tag,
            scan_date=datetime.now(),
            critical_count=critical_count,
            high_count=high_count,
            medium_count=medium_count,
            low_count=low_count,
            total_count=total_count,
            cve_list=cve_list,
            compliance_status=compliance_status
        )
    
    def _determine_compliance_status(self, critical_count: int, high_count: int) -> str:
        """Determine compliance status based on vulnerability counts"""
        if critical_count > 0:
            return "NON_COMPLIANT_CRITICAL"
        elif high_count > 5:
            return "NON_COMPLIANT_HIGH"
        elif high_count > 0:
            return "WARNING"
        else:
            return "COMPLIANT"
    
    def _create_empty_report(self, image_name: str, image_tag: str) -> VulnerabilityReport:
        """Create empty vulnerability report"""
        return VulnerabilityReport(
            image_name=image_name,
            image_tag=image_tag,
            scan_date=datetime.now(),
            critical_count=0,
            high_count=0,
            medium_count=0,
            low_count=0,
            total_count=0,
            cve_list=[],
            compliance_status="UNKNOWN"
        )
    
    def scan_running_containers(self) -> List[ContainerSecurityMetrics]:
        """Scan all running containers"""
        logger.info("Scanning running containers")
        
        container_metrics = []
        containers = self.docker_client.containers.list()
        
        for container in containers:
            try:
                # Get container information
                container_info = container.attrs
                container_name = container_info.get('Name', '').lstrip('/')
                image_name = container_info.get('Config', {}).get('Image', '')
                
                # Skip DataDog agent containers
                if 'datadog' in container_name.lower():
                    continue
                
                # Perform security assessment
                security_metrics = self._assess_container_security(container, container_info)
                container_metrics.append(security_metrics)
                
            except Exception as e:
                logger.error(f"Error scanning container {container.id}: {e}")
                continue
        
        return container_metrics
    
    def _assess_container_security(self, container, container_info: Dict) -> ContainerSecurityMetrics:
        """Assess container security"""
        container_name = container_info.get('Name', '').lstrip('/')
        image_name = container_info.get('Config', {}).get('Image', '')
        
        # Calculate security score based on various factors
        security_score = self._calculate_security_score(container_info)
        
        # Check for compliance violations
        compliance_violations = self._check_compliance_violations(container_info)
        
        # Determine risk level
        risk_level = self._determine_risk_level(security_score, compliance_violations)
        
        return ContainerSecurityMetrics(
            container_id=container.id,
            container_name=container_name,
            image_name=image_name,
            security_score=security_score,
            vulnerability_count=0,  # To be filled by vulnerability scan
            compliance_violations=compliance_violations,
            last_scan_time=datetime.now(),
            risk_level=risk_level
        )
    
    def _calculate_security_score(self, container_info: Dict) -> float:
        """Calculate container security score (0-100)"""
        score = 100.0
        
        # Check if running as root
        if container_info.get('Config', {}).get('User') == 'root' or not container_info.get('Config', {}).get('User'):
            score -= 20
        
        # Check privileged mode
        if container_info.get('HostConfig', {}).get('Privileged'):
            score -= 30
        
        # Check network mode
        network_mode = container_info.get('HostConfig', {}).get('NetworkMode')
        if network_mode == 'host':
            score -= 15
        
        # Check volume mounts
        binds = container_info.get('HostConfig', {}).get('Binds', [])
        for bind in binds:
            if ':/var/run/docker.sock' in bind or ':/proc' in bind or ':/sys' in bind:
                score -= 10
        
        # Check capabilities
        cap_add = container_info.get('HostConfig', {}).get('CapAdd', [])
        if 'SYS_ADMIN' in cap_add or 'ALL' in cap_add:
            score -= 25
        
        return max(0.0, score)
    
    def _check_compliance_violations(self, container_info: Dict) -> int:
        """Check for compliance violations"""
        violations = 0
        
        # CIS Docker Benchmark checks
        if container_info.get('HostConfig', {}).get('Privileged'):
            violations += 1
        
        if not container_info.get('Config', {}).get('User'):
            violations += 1
        
        if container_info.get('HostConfig', {}).get('NetworkMode') == 'host':
            violations += 1
        
        # Check for sensitive volume mounts
        binds = container_info.get('HostConfig', {}).get('Binds', [])
        for bind in binds:
            if any(path in bind for path in ['/var/run/docker.sock', '/proc', '/sys', '/etc']):
                violations += 1
        
        return violations
    
    def _determine_risk_level(self, security_score: float, violations: int) -> str:
        """Determine container risk level"""
        if security_score < 50 or violations > 3:
            return "HIGH"
        elif security_score < 75 or violations > 1:
            return "MEDIUM"
        else:
            return "LOW"
    
    def send_metrics_to_datadog(self, vulnerability_report: VulnerabilityReport, container_metrics: List[ContainerSecurityMetrics]):
        """Send security metrics to DataDog"""
        logger.info("Sending security metrics to DataDog")
        
        try:
            # Send vulnerability metrics
            self._send_vulnerability_metrics(vulnerability_report)
            
            # Send container security metrics
            for metrics in container_metrics:
                self._send_container_security_metrics(metrics)
                
        except Exception as e:
            logger.error(f"Error sending metrics to DataDog: {e}")
    
    def _send_vulnerability_metrics(self, report: VulnerabilityReport):
        """Send vulnerability metrics to DataDog"""
        timestamp = int(time.time())
        
        metrics = [
            {
                'metric': 'container.security.vulnerabilities.critical',
                'points': [(timestamp, report.critical_count)],
                'tags': [f'image:{report.image_name}', f'tag:{report.image_tag}', 'env:production']
            },
            {
                'metric': 'container.security.vulnerabilities.high', 
                'points': [(timestamp, report.high_count)],
                'tags': [f'image:{report.image_name}', f'tag:{report.image_tag}', 'env:production']
            },
            {
                'metric': 'container.security.vulnerabilities.medium',
                'points': [(timestamp, report.medium_count)],
                'tags': [f'image:{report.image_name}', f'tag:{report.image_tag}', 'env:production']
            },
            {
                'metric': 'container.security.vulnerabilities.low',
                'points': [(timestamp, report.low_count)],
                'tags': [f'image:{report.image_name}', f'tag:{report.image_tag}', 'env:production']
            },
            {
                'metric': 'container.security.vulnerabilities.total',
                'points': [(timestamp, report.total_count)],
                'tags': [f'image:{report.image_name}', f'tag:{report.image_tag}', 'env:production']
            }
        ]
        
        # Send metrics to DataDog
        for metric in metrics:
            api.Metric.send(**metric)
    
    def _send_container_security_metrics(self, metrics: ContainerSecurityMetrics):
        """Send container security metrics to DataDog"""
        timestamp = int(time.time())
        
        datadog_metrics = [
            {
                'metric': 'container.security.score',
                'points': [(timestamp, metrics.security_score)],
                'tags': [
                    f'container_name:{metrics.container_name}',
                    f'image:{metrics.image_name}',
                    f'risk_level:{metrics.risk_level}',
                    'env:production'
                ]
            },
            {
                'metric': 'container.security.compliance_violations',
                'points': [(timestamp, metrics.compliance_violations)],
                'tags': [
                    f'container_name:{metrics.container_name}',
                    f'image:{metrics.image_name}',
                    f'risk_level:{metrics.risk_level}',
                    'env:production'
                ]
            }
        ]
        
        # Send metrics to DataDog
        for metric in datadog_metrics:
            api.Metric.send(**metric)
    
    def generate_security_alerts(self, vulnerability_report: VulnerabilityReport, container_metrics: List[ContainerSecurityMetrics]):
        """Generate security alerts based on scan results"""
        logger.info("Generating security alerts")
        
        # Critical vulnerability alert
        if vulnerability_report.critical_count > 0:
            self._send_alert(
                title=f"Critical Vulnerabilities Detected: {vulnerability_report.image_name}",
                text=f"Found {vulnerability_report.critical_count} critical vulnerabilities in {vulnerability_report.image_name}:{vulnerability_report.image_tag}",
                alert_type='error',
                tags=['security', 'vulnerability', 'critical']
            )
        
        # High-risk container alert
        high_risk_containers = [m for m in container_metrics if m.risk_level == 'HIGH']
        if high_risk_containers:
            self._send_alert(
                title=f"High-Risk Containers Detected",
                text=f"Found {len(high_risk_containers)} high-risk containers running",
                alert_type='warning',
                tags=['security', 'container', 'high-risk']
            )
    
    def _send_alert(self, title: str, text: str, alert_type: str, tags: List[str]):
        """Send alert to DataDog"""
        try:
            api.Event.create(
                title=title,
                text=text,
                alert_type=alert_type,
                tags=tags + ['team:data-platform', 'env:production'],
                source_type_name='container-security'
            )
        except Exception as e:
            logger.error(f"Error sending alert to DataDog: {e}")

def main():
    """Main execution function"""
    config = {
        'datadog_api_key': 'your-datadog-api-key',
        'datadog_app_key': 'your-datadog-app-key',
        'trivy_enabled': True,
        'clair_enabled': False,
        'anchore_enabled': False
    }
    
    scanner = ContainerSecurityScanner(config)
    
    try:
        # Scan running containers
        container_metrics = scanner.scan_running_containers()
        
        # Scan specific images for vulnerabilities
        critical_images = [
            'data-platform/ingestion:latest',
            'data-platform/processing:latest',
            'data-platform/analytics:latest'
        ]
        
        for image in critical_images:
            name_tag = image.split(':')
            image_name = name_tag[0]
            image_tag = name_tag[1] if len(name_tag) > 1 else 'latest'
            
            vulnerability_report = scanner.scan_image_vulnerabilities(image_name, image_tag)
            
            # Send metrics to DataDog
            scanner.send_metrics_to_datadog(vulnerability_report, container_metrics)
            
            # Generate alerts if needed
            scanner.generate_security_alerts(vulnerability_report, container_metrics)
        
        logger.info("Container security scanning completed successfully")
        
    except Exception as e:
        logger.error(f"Container security scanning failed: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())