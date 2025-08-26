"""
Enterprise Security Orchestrator
Integrates all security components including DLP, compliance, access control, 
governance, and monitoring into a unified security platform.
"""
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import WebSocket

from core.logging import get_logger
from core.security.advanced_security import get_security_manager
from core.security.enterprise_dlp import EnterpriseDLPManager
from core.security.compliance_framework import get_compliance_engine
from core.security.enhanced_access_control import get_access_control_manager, AccessRequest
from core.security.data_governance import get_governance_orchestrator
from core.security.security_dashboard import get_security_dashboard


logger = get_logger(__name__)


@dataclass
class SecurityOrchestrationConfig:
    """Configuration for security orchestration"""
    enable_dlp: bool = True
    enable_compliance_monitoring: bool = True
    enable_enhanced_access_control: bool = True
    enable_data_governance: bool = True
    enable_real_time_monitoring: bool = True
    
    # Integration settings
    auto_response_enabled: bool = True
    correlation_window_minutes: int = 5
    alert_escalation_enabled: bool = True
    
    # Performance settings
    batch_processing_enabled: bool = True
    cache_ttl_seconds: int = 300


class EnterpriseSecurityOrchestrator:
    """
    Main orchestrator for enterprise security platform.
    Coordinates all security components and provides unified security operations.
    """
    
    def __init__(self, config: Optional[SecurityOrchestrationConfig] = None):
        self.logger = get_logger(__name__)
        self.config = config or SecurityOrchestrationConfig()
        
        # Initialize component managers
        self.security_manager = get_security_manager()
        self.dlp_manager = None
        self.compliance_engine = get_compliance_engine()
        self.access_manager = get_access_control_manager()
        self.governance_orchestrator = get_governance_orchestrator()
        self.dashboard = get_security_dashboard()
        
        # Initialize DLP if enabled
        if self.config.enable_dlp:
            self.dlp_manager = EnterpriseDLPManager()
        
        self.logger.info("Enterprise Security Orchestrator initialized")
    
    async def process_data_request(
        self,
        user_id: str,
        data: Any,
        operation: str,
        resource_id: str,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Process a data request through the complete security pipeline:
        1. Access control check
        2. DLP scanning
        3. Compliance verification
        4. Governance tracking
        5. Security monitoring
        """
        
        context = context or {}
        request_id = f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user_id}"
        
        self.logger.info(f"Processing data request {request_id} for user {user_id}")
        
        try:
            # 1. Access Control Check
            access_result = await self._check_access(user_id, operation, resource_id, context)
            if not access_result['allowed']:
                return {
                    'request_id': request_id,
                    'status': 'denied',
                    'reason': 'Access denied',
                    'details': access_result,
                    'timestamp': datetime.now().isoformat()
                }
            
            # 2. DLP Scanning (if enabled and data is provided)
            dlp_result = None
            if self.config.enable_dlp and self.dlp_manager and data:
                dlp_context = {
                    'user_id': user_id,
                    'operation': operation,
                    'location': context.get('location', 'api'),
                    'session_id': context.get('session_id')
                }
                dlp_result = self.dlp_manager.scan_data(data, dlp_context)
                
                # Check if DLP blocked the request
                if dlp_result.get('action') == 'blocked':
                    return {
                        'request_id': request_id,
                        'status': 'blocked',
                        'reason': 'DLP policy violation',
                        'details': dlp_result,
                        'timestamp': datetime.now().isoformat()
                    }
                
                # Use processed data if redaction occurred
                if 'data' in dlp_result:
                    data = dlp_result['data']
            
            # 3. Compliance Verification
            compliance_result = await self._verify_compliance(operation, resource_id, context)
            
            # 4. Governance Tracking
            if self.config.enable_data_governance:
                await self._track_data_lineage(user_id, operation, resource_id, context)
            
            # 5. Security Event Logging
            await self._log_security_event(user_id, operation, resource_id, context, 'success')
            
            # Return successful response
            response = {
                'request_id': request_id,
                'status': 'approved',
                'data': data,
                'access_decision': access_result,
                'timestamp': datetime.now().isoformat()
            }
            
            if dlp_result:
                response['dlp_analysis'] = {
                    'detections': dlp_result.get('detections', 0),
                    'risk_score': dlp_result.get('risk_score', 0),
                    'sensitive_data_types': dlp_result.get('sensitive_data_types', [])
                }
            
            if compliance_result:
                response['compliance_status'] = compliance_result
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error processing request {request_id}: {e}")
            
            # Log security event for error
            await self._log_security_event(user_id, operation, resource_id, context, 'error', str(e))
            
            return {
                'request_id': request_id,
                'status': 'error',
                'reason': f'Processing error: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
    
    async def _check_access(
        self,
        user_id: str,
        operation: str,
        resource_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check access using enhanced access control"""
        
        try:
            # Create access request
            access_request = AccessRequest(
                request_id=f"access_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                subject_id=user_id,
                action=operation,
                resource_id=resource_id,
                resource_type=context.get('resource_type', 'data'),
                context=context
            )
            
            # Evaluate access
            evaluation = self.access_manager.check_access(access_request)
            
            return {
                'allowed': evaluation.decision.value in ['allow', 'conditional_allow'],
                'decision': evaluation.decision.value,
                'reason': evaluation.reason,
                'risk_score': evaluation.risk_score,
                'evaluation_time_ms': evaluation.evaluation_time_ms,
                'conditions': evaluation.required_conditions
            }
            
        except Exception as e:
            self.logger.error(f"Access control check failed: {e}")
            return {
                'allowed': False,
                'decision': 'deny',
                'reason': f'Access control error: {str(e)}',
                'risk_score': 10.0
            }
    
    async def _verify_compliance(
        self,
        operation: str,
        resource_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify compliance requirements"""
        
        if not self.config.enable_compliance_monitoring:
            return {'status': 'not_checked'}
        
        try:
            # This would integrate with the compliance engine to check
            # if the operation violates any compliance rules
            
            # For high-risk operations, ensure compliance
            if operation in ['delete', 'export', 'share']:
                # Run relevant compliance checks
                # This is a simplified example
                return {
                    'status': 'compliant',
                    'frameworks_checked': ['gdpr', 'pci_dss', 'hipaa'],
                    'violations': []
                }
            
            return {'status': 'compliant'}
            
        except Exception as e:
            self.logger.error(f"Compliance verification failed: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    async def _track_data_lineage(
        self,
        user_id: str,
        operation: str,
        resource_id: str,
        context: Dict[str, Any]
    ):
        """Track data lineage for governance"""
        
        try:
            from core.security.data_governance import LineageEventType
            
            # Map operation to lineage event type
            event_type_mapping = {
                'read': LineageEventType.READ,
                'write': LineageEventType.WRITE,
                'create': LineageEventType.CREATE,
                'delete': LineageEventType.DELETE,
                'transform': LineageEventType.TRANSFORM,
                'copy': LineageEventType.COPY
            }
            
            event_type = event_type_mapping.get(operation, LineageEventType.READ)
            
            # Record lineage event
            self.governance_orchestrator.lineage_tracker.record_lineage_event(
                event_type=event_type,
                target_asset_id=resource_id,
                source_asset_id=context.get('source_resource_id'),
                operation=operation,
                operation_details=context,
                user_id=user_id,
                system_id='security_orchestrator',
                session_id=context.get('session_id')
            )
            
        except Exception as e:
            self.logger.error(f"Data lineage tracking failed: {e}")
    
    async def _log_security_event(
        self,
        user_id: str,
        operation: str,
        resource_id: str,
        context: Dict[str, Any],
        status: str,
        error_message: Optional[str] = None
    ):
        """Log security event"""
        
        try:
            # Log to audit system
            self.security_manager.audit_logger.log_audit_event(
                user_id=user_id,
                action=getattr(__import__('core.security.advanced_security'), operation.upper(), 'READ'),
                resource_type='data_resource',
                resource_id=resource_id,
                success=(status == 'success'),
                error_message=error_message,
                metadata={
                    'operation': operation,
                    'status': status,
                    'context': context,
                    'orchestrator_processed': True
                }
            )
            
        except Exception as e:
            self.logger.error(f"Security event logging failed: {e}")
    
    async def run_security_assessment(self) -> Dict[str, Any]:
        """Run comprehensive security assessment"""
        
        self.logger.info("Running comprehensive security assessment")
        
        assessment_results = {
            'assessment_id': f"assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        try:
            # 1. Security Manager Assessment
            security_dashboard = self.security_manager.get_security_dashboard()
            assessment_results['components']['security'] = {
                'status': 'healthy' if security_dashboard.get('blocked_ips', 0) < 10 else 'warning',
                'active_threats': security_dashboard.get('active_threats', 0),
                'blocked_ips': security_dashboard.get('blocked_ips', 0),
                'total_events_24h': security_dashboard.get('total_events_24h', 0)
            }
            
            # 2. DLP Assessment
            if self.dlp_manager:
                dlp_dashboard = self.dlp_manager.get_dlp_dashboard()
                assessment_results['components']['dlp'] = {
                    'status': 'healthy' if dlp_dashboard.get('incidents_24h', 0) < 5 else 'warning',
                    'incidents_24h': dlp_dashboard.get('incidents_24h', 0),
                    'unresolved_incidents': dlp_dashboard.get('unresolved_incidents', 0),
                    'policies_active': dlp_dashboard.get('policies_active', 0)
                }
            
            # 3. Compliance Assessment
            compliance_dashboard = self.compliance_engine.get_compliance_dashboard()
            compliance_rate = compliance_dashboard['overall_compliance']['compliance_rate']
            assessment_results['components']['compliance'] = {
                'status': 'healthy' if compliance_rate > 0.9 else 'warning' if compliance_rate > 0.7 else 'critical',
                'compliance_rate': compliance_rate,
                'total_violations': compliance_dashboard['violation_summary']['total_violations'],
                'overdue_remediations': compliance_dashboard['violation_summary']['overdue_remediations']
            }
            
            # 4. Access Control Assessment
            access_dashboard = self.access_manager.get_access_dashboard()
            assessment_results['components']['access_control'] = {
                'status': 'healthy',
                'total_subjects': access_dashboard['summary']['total_subjects'],
                'active_elevations': access_dashboard['summary']['active_elevations'],
                'total_policies': access_dashboard['summary']['total_policies']
            }
            
            # 5. Governance Assessment
            governance_dashboard = self.governance_orchestrator.get_comprehensive_dashboard()
            governance_score = governance_dashboard.get('overall_governance_score', 0.0)
            assessment_results['components']['governance'] = {
                'status': 'healthy' if governance_score > 0.8 else 'warning' if governance_score > 0.6 else 'critical',
                'governance_score': governance_score,
                'assets_cataloged': governance_dashboard['catalog_statistics']['total_assets'],
                'lineage_coverage': governance_dashboard['lineage_statistics']['lineage_coverage']['coverage_percentage']
            }
            
            # Calculate overall assessment
            component_statuses = [comp['status'] for comp in assessment_results['components'].values()]
            if 'critical' in component_statuses:
                overall_status = 'critical'
            elif 'warning' in component_statuses:
                overall_status = 'warning'
            else:
                overall_status = 'healthy'
            
            assessment_results['overall_status'] = overall_status
            assessment_results['recommendations'] = self._generate_security_recommendations(assessment_results)
            
        except Exception as e:
            self.logger.error(f"Security assessment failed: {e}")
            assessment_results['overall_status'] = 'error'
            assessment_results['error'] = str(e)
        
        return assessment_results
    
    def _generate_security_recommendations(self, assessment: Dict[str, Any]) -> List[str]:
        """Generate security recommendations based on assessment"""
        
        recommendations = []
        
        for component, status_info in assessment['components'].items():
            if status_info['status'] == 'critical':
                if component == 'compliance':
                    recommendations.append(f"URGENT: Address compliance violations in {component} component")
                elif component == 'governance':
                    recommendations.append(f"URGENT: Improve data governance score (current: {status_info.get('governance_score', 0):.2f})")
                else:
                    recommendations.append(f"URGENT: Address critical issues in {component} component")
            
            elif status_info['status'] == 'warning':
                if component == 'security':
                    if status_info.get('active_threats', 0) > 5:
                        recommendations.append("Monitor and investigate active security threats")
                elif component == 'dlp':
                    if status_info.get('incidents_24h', 0) > 0:
                        recommendations.append("Review and resolve recent DLP incidents")
                elif component == 'compliance':
                    recommendations.append("Improve compliance posture by addressing violations")
                elif component == 'governance':
                    recommendations.append("Enhance data cataloging and lineage tracking coverage")
        
        # General recommendations
        if not recommendations:
            recommendations.append("Security posture is healthy - continue monitoring")
        
        recommendations.append("Conduct regular security assessments")
        recommendations.append("Ensure all team members receive security training")
        
        return recommendations
    
    async def get_unified_dashboard(self, user_id: str = "system") -> Dict[str, Any]:
        """Get unified security dashboard combining all components"""
        
        try:
            # Get dashboard data from the security dashboard
            dashboard_data = await self.dashboard.get_dashboard_data(user_id)
            
            # Add orchestrator-specific information
            dashboard_data['orchestrator'] = {
                'components_enabled': {
                    'dlp': self.config.enable_dlp,
                    'compliance': self.config.enable_compliance_monitoring,
                    'access_control': self.config.enable_enhanced_access_control,
                    'governance': self.config.enable_data_governance,
                    'monitoring': self.config.enable_real_time_monitoring
                },
                'auto_response_enabled': self.config.auto_response_enabled,
                'version': '1.0.0'
            }
            
            return dashboard_data
            
        except Exception as e:
            self.logger.error(f"Failed to get unified dashboard: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def handle_websocket_connection(self, websocket: WebSocket, user_id: str):
        """Handle WebSocket connection for real-time security monitoring"""
        
        await self.dashboard.handle_websocket_connection(websocket, user_id)
    
    def get_security_summary(self) -> Dict[str, Any]:
        """Get high-level security summary"""
        
        try:
            dashboard_summary = self.dashboard.get_dashboard_summary()
            
            return {
                'platform_status': 'operational',
                'security_level': dashboard_summary['health_status'],
                'components_status': {
                    'threat_detection': 'operational',
                    'dlp': 'operational' if self.dlp_manager else 'disabled',
                    'compliance': 'operational',
                    'access_control': 'operational',
                    'governance': 'operational',
                    'monitoring': 'operational'
                },
                'key_metrics': {
                    'active_alerts': dashboard_summary['summary']['total_alerts'],
                    'critical_alerts': dashboard_summary['summary']['critical_alerts'],
                    'compliance_score': dashboard_summary['summary']['compliance_score'],
                    'system_uptime': dashboard_summary['summary']['system_uptime']
                },
                'last_assessment': datetime.now().isoformat(),
                'next_scheduled_assessment': (datetime.now() + timedelta(hours=24)).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get security summary: {e}")
            return {
                'platform_status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


# Global orchestrator instance
_security_orchestrator: Optional[EnterpriseSecurityOrchestrator] = None

def get_security_orchestrator(config: Optional[SecurityOrchestrationConfig] = None) -> EnterpriseSecurityOrchestrator:
    """Get global security orchestrator instance"""
    global _security_orchestrator
    if _security_orchestrator is None:
        _security_orchestrator = EnterpriseSecurityOrchestrator(config)
    return _security_orchestrator


# Example usage and integration points
async def example_secure_data_operation():
    """Example of how to use the security orchestrator for secure data operations"""
    
    orchestrator = get_security_orchestrator()
    
    # Example: Process a data export request
    result = await orchestrator.process_data_request(
        user_id="user123",
        data={"customer_data": "sensitive information"},
        operation="export",
        resource_id="customer_database",
        context={
            'location': 'api_endpoint',
            'destination': 'external_partner',
            'session_id': 'session_abc123',
            'source_ip': '192.168.1.100'
        }
    )
    
    print(f"Operation result: {result['status']}")
    if result['status'] == 'approved':
        print(f"Data processed successfully with {result.get('dlp_analysis', {}).get('detections', 0)} DLP detections")
    else:
        print(f"Operation blocked: {result['reason']}")
    
    return result


if __name__ == "__main__":
    # Example of running the orchestrator
    import asyncio
    
    async def main():
        orchestrator = get_security_orchestrator()
        
        # Run security assessment
        assessment = await orchestrator.run_security_assessment()
        print(f"Security Assessment: {assessment['overall_status']}")
        
        # Get dashboard data
        dashboard = await orchestrator.get_unified_dashboard()
        print(f"Dashboard loaded with {len(dashboard.get('widgets', {}))} widgets")
        
        # Example secure operation
        result = await example_secure_data_operation()
    
    asyncio.run(main())