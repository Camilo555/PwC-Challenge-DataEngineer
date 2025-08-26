"""
Monitoring Dashboard
Provides web-based dashboard for system monitoring and observability.
"""
from __future__ import annotations

import asyncio
from datetime import datetime

try:
    import uvicorn
    from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    from fastapi.templating import Jinja2Templates
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

from core.logging import get_logger
from core.monitoring.alerting import AlertSeverity, alert_manager
from core.monitoring.health_checks import health_manager
from core.monitoring.metrics import default_collector

logger = get_logger(__name__)


class MonitoringDashboard:
    """Web-based monitoring dashboard."""

    def __init__(self, title: str = "PwC Data Engineer Challenge - Monitoring",
                 host: str = "localhost", port: int = 8080):
        if not FASTAPI_AVAILABLE:
            raise ImportError("FastAPI not available. Install with: pip install fastapi uvicorn jinja2")

        self.title = title
        self.host = host
        self.port = port

        # Create FastAPI app
        self.app = FastAPI(title=title, description="Monitoring Dashboard")

        # Setup templates and static files
        self._setup_routes()

        # Background tasks
        self._monitoring_tasks = []

    def _setup_routes(self):
        """Setup dashboard routes."""

        # Static dashboard page
        @self.app.get("/", response_class=HTMLResponse)
        async def dashboard_home(request: Request):
            return self._render_dashboard_html()

        # API endpoints
        @self.app.get("/api/health")
        async def get_health():
            """Get system health status."""
            try:
                # Run health checks
                health_results = await health_manager.check_all()
                overall_status = health_manager.get_overall_status()

                return {
                    "overall_status": overall_status.value,
                    "timestamp": datetime.utcnow().isoformat(),
                    "components": {
                        name: {
                            "status": result.status.value,
                            "message": result.message,
                            "response_time_ms": result.response_time_ms,
                            "details": result.details or {}
                        }
                        for name, result in health_results.items()
                    }
                }
            except Exception as e:
                logger.error(f"Error getting health status: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/metrics/summary")
        async def get_metrics_summary():
            """Get metrics summary."""
            try:
                summary = default_collector.get_metrics_summary()
                return summary
            except Exception as e:
                logger.error(f"Error getting metrics summary: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/metrics/jobs")
        async def get_job_metrics():
            """Get ETL job metrics."""
            try:
                job_metrics = default_collector.get_all_job_metrics()
                return {
                    name: {
                        "job_name": metrics.job_name,
                        "status": metrics.status,
                        "start_time": metrics.start_time.isoformat(),
                        "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
                        "duration_seconds": metrics.duration_seconds,
                        "records_processed": metrics.records_processed,
                        "records_failed": metrics.records_failed,
                        "success_rate": metrics.success_rate,
                        "throughput_per_second": metrics.throughput_per_second,
                        "error_count": len(metrics.errors)
                    }
                    for name, metrics in job_metrics.items()
                }
            except Exception as e:
                logger.error(f"Error getting job metrics: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/alerts")
        async def get_alerts():
            """Get active alerts."""
            try:
                active_alerts = alert_manager.get_active_alerts()
                return {
                    "active_alerts": [alert.to_dict() for alert in active_alerts],
                    "summary": alert_manager.get_alert_summary()
                }
            except Exception as e:
                logger.error(f"Error getting alerts: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/alerts/history")
        async def get_alert_history(limit: int = 50):
            """Get alert history."""
            try:
                history = alert_manager.get_alert_history(limit)
                return [alert.to_dict() for alert in history]
            except Exception as e:
                logger.error(f"Error getting alert history: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/alerts/{alert_id}/acknowledge")
        async def acknowledge_alert(alert_id: str, request: Request):
            """Acknowledge an alert."""
            try:
                body = await request.json()
                acknowledged_by = body.get("acknowledged_by", "dashboard_user")

                success = await alert_manager.acknowledge_alert(alert_id, acknowledged_by)
                if success:
                    return {"status": "acknowledged", "alert_id": alert_id}
                else:
                    raise HTTPException(status_code=404, detail="Alert not found")

            except Exception as e:
                logger.error(f"Error acknowledging alert {alert_id}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/alerts/{alert_id}/resolve")
        async def resolve_alert(alert_id: str):
            """Resolve an alert."""
            try:
                success = await alert_manager.resolve_alert(alert_id)
                if success:
                    return {"status": "resolved", "alert_id": alert_id}
                else:
                    raise HTTPException(status_code=404, detail="Alert not found")

            except Exception as e:
                logger.error(f"Error resolving alert {alert_id}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/metrics/history/{metric_name}")
        async def get_metric_history(metric_name: str, limit: int = 100):
            """Get metric history."""
            try:
                history = default_collector.get_metric_history(metric_name, limit=limit)
                return [
                    {
                        "timestamp": point.timestamp.isoformat(),
                        "value": point.value,
                        "labels": point.labels
                    }
                    for point in history
                ]
            except Exception as e:
                logger.error(f"Error getting metric history for {metric_name}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        # Messaging system endpoints
        @self.app.get("/api/messaging/rabbitmq")
        async def get_rabbitmq_metrics():
            """Get RabbitMQ metrics and health status."""
            try:
                # This would integrate with the messaging monitoring system
                # For now, return mock data structure
                return {
                    "status": "healthy",
                    "queues": {
                        "task_queue": {"depth": 25, "consumers": 3, "rate": 12.5},
                        "result_queue": {"depth": 5, "consumers": 2, "rate": 8.2},
                        "etl_queue": {"depth": 150, "consumers": 5, "rate": 45.3}
                    },
                    "connections": 8,
                    "channels": 15,
                    "memory_usage_mb": 125,
                    "timestamp": datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting RabbitMQ metrics: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/messaging/kafka")
        async def get_kafka_metrics():
            """Get Kafka metrics and health status."""
            try:
                # This would integrate with the messaging monitoring system
                # For now, return mock data structure
                return {
                    "status": "healthy",
                    "brokers": 3,
                    "topics": {
                        "retail-transactions": {"partitions": 3, "lag": 245},
                        "customer-events": {"partitions": 2, "lag": 12},
                        "system-events": {"partitions": 1, "lag": 0}
                    },
                    "total_consumer_lag": 257,
                    "messages_per_sec": 125.7,
                    "under_replicated_partitions": 0,
                    "offline_partitions": 0,
                    "timestamp": datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting Kafka metrics: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/messaging/health")  
        async def get_messaging_health():
            """Get overall messaging system health."""
            try:
                return {
                    "overall_status": "healthy",
                    "components": {
                        "rabbitmq": {
                            "status": "healthy",
                            "queues_healthy": 3,
                            "queues_warning": 1,
                            "queues_critical": 0
                        },
                        "kafka": {
                            "status": "healthy",
                            "brokers_online": 3,
                            "topics_healthy": 8,
                            "consumer_lag_critical": 0
                        }
                    },
                    "alerts": {
                        "active": 2,
                        "critical": 0,
                        "warning": 2
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting messaging health: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        # System info endpoint
        @self.app.get("/api/system/info")
        async def get_system_info():
            """Get system information."""
            try:
                import platform

                import psutil

                return {
                    "platform": platform.platform(),
                    "python_version": platform.python_version(),
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                    "disk_total_gb": psutil.disk_usage('/').total / (1024**3),
                    "uptime": datetime.utcnow().isoformat()  # App uptime would be tracked separately
                }
            except Exception as e:
                logger.error(f"Error getting system info: {e}")
                raise HTTPException(status_code=500, detail=str(e))

    def _render_dashboard_html(self) -> str:
        """Render the main dashboard HTML."""
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>""" + self.title + """</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }
                
                .header {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 20px;
                }
                
                .dashboard-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }
                
                .card {
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }
                
                .card h3 {
                    margin-top: 0;
                    color: #333;
                }
                
                .status-indicator {
                    display: inline-block;
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    margin-right: 8px;
                }
                
                .status-healthy { background-color: #4CAF50; }
                .status-degraded { background-color: #FF9800; }
                .status-unhealthy { background-color: #F44336; }
                .status-unknown { background-color: #9E9E9E; }
                
                .metric-value {
                    font-size: 2em;
                    font-weight: bold;
                    color: #2196F3;
                }
                
                .alert-item {
                    padding: 10px;
                    margin: 5px 0;
                    border-left: 4px solid;
                    border-radius: 4px;
                    background-color: #f9f9f9;
                }
                
                .alert-critical { border-left-color: #9C27B0; }
                .alert-error { border-left-color: #F44336; }
                .alert-warning { border-left-color: #FF9800; }
                .alert-info { border-left-color: #2196F3; }
                
                .job-status {
                    display: inline-block;
                    padding: 4px 8px;
                    border-radius: 4px;
                    color: white;
                    font-size: 0.8em;
                }
                
                .job-running { background-color: #2196F3; }
                .job-completed { background-color: #4CAF50; }
                .job-failed { background-color: #F44336; }
                
                .refresh-btn {
                    background: #2196F3;
                    color: white;
                    border: none;
                    padding: 10px 20px;
                    border-radius: 5px;
                    cursor: pointer;
                    margin: 10px 0;
                }
                
                .refresh-btn:hover {
                    background: #1976D2;
                }
                
                .table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 10px;
                }
                
                .table th, .table td {
                    padding: 8px 12px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }
                
                .table th {
                    background-color: #f5f5f5;
                    font-weight: 600;
                }
                
                #refreshIndicator {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: #4CAF50;
                    color: white;
                    padding: 10px 15px;
                    border-radius: 5px;
                    display: none;
                }
            </style>
        </head>
        <body>
            <div id="refreshIndicator">Updated ✓</div>
            
            <div class="header">
                <h1>""" + self.title + """</h1>
                <p>Real-time monitoring dashboard</p>
                <button class="refresh-btn" onclick="refreshAll()">Refresh All</button>
                <span id="lastUpdate"></span>
            </div>
            
            <!-- Overall Status -->
            <div class="dashboard-grid">
                <div class="card">
                    <h3>System Health</h3>
                    <div id="overallHealth">
                        <div class="status-indicator status-unknown"></div>
                        Loading...
                    </div>
                    <div id="healthComponents"></div>
                </div>
                
                <div class="card">
                    <h3>Active Alerts</h3>
                    <div class="metric-value" id="activeAlertCount">-</div>
                    <div id="alertSummary"></div>
                </div>
                
                <div class="card">
                    <h3>ETL Jobs</h3>
                    <div id="jobsSummary">
                        <div>Running: <span id="runningJobs">-</span></div>
                        <div>Completed: <span id="completedJobs">-</span></div>
                        <div>Failed: <span id="failedJobs">-</span></div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>System Resources</h3>
                    <div id="systemResources">Loading...</div>
                </div>
                
                <div class="card">
                    <h3>RabbitMQ Status</h3>
                    <div id="rabbitmqStatus">Loading...</div>
                </div>
                
                <div class="card">
                    <h3>Kafka Status</h3>
                    <div id="kafkaStatus">Loading...</div>
                </div>
            </div>
            
            <!-- Detailed Views -->
            <div class="dashboard-grid">
                <div class="card">
                    <h3>Recent Jobs</h3>
                    <div id="recentJobs"></div>
                </div>
                
                <div class="card">
                    <h3>Active Alerts</h3>
                    <div id="activeAlerts"></div>
                </div>
            </div>
            
            <div class="dashboard-grid">
                <div class="card">
                    <h3>Health Check Details</h3>
                    <div id="healthDetails"></div>
                </div>
                
                <div class="card">
                    <h3>System Information</h3>
                    <div id="systemInfo"></div>
                </div>
            </div>
            
            <script>
                let refreshInterval;
                
                async function fetchAPI(endpoint) {
                    try {
                        const response = await fetch(`/api${endpoint}`);
                        if (!response.ok) throw new Error(`HTTP ${response.status}`);
                        return await response.json();
                    } catch (error) {
                        console.error(`Error fetching ${endpoint}:`, error);
                        return null;
                    }
                }
                
                function showRefreshIndicator() {
                    const indicator = document.getElementById('refreshIndicator');
                    indicator.style.display = 'block';
                    setTimeout(() => {
                        indicator.style.display = 'none';
                    }, 1500);
                }
                
                async function updateHealth() {
                    const health = await fetchAPI('/health');
                    if (!health) return;
                    
                    const overallElement = document.getElementById('overallHealth');
                    const statusClass = `status-${health.overall_status}`;
                    overallElement.innerHTML = `
                        <div class="status-indicator ${statusClass}"></div>
                        System Status: ${health.overall_status.toUpperCase()}
                    `;
                    
                    const componentsElement = document.getElementById('healthComponents');
                    let componentsHtml = '<div class="table-container"><table class="table"><thead><tr><th>Component</th><th>Status</th><th>Response Time</th></tr></thead><tbody>';
                    
                    Object.entries(health.components).forEach(([name, component]) => {
                        const statusClass = `status-${component.status}`;
                        componentsHtml += `
                            <tr>
                                <td>${name}</td>
                                <td><span class="status-indicator ${statusClass}"></span>${component.status}</td>
                                <td>${component.response_time_ms?.toFixed(1)}ms</td>
                            </tr>
                        `;
                    });
                    
                    componentsHtml += '</tbody></table></div>';
                    componentsElement.innerHTML = componentsHtml;
                    
                    // Update detailed health view
                    const detailsElement = document.getElementById('healthDetails');
                    let detailsHtml = '';
                    Object.entries(health.components).forEach(([name, component]) => {
                        const statusClass = `status-${component.status}`;
                        detailsHtml += `
                            <div style="margin-bottom: 10px;">
                                <strong><span class="status-indicator ${statusClass}"></span>${name}</strong><br>
                                <small>${component.message}</small>
                            </div>
                        `;
                    });
                    detailsElement.innerHTML = detailsHtml;
                }
                
                async function updateAlerts() {
                    const alerts = await fetchAPI('/alerts');
                    if (!alerts) return;
                    
                    document.getElementById('activeAlertCount').textContent = alerts.active_alerts.length;
                    
                    const summaryElement = document.getElementById('alertSummary');
                    const summary = alerts.summary.active_by_severity;
                    summaryElement.innerHTML = `
                        <div>Critical: ${summary.critical || 0}</div>
                        <div>Error: ${summary.error || 0}</div>
                        <div>Warning: ${summary.warning || 0}</div>
                        <div>Info: ${summary.info || 0}</div>
                    `;
                    
                    const activeAlertsElement = document.getElementById('activeAlerts');
                    if (alerts.active_alerts.length === 0) {
                        activeAlertsElement.innerHTML = '<p>No active alerts ✓</p>';
                    } else {
                        let alertsHtml = '';
                        alerts.active_alerts.forEach(alert => {
                            alertsHtml += `
                                <div class="alert-item alert-${alert.severity}">
                                    <strong>${alert.title}</strong><br>
                                    <small>${alert.description}</small><br>
                                    <small>${new Date(alert.timestamp).toLocaleString()}</small>
                                </div>
                            `;
                        });
                        activeAlertsElement.innerHTML = alertsHtml;
                    }
                }
                
                async function updateJobs() {
                    const jobs = await fetchAPI('/metrics/jobs');
                    if (!jobs) return;
                    
                    const jobsArray = Object.values(jobs);
                    const running = jobsArray.filter(j => j.status === 'running').length;
                    const completed = jobsArray.filter(j => j.status === 'completed').length;
                    const failed = jobsArray.filter(j => j.status === 'failed').length;
                    
                    document.getElementById('runningJobs').textContent = running;
                    document.getElementById('completedJobs').textContent = completed;
                    document.getElementById('failedJobs').textContent = failed;
                    
                    const recentJobsElement = document.getElementById('recentJobs');
                    if (jobsArray.length === 0) {
                        recentJobsElement.innerHTML = '<p>No job data available</p>';
                    } else {
                        let jobsHtml = '<table class="table"><thead><tr><th>Job</th><th>Status</th><th>Records</th><th>Success Rate</th></tr></thead><tbody>';
                        
                        jobsArray.slice(-10).forEach(job => {
                            jobsHtml += `
                                <tr>
                                    <td>${job.job_name}</td>
                                    <td><span class="job-status job-${job.status}">${job.status}</span></td>
                                    <td>${job.records_processed.toLocaleString()}</td>
                                    <td>${(job.success_rate * 100).toFixed(1)}%</td>
                                </tr>
                            `;
                        });
                        
                        jobsHtml += '</tbody></table>';
                        recentJobsElement.innerHTML = jobsHtml;
                    }
                }
                
                async function updateSystemInfo() {
                    const systemInfo = await fetchAPI('/system/info');
                    if (!systemInfo) return;
                    
                    const infoElement = document.getElementById('systemInfo');
                    infoElement.innerHTML = `
                        <div><strong>Platform:</strong> ${systemInfo.platform}</div>
                        <div><strong>Python:</strong> ${systemInfo.python_version}</div>
                        <div><strong>CPU Cores:</strong> ${systemInfo.cpu_count}</div>
                        <div><strong>Memory:</strong> ${systemInfo.memory_total_gb.toFixed(1)} GB</div>
                        <div><strong>Disk:</strong> ${systemInfo.disk_total_gb.toFixed(1)} GB</div>
                    `;
                }
                
                async function updateRabbitMQ() {
                    const rabbitmq = await fetchAPI('/messaging/rabbitmq');
                    if (!rabbitmq) return;
                    
                    const statusElement = document.getElementById('rabbitmqStatus');
                    const statusClass = `status-${rabbitmq.status}`;
                    
                    let queuesHtml = '<div class="table-container"><table class="table"><thead><tr><th>Queue</th><th>Depth</th><th>Consumers</th><th>Rate</th></tr></thead><tbody>';
                    
                    Object.entries(rabbitmq.queues).forEach(([name, stats]) => {
                        queuesHtml += `
                            <tr>
                                <td>${name}</td>
                                <td>${stats.depth}</td>
                                <td>${stats.consumers}</td>
                                <td>${stats.rate.toFixed(1)}/s</td>
                            </tr>
                        `;
                    });
                    
                    queuesHtml += '</tbody></table></div>';
                    
                    statusElement.innerHTML = `
                        <div><span class="status-indicator ${statusClass}"></span>${rabbitmq.status}</div>
                        <div><strong>Connections:</strong> ${rabbitmq.connections}</div>
                        <div><strong>Channels:</strong> ${rabbitmq.channels}</div>
                        <div><strong>Memory:</strong> ${rabbitmq.memory_usage_mb} MB</div>
                        ${queuesHtml}
                    `;
                }
                
                async function updateKafka() {
                    const kafka = await fetchAPI('/messaging/kafka');
                    if (!kafka) return;
                    
                    const statusElement = document.getElementById('kafkaStatus');
                    const statusClass = `status-${kafka.status}`;
                    
                    let topicsHtml = '<div class="table-container"><table class="table"><thead><tr><th>Topic</th><th>Partitions</th><th>Lag</th></tr></thead><tbody>';
                    
                    Object.entries(kafka.topics).forEach(([name, stats]) => {
                        topicsHtml += `
                            <tr>
                                <td>${name}</td>
                                <td>${stats.partitions}</td>
                                <td>${stats.lag}</td>
                            </tr>
                        `;
                    });
                    
                    topicsHtml += '</tbody></table></div>';
                    
                    statusElement.innerHTML = `
                        <div><span class="status-indicator ${statusClass}"></span>${kafka.status}</div>
                        <div><strong>Brokers:</strong> ${kafka.brokers}</div>
                        <div><strong>Total Lag:</strong> ${kafka.total_consumer_lag}</div>
                        <div><strong>Messages/sec:</strong> ${kafka.messages_per_sec.toFixed(1)}</div>
                        <div><strong>Under-replicated:</strong> ${kafka.under_replicated_partitions}</div>
                        <div><strong>Offline:</strong> ${kafka.offline_partitions}</div>
                        ${topicsHtml}
                    `;
                }
                
                async function refreshAll() {
                    await Promise.all([
                        updateHealth(),
                        updateAlerts(), 
                        updateJobs(),
                        updateSystemInfo(),
                        updateRabbitMQ(),
                        updateKafka()
                    ]);
                    
                    document.getElementById('lastUpdate').textContent = 
                        `Last updated: ${new Date().toLocaleTimeString()}`;
                    showRefreshIndicator();
                }
                
                // Initialize dashboard
                document.addEventListener('DOMContentLoaded', function() {
                    refreshAll();
                    
                    // Auto-refresh every 30 seconds
                    refreshInterval = setInterval(refreshAll, 30000);
                });
                
                // Stop auto-refresh when page is not visible
                document.addEventListener('visibilitychange', function() {
                    if (document.hidden) {
                        if (refreshInterval) {
                            clearInterval(refreshInterval);
                        }
                    } else {
                        refreshInterval = setInterval(refreshAll, 30000);
                        refreshAll(); // Refresh immediately when page becomes visible
                    }
                });
            </script>
        </body>
        </html>
        """

    def start_background_monitoring(self):
        """Start background monitoring tasks."""
        async def monitoring_loop():
            while True:
                try:
                    # Update metrics from various sources
                    await self._collect_system_metrics()
                    await asyncio.sleep(30)  # Collect every 30 seconds
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}")
                    await asyncio.sleep(30)

        # Start monitoring loop
        task = asyncio.create_task(monitoring_loop())
        self._monitoring_tasks.append(task)
        logger.info("Background monitoring started")

    async def _collect_system_metrics(self):
        """Collect system metrics for dashboard."""
        try:
            import psutil

            # System resource metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Update metrics collector
            default_collector.set_gauge("system_cpu_percent", cpu_percent)
            default_collector.set_gauge("system_memory_percent", memory.percent)
            default_collector.set_gauge("system_disk_percent", disk.percent)

        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")

    async def run(self):
        """Run the dashboard server."""
        if not FASTAPI_AVAILABLE:
            logger.error("FastAPI not available - cannot start dashboard")
            return

        logger.info(f"Starting monitoring dashboard on {self.host}:{self.port}")

        # Start background monitoring
        self.start_background_monitoring()

        try:
            config = uvicorn.Config(
                self.app,
                host=self.host,
                port=self.port,
                log_level="info"
            )
            server = uvicorn.Server(config)
            await server.serve()

        except Exception as e:
            logger.error(f"Error running dashboard server: {e}")
            raise
        finally:
            # Clean up background tasks
            for task in self._monitoring_tasks:
                task.cancel()

    def run_sync(self):
        """Run dashboard in sync mode."""
        asyncio.run(self.run())


# Utility functions for dashboard setup

def create_monitoring_dashboard(title: str = "System Monitoring Dashboard",
                              host: str = "localhost",
                              port: int = 8080) -> MonitoringDashboard:
    """Create and configure monitoring dashboard."""
    dashboard = MonitoringDashboard(title=title, host=host, port=port)
    logger.info(f"Monitoring dashboard created at http://{host}:{port}")
    return dashboard


async def setup_monitoring_stack(database_url: str | None = None,
                                rabbitmq_host: str | None = None,
                                kafka_servers: list[str] | None = None,
                                enable_dashboard: bool = True,
                                dashboard_port: int = 8080):
    """Setup complete monitoring stack."""
    from core.monitoring.alerting import (
        LogAlertChannel,
        create_health_check_rule,
        create_system_resource_rule,
    )
    from core.monitoring.health_checks import setup_basic_health_checks

    # Setup health checks
    setup_basic_health_checks(
        database_url=database_url,
        rabbitmq_host=rabbitmq_host,
        kafka_servers=kafka_servers,
        file_paths=["/tmp", "/var/log"] if database_url else None
    )

    # Setup basic alert rules
    alert_manager.add_rule(create_health_check_rule("database"))
    alert_manager.add_rule(create_health_check_rule("rabbitmq"))
    alert_manager.add_rule(create_health_check_rule("kafka"))
    alert_manager.add_rule(create_system_resource_rule("cpu", 80.0))
    alert_manager.add_rule(create_system_resource_rule("memory", 85.0))

    # Setup alert channels
    log_channel = LogAlertChannel("system_log")
    alert_manager.add_channel(log_channel)

    # Route all alerts to log channel
    for severity in [AlertSeverity.INFO, AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
        alert_manager.set_channel_routing(severity, ["system_log"])

    # Start dashboard if requested
    dashboard = None
    if enable_dashboard and FASTAPI_AVAILABLE:
        dashboard = create_monitoring_dashboard(port=dashboard_port)

        # Start dashboard in background
        dashboard_task = asyncio.create_task(dashboard.run())
        logger.info(f"Monitoring dashboard available at http://localhost:{dashboard_port}")

    logger.info("Monitoring stack setup complete")
    return dashboard
