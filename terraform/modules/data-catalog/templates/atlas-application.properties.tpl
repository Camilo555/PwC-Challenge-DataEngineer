# Apache Atlas Configuration Template

# Atlas Server Configuration
atlas.rest.address=http://localhost:21000
atlas.server.http.port=21000
atlas.server.https.port=21443

# Storage Configuration
atlas.graph.storage.backend=hbase2
atlas.graph.storage.hbase.table=atlas_titan

# Database Configuration (PostgreSQL)
atlas.audit.hbase.zookeeper.quorum=${database_host}:2181
atlas.audit.hbase.tablename=atlas_audit

# Notification Configuration
atlas.notification.embedded=true
atlas.kafka.data=${ATLAS_HOME}/data/kafka
atlas.kafka.zookeeper.connect=localhost:2181
atlas.kafka.bootstrap.servers=localhost:9092

# Index Search Configuration
atlas.graph.index.search.backend=elasticsearch
atlas.graph.index.search.hostname=${database_host}
atlas.graph.index.search.port=9200

# Authentication Configuration
atlas.authentication.method.kerberos=false
atlas.authentication.method.file=true
atlas.authentication.method.file.filename=${ATLAS_HOME}/conf/users-credentials.properties

# Authorization Configuration
atlas.authorizer.impl=simple
atlas.authorizer.simple.authz.policy.file=${ATLAS_HOME}/conf/atlas-simple-authz-policy.json

# Lineage Configuration
atlas.lineage.schema.query.hive_table=true
atlas.lineage.schema.query.hbase_table=true

# REST API Configuration
atlas.rest.csrf.enabled=false
atlas.metric.enable=true

# Log Configuration
atlas.log.dir=${ATLAS_HOME}/logs
atlas.log.level=WARN

# Data Lake Integration
atlas.hook.hive.synchronous=false
atlas.hook.hive.numRetries=3
atlas.hook.hive.queueSize=10000

# Multi-cloud Metadata Sync
atlas.metadata.sync.enabled=true
atlas.metadata.sync.aws.glue.enabled=true
atlas.metadata.sync.azure.purview.enabled=true
atlas.metadata.sync.gcp.datacatalog.enabled=true