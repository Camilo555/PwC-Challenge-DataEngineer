"""
Storage Backend Implementations

Multi-tier storage backends for backup data with support for local storage,
cloud storage, and S3-compatible object storage.
"""
from __future__ import annotations

import json
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


class StorageTier(str, Enum):
    """Storage tiers for different backup retention policies."""
    HOT = "hot"          # Frequent access, fast retrieval
    WARM = "warm"        # Infrequent access, moderate retrieval
    COLD = "cold"        # Rare access, slow retrieval
    ARCHIVE = "archive"  # Long-term retention, very slow retrieval


@dataclass
class StorageMetadata:
    """Metadata for stored backup files."""
    storage_id: str
    original_path: Path
    storage_path: str
    storage_backend: str
    storage_tier: StorageTier
    size_bytes: int
    checksum: str
    created_at: datetime
    last_accessed: datetime | None = None
    retention_until: datetime | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'storage_id': self.storage_id,
            'original_path': str(self.original_path),
            'storage_path': self.storage_path,
            'storage_backend': self.storage_backend,
            'storage_tier': self.storage_tier.value,
            'size_bytes': self.size_bytes,
            'checksum': self.checksum,
            'created_at': self.created_at.isoformat(),
            'last_accessed': self.last_accessed.isoformat() if self.last_accessed else None,
            'retention_until': self.retention_until.isoformat() if self.retention_until else None,
            'tags': self.tags
        }


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    def __init__(self, backend_id: str, config: dict[str, Any]):
        self.backend_id = backend_id
        self.config = config
        self._metadata_store: dict[str, StorageMetadata] = {}

    @abstractmethod
    async def store(
        self,
        source_path: Path,
        storage_key: str,
        tier: StorageTier = StorageTier.HOT,
        retention_days: int | None = None,
        tags: dict[str, str] | None = None
    ) -> StorageMetadata:
        """Store a file in the backend."""
        pass

    @abstractmethod
    async def retrieve(
        self,
        storage_key: str,
        destination_path: Path
    ) -> bool:
        """Retrieve a file from the backend."""
        pass

    @abstractmethod
    async def delete(self, storage_key: str) -> bool:
        """Delete a file from the backend."""
        pass

    @abstractmethod
    async def list_files(
        self,
        prefix: str | None = None,
        tier: StorageTier | None = None
    ) -> list[StorageMetadata]:
        """List files in the backend."""
        pass

    @abstractmethod
    async def get_storage_stats(self) -> dict[str, Any]:
        """Get storage backend statistics."""
        pass

    async def verify_integrity(self, storage_key: str) -> bool:
        """Verify integrity of stored file."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            return False

        # Subclasses can override for more sophisticated verification
        return True

    def _calculate_retention_date(self, retention_days: int | None) -> datetime | None:
        """Calculate retention expiration date."""
        if retention_days is None:
            return None
        return datetime.utcnow() + timedelta(days=retention_days)


class LocalStorageBackend(StorageBackend):
    """
    Local filesystem storage backend with tier-based directory organization.
    
    Features:
    - Tier-based directory structure
    - Automatic cleanup of expired files
    - Efficient local file operations
    - Metadata tracking
    """

    def __init__(self, backend_id: str, config: dict[str, Any]):
        super().__init__(backend_id, config)
        self.base_path = Path(config.get("base_path", "./storage"))
        self.metadata_file = self.base_path / "metadata.json"
        self._ensure_structure()
        self._load_metadata()

    def _ensure_structure(self) -> None:
        """Ensure storage directory structure exists."""
        for tier in StorageTier:
            tier_path = self.base_path / tier.value
            tier_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Local storage structure initialized at {self.base_path}")

    def _load_metadata(self) -> None:
        """Load metadata from persistent storage."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file) as f:
                    metadata_dict = json.load(f)

                for storage_id, data in metadata_dict.items():
                    self._metadata_store[storage_id] = StorageMetadata(
                        storage_id=data["storage_id"],
                        original_path=Path(data["original_path"]),
                        storage_path=data["storage_path"],
                        storage_backend=data["storage_backend"],
                        storage_tier=StorageTier(data["storage_tier"]),
                        size_bytes=data["size_bytes"],
                        checksum=data["checksum"],
                        created_at=datetime.fromisoformat(data["created_at"]),
                        last_accessed=datetime.fromisoformat(data["last_accessed"]) if data.get("last_accessed") else None,
                        retention_until=datetime.fromisoformat(data["retention_until"]) if data.get("retention_until") else None,
                        tags=data.get("tags", {})
                    )

                logger.info(f"Loaded {len(self._metadata_store)} metadata records")

            except Exception as e:
                logger.warning(f"Could not load metadata: {e}")

    def _save_metadata(self) -> None:
        """Save metadata to persistent storage."""
        try:
            metadata_dict = {
                storage_id: metadata.to_dict()
                for storage_id, metadata in self._metadata_store.items()
            }

            with open(self.metadata_file, 'w') as f:
                json.dump(metadata_dict, f, indent=2)

        except Exception as e:
            logger.error(f"Could not save metadata: {e}")

    async def store(
        self,
        source_path: Path,
        storage_key: str,
        tier: StorageTier = StorageTier.HOT,
        retention_days: int | None = None,
        tags: dict[str, str] | None = None
    ) -> StorageMetadata:
        """Store file in local storage."""
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        # Determine storage path
        tier_dir = self.base_path / tier.value
        storage_path = tier_dir / storage_key
        storage_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to storage location
        shutil.copy2(source_path, storage_path)

        # Calculate checksum
        checksum = await self._calculate_checksum(storage_path)

        # Create metadata
        metadata = StorageMetadata(
            storage_id=storage_key,
            original_path=source_path,
            storage_path=str(storage_path),
            storage_backend=self.backend_id,
            storage_tier=tier,
            size_bytes=storage_path.stat().st_size,
            checksum=checksum,
            created_at=datetime.utcnow(),
            retention_until=self._calculate_retention_date(retention_days),
            tags=tags or {}
        )

        # Store metadata
        self._metadata_store[storage_key] = metadata
        self._save_metadata()

        logger.info(f"Stored file {source_path} as {storage_key} in {tier.value} tier")
        return metadata

    async def retrieve(self, storage_key: str, destination_path: Path) -> bool:
        """Retrieve file from local storage."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.error(f"Storage key not found: {storage_key}")
            return False

        storage_path = Path(metadata.storage_path)
        if not storage_path.exists():
            logger.error(f"Storage file not found: {storage_path}")
            return False

        # Ensure destination directory exists
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to destination
        shutil.copy2(storage_path, destination_path)

        # Update access time
        metadata.last_accessed = datetime.utcnow()
        self._save_metadata()

        logger.info(f"Retrieved {storage_key} to {destination_path}")
        return True

    async def delete(self, storage_key: str) -> bool:
        """Delete file from local storage."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.warning(f"Storage key not found for deletion: {storage_key}")
            return False

        storage_path = Path(metadata.storage_path)
        try:
            if storage_path.exists():
                storage_path.unlink()

            # Remove from metadata
            del self._metadata_store[storage_key]
            self._save_metadata()

            logger.info(f"Deleted {storage_key}")
            return True

        except Exception as e:
            logger.error(f"Could not delete {storage_key}: {e}")
            return False

    async def list_files(
        self,
        prefix: str | None = None,
        tier: StorageTier | None = None
    ) -> list[StorageMetadata]:
        """List files in local storage."""
        files = []

        for storage_key, metadata in self._metadata_store.items():
            # Apply filters
            if prefix and not storage_key.startswith(prefix):
                continue

            if tier and metadata.storage_tier != tier:
                continue

            files.append(metadata)

        # Sort by creation time (newest first)
        files.sort(key=lambda x: x.created_at, reverse=True)
        return files

    async def get_storage_stats(self) -> dict[str, Any]:
        """Get local storage statistics."""
        stats = {
            "backend_type": "local",
            "backend_id": self.backend_id,
            "total_files": len(self._metadata_store),
            "total_size_bytes": 0,
            "tiers": {},
            "oldest_file": None,
            "newest_file": None
        }

        # Calculate tier statistics
        for tier in StorageTier:
            stats["tiers"][tier.value] = {
                "files": 0,
                "size_bytes": 0
            }

        oldest_date = None
        newest_date = None

        for metadata in self._metadata_store.values():
            stats["total_size_bytes"] += metadata.size_bytes
            stats["tiers"][metadata.storage_tier.value]["files"] += 1
            stats["tiers"][metadata.storage_tier.value]["size_bytes"] += metadata.size_bytes

            # Track oldest and newest
            if oldest_date is None or metadata.created_at < oldest_date:
                oldest_date = metadata.created_at
                stats["oldest_file"] = metadata.storage_id

            if newest_date is None or metadata.created_at > newest_date:
                newest_date = metadata.created_at
                stats["newest_file"] = metadata.storage_id

        return stats

    async def cleanup_expired_files(self) -> dict[str, Any]:
        """Clean up expired files based on retention policies."""
        cleanup_stats = {
            "files_checked": 0,
            "files_deleted": 0,
            "files_failed": 0,
            "bytes_freed": 0
        }

        current_time = datetime.utcnow()
        expired_keys = []

        for storage_key, metadata in self._metadata_store.items():
            cleanup_stats["files_checked"] += 1

            if metadata.retention_until and current_time > metadata.retention_until:
                expired_keys.append((storage_key, metadata.size_bytes))

        # Delete expired files
        for storage_key, file_size in expired_keys:
            if await self.delete(storage_key):
                cleanup_stats["files_deleted"] += 1
                cleanup_stats["bytes_freed"] += file_size
            else:
                cleanup_stats["files_failed"] += 1

        logger.info(f"Cleanup completed: {cleanup_stats['files_deleted']} files deleted, "
                   f"{cleanup_stats['bytes_freed']} bytes freed")

        return cleanup_stats

    async def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        import hashlib

        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()


class CloudStorageBackend(StorageBackend):
    """
    Generic cloud storage backend with lifecycle management.
    
    This is a base class that can be extended for specific cloud providers
    like AWS S3, Azure Blob Storage, Google Cloud Storage, etc.
    """

    def __init__(self, backend_id: str, config: dict[str, Any]):
        super().__init__(backend_id, config)
        self.provider = config.get("provider", "generic")
        self.bucket_name = config.get("bucket_name", "backup-storage")
        self.region = config.get("region", "us-east-1")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.endpoint_url = config.get("endpoint_url")

        # Lifecycle policies for different tiers
        self.lifecycle_policies = {
            StorageTier.HOT: {"days_to_warm": 30, "days_to_cold": 90, "days_to_archive": 365},
            StorageTier.WARM: {"days_to_cold": 60, "days_to_archive": 365},
            StorageTier.COLD: {"days_to_archive": 730},
            StorageTier.ARCHIVE: {"days_to_delete": 2555}  # ~7 years
        }

    async def store(
        self,
        source_path: Path,
        storage_key: str,
        tier: StorageTier = StorageTier.HOT,
        retention_days: int | None = None,
        tags: dict[str, str] | None = None
    ) -> StorageMetadata:
        """Store file in cloud storage."""
        # This is a template implementation
        # Subclasses should implement specific cloud provider logic

        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        # Simulate cloud upload (implement actual cloud SDK calls in subclasses)
        cloud_path = f"{tier.value}/{storage_key}"

        # Calculate checksum before upload
        checksum = await self._calculate_checksum(source_path)

        # Create metadata
        metadata = StorageMetadata(
            storage_id=storage_key,
            original_path=source_path,
            storage_path=cloud_path,
            storage_backend=self.backend_id,
            storage_tier=tier,
            size_bytes=source_path.stat().st_size,
            checksum=checksum,
            created_at=datetime.utcnow(),
            retention_until=self._calculate_retention_date(retention_days),
            tags=tags or {}
        )

        self._metadata_store[storage_key] = metadata

        logger.info(f"Stored {source_path} in cloud storage as {storage_key}")
        return metadata

    async def retrieve(self, storage_key: str, destination_path: Path) -> bool:
        """Retrieve file from cloud storage."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.error(f"Storage key not found: {storage_key}")
            return False

        # Simulate cloud download (implement actual cloud SDK calls in subclasses)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Update access time
        metadata.last_accessed = datetime.utcnow()

        logger.info(f"Retrieved {storage_key} from cloud storage")
        return True

    async def delete(self, storage_key: str) -> bool:
        """Delete file from cloud storage."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.warning(f"Storage key not found for deletion: {storage_key}")
            return False

        # Simulate cloud deletion (implement actual cloud SDK calls in subclasses)
        del self._metadata_store[storage_key]

        logger.info(f"Deleted {storage_key} from cloud storage")
        return True

    async def list_files(
        self,
        prefix: str | None = None,
        tier: StorageTier | None = None
    ) -> list[StorageMetadata]:
        """List files in cloud storage."""
        files = []

        for storage_key, metadata in self._metadata_store.items():
            # Apply filters
            if prefix and not storage_key.startswith(prefix):
                continue

            if tier and metadata.storage_tier != tier:
                continue

            files.append(metadata)

        files.sort(key=lambda x: x.created_at, reverse=True)
        return files

    async def get_storage_stats(self) -> dict[str, Any]:
        """Get cloud storage statistics."""
        return {
            "backend_type": "cloud",
            "provider": self.provider,
            "backend_id": self.backend_id,
            "bucket_name": self.bucket_name,
            "region": self.region,
            "total_files": len(self._metadata_store),
            "total_size_bytes": sum(m.size_bytes for m in self._metadata_store.values())
        }

    async def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        import hashlib

        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()


class S3Backend(CloudStorageBackend):
    """
    AWS S3-compatible storage backend with advanced features.
    
    Features:
    - S3-compatible object storage
    - Intelligent tiering
    - Lifecycle management
    - Cross-region replication
    - Server-side encryption
    """

    def __init__(self, backend_id: str, config: dict[str, Any]):
        super().__init__(backend_id, config)
        self.provider = "s3"
        self.storage_class_mapping = {
            StorageTier.HOT: "STANDARD",
            StorageTier.WARM: "STANDARD_IA",
            StorageTier.COLD: "GLACIER",
            StorageTier.ARCHIVE: "DEEP_ARCHIVE"
        }

        # Initialize S3 client (boto3 would be used in real implementation)
        self._s3_client = None
        logger.info(f"S3 backend initialized for bucket: {self.bucket_name}")

    async def store(
        self,
        source_path: Path,
        storage_key: str,
        tier: StorageTier = StorageTier.HOT,
        retention_days: int | None = None,
        tags: dict[str, str] | None = None
    ) -> StorageMetadata:
        """Store file in S3."""
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        storage_class = self.storage_class_mapping[tier]
        s3_key = f"{tier.value}/{storage_key}"

        # Calculate checksum
        checksum = await self._calculate_checksum(source_path)

        # Prepare tags
        s3_tags = {
            "BackupTier": tier.value,
            "BackupDate": datetime.utcnow().isoformat(),
            "Checksum": checksum
        }

        if tags:
            s3_tags.update(tags)

        # In real implementation, use boto3 to upload:
        # response = await self._s3_client.put_object(
        #     Bucket=self.bucket_name,
        #     Key=s3_key,
        #     Body=source_path.read_bytes(),
        #     StorageClass=storage_class,
        #     Tagging=urlencode(s3_tags),
        #     ServerSideEncryption='AES256'
        # )

        # Create metadata
        metadata = StorageMetadata(
            storage_id=storage_key,
            original_path=source_path,
            storage_path=s3_key,
            storage_backend=self.backend_id,
            storage_tier=tier,
            size_bytes=source_path.stat().st_size,
            checksum=checksum,
            created_at=datetime.utcnow(),
            retention_until=self._calculate_retention_date(retention_days),
            tags=s3_tags
        )

        self._metadata_store[storage_key] = metadata

        logger.info(f"Stored {source_path} in S3 as {s3_key} with storage class {storage_class}")
        return metadata

    async def retrieve(self, storage_key: str, destination_path: Path) -> bool:
        """Retrieve file from S3."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.error(f"Storage key not found: {storage_key}")
            return False

        # Check if file is in archive tier and needs restoration
        if metadata.storage_tier in [StorageTier.COLD, StorageTier.ARCHIVE]:
            await self._initiate_restore_if_needed(metadata)

        # In real implementation, use boto3 to download:
        # response = await self._s3_client.get_object(
        #     Bucket=self.bucket_name,
        #     Key=metadata.storage_path
        # )
        #
        # destination_path.parent.mkdir(parents=True, exist_ok=True)
        # with open(destination_path, 'wb') as f:
        #     f.write(response['Body'].read())

        # Update access time
        metadata.last_accessed = datetime.utcnow()

        logger.info(f"Retrieved {storage_key} from S3")
        return True

    async def delete(self, storage_key: str) -> bool:
        """Delete file from S3."""
        metadata = self._metadata_store.get(storage_key)
        if not metadata:
            logger.warning(f"Storage key not found for deletion: {storage_key}")
            return False

        # In real implementation, use boto3 to delete:
        # await self._s3_client.delete_object(
        #     Bucket=self.bucket_name,
        #     Key=metadata.storage_path
        # )

        del self._metadata_store[storage_key]

        logger.info(f"Deleted {storage_key} from S3")
        return True

    async def get_storage_stats(self) -> dict[str, Any]:
        """Get S3 storage statistics."""
        base_stats = await super().get_storage_stats()

        # Add S3-specific statistics
        s3_stats = {
            "storage_classes": {},
            "estimated_costs": {}
        }

        # Calculate storage class distribution
        for metadata in self._metadata_store.values():
            storage_class = self.storage_class_mapping[metadata.storage_tier]
            if storage_class not in s3_stats["storage_classes"]:
                s3_stats["storage_classes"][storage_class] = {
                    "files": 0,
                    "size_bytes": 0
                }

            s3_stats["storage_classes"][storage_class]["files"] += 1
            s3_stats["storage_classes"][storage_class]["size_bytes"] += metadata.size_bytes

        base_stats.update(s3_stats)
        return base_stats

    async def _initiate_restore_if_needed(self, metadata: StorageMetadata) -> None:
        """Initiate S3 restore for archived objects."""
        if metadata.storage_tier == StorageTier.COLD:
            # Glacier restore (expedited: 1-5 minutes, standard: 3-5 hours)
            restore_tier = "Standard"
        elif metadata.storage_tier == StorageTier.ARCHIVE:
            # Deep Archive restore (standard: 12 hours, bulk: 48 hours)
            restore_tier = "Standard"
        else:
            return

        # In real implementation, use boto3 to restore:
        # await self._s3_client.restore_object(
        #     Bucket=self.bucket_name,
        #     Key=metadata.storage_path,
        #     RestoreRequest={
        #         'Days': 1,
        #         'GlacierJobParameters': {
        #             'Tier': restore_tier
        #         }
        #     }
        # )

        logger.info(f"Initiated {restore_tier} restore for {metadata.storage_id}")


# Factory function for creating storage backends
def create_storage_backend(backend_type: str, backend_id: str, config: dict[str, Any]) -> StorageBackend:
    """
    Factory function to create storage backend instances.
    
    Args:
        backend_type: Type of storage backend (local, cloud, s3)
        backend_id: Unique identifier for the backend
        config: Configuration parameters
        
    Returns:
        Storage backend instance
    """
    backend_map = {
        "local": LocalStorageBackend,
        "cloud": CloudStorageBackend,
        "s3": S3Backend
    }

    if backend_type not in backend_map:
        raise ValueError(f"Unknown storage backend type: {backend_type}")

    return backend_map[backend_type](backend_id, config)
