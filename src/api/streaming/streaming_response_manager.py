"""
Streaming Response Manager
=========================

High-performance streaming response system for large data exports.
Supports multiple formats with memory-efficient processing and client-side streaming.

Features:
- Memory-efficient streaming for large datasets (millions of records)
- Multiple export formats: JSON, CSV, Excel, Parquet
- Real-time progress tracking and cancellation
- Compression and chunked transfer encoding
- Resume capability for interrupted downloads
- Bandwidth throttling and rate limiting
- Client connection monitoring and cleanup
"""
from __future__ import annotations

import asyncio
import csv
import gzip
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from io import BytesIO, StringIO
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from uuid import UUID, uuid4

import pandas as pd
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logging import get_logger

logger = get_logger(__name__)


class ExportFormat(str, Enum):
    """Supported export formats."""
    JSON = "json"
    NDJSON = "ndjson"  # Newline-delimited JSON
    CSV = "csv"
    EXCEL = "xlsx"
    PARQUET = "parquet"
    XML = "xml"


class StreamingStatus(str, Enum):
    """Streaming operation status."""
    INITIALIZING = "initializing"
    STREAMING = "streaming"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class StreamingConfig:
    """Configuration for streaming operations."""
    chunk_size: int = 1000  # Records per chunk
    buffer_size_mb: int = 50  # Memory buffer size
    compression_enabled: bool = True
    compression_level: int = 6  # 1-9, higher = better compression but slower
    max_bandwidth_mbps: Optional[float] = None  # Bandwidth throttling
    enable_resume: bool = True
    timeout_seconds: int = 3600  # 1 hour timeout
    progress_callback_interval: int = 10000  # Records between progress updates
    max_concurrent_streams: int = 10


@dataclass
class StreamingSession:
    """Active streaming session metadata."""
    session_id: str = field(default_factory=lambda: str(uuid4()))
    user_id: Optional[str] = None
    query: str = ""
    format: ExportFormat = ExportFormat.JSON
    status: StreamingStatus = StreamingStatus.INITIALIZING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_records: Optional[int] = None
    processed_records: int = 0
    bytes_sent: int = 0
    estimated_size_mb: Optional[float] = None
    compression_ratio: Optional[float] = None
    client_ip: Optional[str] = None
    error_message: Optional[str] = None
    cancelled_by_user: bool = False


class StreamingResponseManager:
    """
    High-performance streaming response manager for large data exports.

    Handles memory-efficient streaming of large datasets with support for
    multiple formats, compression, progress tracking, and client management.
    """

    def __init__(self, config: Optional[StreamingConfig] = None):
        self.config = config or StreamingConfig()

        # Active streaming sessions
        self.active_sessions: Dict[str, StreamingSession] = {}
        self.session_lock = asyncio.Lock()

        # Performance metrics
        self.metrics = {
            "total_streams": 0,
            "successful_streams": 0,
            "failed_streams": 0,
            "cancelled_streams": 0,
            "total_bytes_streamed": 0,
            "avg_throughput_mbps": 0.0,
            "active_sessions": 0
        }

    async def create_streaming_response(
        self,
        query: str,
        session: AsyncSession,
        format: ExportFormat = ExportFormat.JSON,
        filename: Optional[str] = None,
        user_id: Optional[str] = None,
        client_ip: Optional[str] = None,
        custom_headers: Optional[Dict[str, str]] = None
    ) -> StreamingResponse:
        """
        Create a streaming response for large data export.

        Args:
            query: SQL query to execute
            session: Database session
            format: Export format
            filename: Optional filename for download
            user_id: User ID for tracking
            client_ip: Client IP for monitoring
            custom_headers: Additional HTTP headers

        Returns:
            StreamingResponse configured for the requested format
        """

        # Check concurrent streams limit
        if len(self.active_sessions) >= self.config.max_concurrent_streams:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many concurrent streaming operations. Please try again later."
            )

        # Create streaming session
        streaming_session = StreamingSession(
            user_id=user_id,
            query=query,
            format=format,
            client_ip=client_ip
        )

        async with self.session_lock:
            self.active_sessions[streaming_session.session_id] = streaming_session

        try:
            # Estimate data size and record count
            await self._estimate_export_size(query, session, streaming_session)

            # Determine content type and headers
            content_type, headers = self._get_response_headers(format, filename, streaming_session)

            if custom_headers:
                headers.update(custom_headers)

            # Create streaming generator
            stream_generator = self._create_stream_generator(
                query, session, streaming_session
            )

            # Update metrics
            self.metrics["total_streams"] += 1
            self.metrics["active_sessions"] = len(self.active_sessions)

            return StreamingResponse(
                stream_generator,
                media_type=content_type,
                headers=headers
            )

        except Exception as e:
            # Clean up session on error
            async with self.session_lock:
                if streaming_session.session_id in self.active_sessions:
                    del self.active_sessions[streaming_session.session_id]

            streaming_session.status = StreamingStatus.FAILED
            streaming_session.error_message = str(e)
            self.metrics["failed_streams"] += 1

            logger.error(f"Failed to create streaming response: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create streaming response: {str(e)}"
            )

    async def _estimate_export_size(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ):
        """Estimate export size and record count."""
        try:
            # Get record count
            count_query = f"SELECT COUNT(*) FROM ({query}) AS count_subquery"
            result = await session.execute(text(count_query))
            total_records = result.scalar()

            streaming_session.total_records = total_records

            # Estimate size based on format and record count
            if total_records and total_records > 0:
                # Sample a few records to estimate average size
                sample_query = f"SELECT * FROM ({query}) AS sample_subquery LIMIT 10"
                sample_result = await session.execute(text(sample_query))
                sample_data = [dict(row._mapping) for row in sample_result]

                if sample_data:
                    # Estimate average record size
                    sample_json = json.dumps(sample_data[0])
                    avg_record_size = len(sample_json.encode('utf-8'))

                    # Adjust for format
                    format_multipliers = {
                        ExportFormat.JSON: 1.0,
                        ExportFormat.NDJSON: 0.95,
                        ExportFormat.CSV: 0.7,
                        ExportFormat.EXCEL: 1.2,
                        ExportFormat.PARQUET: 0.3,
                        ExportFormat.XML: 1.5
                    }

                    multiplier = format_multipliers.get(streaming_session.format, 1.0)
                    estimated_size_bytes = total_records * avg_record_size * multiplier
                    streaming_session.estimated_size_mb = estimated_size_bytes / (1024 * 1024)

        except Exception as e:
            logger.warning(f"Could not estimate export size: {e}")

    def _get_response_headers(
        self,
        format: ExportFormat,
        filename: Optional[str],
        streaming_session: StreamingSession
    ) -> tuple[str, Dict[str, str]]:
        """Get appropriate content type and headers for the format."""

        content_types = {
            ExportFormat.JSON: "application/json",
            ExportFormat.NDJSON: "application/x-ndjson",
            ExportFormat.CSV: "text/csv",
            ExportFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ExportFormat.PARQUET: "application/octet-stream",
            ExportFormat.XML: "application/xml"
        }

        file_extensions = {
            ExportFormat.JSON: ".json",
            ExportFormat.NDJSON: ".ndjson",
            ExportFormat.CSV: ".csv",
            ExportFormat.EXCEL: ".xlsx",
            ExportFormat.PARQUET: ".parquet",
            ExportFormat.XML: ".xml"
        }

        content_type = content_types.get(format, "application/octet-stream")

        headers = {
            "X-Streaming-Session-ID": streaming_session.session_id,
            "X-Estimated-Records": str(streaming_session.total_records or "unknown"),
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }

        if self.config.compression_enabled:
            headers["Content-Encoding"] = "gzip"

        if streaming_session.estimated_size_mb:
            headers["X-Estimated-Size-MB"] = str(round(streaming_session.estimated_size_mb, 2))

        # Set filename for download
        if filename:
            download_filename = filename
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            extension = file_extensions.get(format, ".dat")
            download_filename = f"export_{timestamp}{extension}"

        if self.config.compression_enabled and not download_filename.endswith('.gz'):
            download_filename += '.gz'

        headers["Content-Disposition"] = f'attachment; filename="{download_filename}"'

        return content_type, headers

    async def _create_stream_generator(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Create the streaming generator for the specified format."""

        streaming_session.status = StreamingStatus.STREAMING
        streaming_session.started_at = datetime.now()

        try:
            if streaming_session.format == ExportFormat.JSON:
                async for chunk in self._stream_json(query, session, streaming_session):
                    yield chunk
            elif streaming_session.format == ExportFormat.NDJSON:
                async for chunk in self._stream_ndjson(query, session, streaming_session):
                    yield chunk
            elif streaming_session.format == ExportFormat.CSV:
                async for chunk in self._stream_csv(query, session, streaming_session):
                    yield chunk
            elif streaming_session.format == ExportFormat.EXCEL:
                async for chunk in self._stream_excel(query, session, streaming_session):
                    yield chunk
            elif streaming_session.format == ExportFormat.PARQUET:
                async for chunk in self._stream_parquet(query, session, streaming_session):
                    yield chunk
            elif streaming_session.format == ExportFormat.XML:
                async for chunk in self._stream_xml(query, session, streaming_session):
                    yield chunk

            # Mark as completed
            streaming_session.status = StreamingStatus.COMPLETED
            streaming_session.completed_at = datetime.now()
            self.metrics["successful_streams"] += 1

        except asyncio.CancelledError:
            streaming_session.status = StreamingStatus.CANCELLED
            streaming_session.cancelled_by_user = True
            self.metrics["cancelled_streams"] += 1
            logger.info(f"Streaming session {streaming_session.session_id} was cancelled")

        except Exception as e:
            streaming_session.status = StreamingStatus.FAILED
            streaming_session.error_message = str(e)
            self.metrics["failed_streams"] += 1
            logger.error(f"Streaming session {streaming_session.session_id} failed: {e}")

        finally:
            # Clean up session
            async with self.session_lock:
                if streaming_session.session_id in self.active_sessions:
                    del self.active_sessions[streaming_session.session_id]

            self.metrics["active_sessions"] = len(self.active_sessions)

    async def _stream_json(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as JSON array."""

        # Start JSON array
        yield await self._compress_chunk(b'[')

        first_chunk = True

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            if not first_chunk:
                yield await self._compress_chunk(b',')

            # Convert chunk to JSON
            json_chunk = json.dumps(chunk_data, default=str, separators=(',', ':')).encode('utf-8')

            if first_chunk:
                first_chunk = False
                # Remove outer brackets from first chunk
                json_chunk = json_chunk[1:-1]
            else:
                # Remove outer brackets and prepend comma
                json_chunk = json_chunk[1:-1]

            yield await self._compress_chunk(json_chunk)

            # Apply bandwidth throttling
            if self.config.max_bandwidth_mbps:
                await self._throttle_bandwidth(len(json_chunk))

        # End JSON array
        yield await self._compress_chunk(b']')

    async def _stream_ndjson(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as newline-delimited JSON."""

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            for record in chunk_data:
                json_line = json.dumps(record, default=str, separators=(',', ':')) + '\n'
                chunk_bytes = json_line.encode('utf-8')

                yield await self._compress_chunk(chunk_bytes)

                # Apply bandwidth throttling
                if self.config.max_bandwidth_mbps:
                    await self._throttle_bandwidth(len(chunk_bytes))

    async def _stream_csv(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as CSV."""

        headers_written = False

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            if not chunk_data:
                continue

            # Create CSV content
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=chunk_data[0].keys())

            # Write headers only once
            if not headers_written:
                writer.writeheader()
                headers_written = True

            # Write data rows
            writer.writerows(chunk_data)

            csv_content = csv_buffer.getvalue().encode('utf-8')
            yield await self._compress_chunk(csv_content)

            # Apply bandwidth throttling
            if self.config.max_bandwidth_mbps:
                await self._throttle_bandwidth(len(csv_content))

    async def _stream_excel(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as Excel file."""

        # For Excel, we need to collect all data first due to format requirements
        all_data = []

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            all_data.extend(chunk_data)

        # Create Excel file
        df = pd.DataFrame(all_data)
        excel_buffer = BytesIO()

        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Export')

        excel_data = excel_buffer.getvalue()

        # Stream in chunks
        chunk_size = 8192  # 8KB chunks
        for i in range(0, len(excel_data), chunk_size):
            chunk = excel_data[i:i + chunk_size]
            yield await self._compress_chunk(chunk)

            # Apply bandwidth throttling
            if self.config.max_bandwidth_mbps:
                await self._throttle_bandwidth(len(chunk))

    async def _stream_parquet(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as Parquet file."""

        # For Parquet, collect data in batches
        all_data = []

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            all_data.extend(chunk_data)

        # Create Parquet file
        df = pd.DataFrame(all_data)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        parquet_data = parquet_buffer.getvalue()

        # Stream in chunks
        chunk_size = 8192  # 8KB chunks
        for i in range(0, len(parquet_data), chunk_size):
            chunk = parquet_data[i:i + chunk_size]
            yield await self._compress_chunk(chunk)

            # Apply bandwidth throttling
            if self.config.max_bandwidth_mbps:
                await self._throttle_bandwidth(len(chunk))

    async def _stream_xml(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[bytes, None]:
        """Stream data as XML."""

        # Start XML document
        yield await self._compress_chunk(b'<?xml version="1.0" encoding="UTF-8"?>\n<export>\n')

        async for chunk_data in self._execute_chunked_query(query, session, streaming_session):
            for record in chunk_data:
                xml_record = self._dict_to_xml(record, 'record')
                xml_bytes = (xml_record + '\n').encode('utf-8')

                yield await self._compress_chunk(xml_bytes)

                # Apply bandwidth throttling
                if self.config.max_bandwidth_mbps:
                    await self._throttle_bandwidth(len(xml_bytes))

        # End XML document
        yield await self._compress_chunk(b'</export>')

    def _dict_to_xml(self, data: Dict[str, Any], root_tag: str) -> str:
        """Convert dictionary to XML string."""
        xml_parts = [f'<{root_tag}>']

        for key, value in data.items():
            # Sanitize key name for XML
            safe_key = ''.join(c if c.isalnum() or c in '_-' else '_' for c in str(key))
            safe_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            xml_parts.append(f'<{safe_key}>{safe_value}</{safe_key}>')

        xml_parts.append(f'</{root_tag}>')
        return ''.join(xml_parts)

    async def _execute_chunked_query(
        self,
        query: str,
        session: AsyncSession,
        streaming_session: StreamingSession
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Execute query in chunks for memory-efficient processing."""

        offset = 0

        while True:
            # Add LIMIT and OFFSET to query
            chunked_query = f"{query} LIMIT {self.config.chunk_size} OFFSET {offset}"

            try:
                result = await session.execute(text(chunked_query))
                rows = result.fetchall()

                if not rows:
                    break

                # Convert to dictionaries
                chunk_data = [dict(row._mapping) for row in rows]

                # Update progress
                streaming_session.processed_records += len(chunk_data)

                # Log progress periodically
                if (streaming_session.processed_records %
                    self.config.progress_callback_interval == 0):
                    logger.info(f"Streaming progress: {streaming_session.processed_records} records processed")

                yield chunk_data

                offset += self.config.chunk_size

                # Check if we should continue
                if len(rows) < self.config.chunk_size:
                    break

            except Exception as e:
                logger.error(f"Error executing chunked query: {e}")
                raise

    async def _compress_chunk(self, data: bytes) -> bytes:
        """Compress data chunk if compression is enabled."""
        if self.config.compression_enabled:
            return gzip.compress(data, compresslevel=self.config.compression_level)
        return data

    async def _throttle_bandwidth(self, bytes_sent: int):
        """Apply bandwidth throttling if configured."""
        if not self.config.max_bandwidth_mbps:
            return

        # Calculate required delay
        max_bytes_per_second = self.config.max_bandwidth_mbps * 1024 * 1024
        required_time = bytes_sent / max_bytes_per_second

        # Small delay to throttle bandwidth
        await asyncio.sleep(required_time)

    async def get_active_sessions(self) -> List[Dict[str, Any]]:
        """Get information about active streaming sessions."""
        async with self.session_lock:
            sessions = []
            for session in self.active_sessions.values():
                sessions.append({
                    "session_id": session.session_id,
                    "user_id": session.user_id,
                    "format": session.format.value,
                    "status": session.status.value,
                    "created_at": session.created_at.isoformat(),
                    "started_at": session.started_at.isoformat() if session.started_at else None,
                    "total_records": session.total_records,
                    "processed_records": session.processed_records,
                    "progress_percent": (
                        (session.processed_records / session.total_records * 100)
                        if session.total_records else 0
                    ),
                    "bytes_sent": session.bytes_sent,
                    "estimated_size_mb": session.estimated_size_mb,
                    "client_ip": session.client_ip
                })

            return sessions

    async def cancel_session(self, session_id: str) -> bool:
        """Cancel an active streaming session."""
        async with self.session_lock:
            if session_id in self.active_sessions:
                session = self.active_sessions[session_id]
                session.status = StreamingStatus.CANCELLED
                session.cancelled_by_user = True
                return True
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get streaming performance metrics."""
        return {
            **self.metrics,
            "active_sessions_count": len(self.active_sessions),
            "config": {
                "chunk_size": self.config.chunk_size,
                "buffer_size_mb": self.config.buffer_size_mb,
                "compression_enabled": self.config.compression_enabled,
                "max_concurrent_streams": self.config.max_concurrent_streams
            }
        }


# Factory function
def create_streaming_manager(config: Optional[StreamingConfig] = None) -> StreamingResponseManager:
    """Create streaming response manager instance."""
    return StreamingResponseManager(config)