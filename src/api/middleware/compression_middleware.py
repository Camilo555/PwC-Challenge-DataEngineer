"""
High-Performance Response Compression Middleware

Advanced compression middleware supporting multiple algorithms (GZip, Brotli, Zstandard)
with adaptive compression based on content type and size.
"""

import asyncio
import gzip
import time
import zlib
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from core.logging import get_logger

logger = get_logger(__name__)


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    GZIP = "gzip"
    BROTLI = "br"
    ZSTD = "zstd"
    DEFLATE = "deflate"


class CompressionQuality(Enum):
    """Compression quality levels."""
    FASTEST = 1
    FAST = 3
    BALANCED = 6
    HIGH = 9
    MAXIMUM = 11


class CompressionConfig:
    """Configuration for compression middleware."""
    
    def __init__(self,
                 min_size: int = 500,  # Minimum response size to compress
                 max_size: int = 10 * 1024 * 1024,  # 10MB max size
                 gzip_level: int = 6,
                 brotli_quality: int = 4,
                 zstd_level: int = 3,
                 compressible_types: Optional[Set[str]] = None,
                 excluded_paths: Optional[Set[str]] = None,
                 adaptive_compression: bool = True,
                 enable_streaming_compression: bool = True):
        
        self.min_size = min_size
        self.max_size = max_size
        self.gzip_level = gzip_level
        self.brotli_quality = brotli_quality
        self.zstd_level = zstd_level
        self.adaptive_compression = adaptive_compression
        self.enable_streaming_compression = enable_streaming_compression
        
        # Default compressible content types
        if compressible_types is None:
            self.compressible_types = {
                "application/json",
                "application/javascript", 
                "application/xml",
                "text/html",
                "text/css",
                "text/javascript",
                "text/plain",
                "text/xml",
                "text/csv",
                "application/csv",
                "image/svg+xml",
                "application/rss+xml",
                "application/atom+xml"
            }
        else:
            self.compressible_types = compressible_types
        
        # Paths to exclude from compression
        self.excluded_paths = excluded_paths or {
            "/health",
            "/metrics", 
            "/favicon.ico",
            "/docs",
            "/redoc",
            "/openapi.json"
        }


class CompressionStats:
    """Statistics tracking for compression."""
    
    def __init__(self):
        self.total_requests = 0
        self.compressed_requests = 0
        self.total_original_size = 0
        self.total_compressed_size = 0
        self.compression_times = []
        self.algorithm_usage = {
            CompressionAlgorithm.GZIP: 0,
            CompressionAlgorithm.BROTLI: 0,
            CompressionAlgorithm.ZSTD: 0,
            CompressionAlgorithm.DEFLATE: 0
        }
    
    def record_compression(self, original_size: int, compressed_size: int, 
                          algorithm: CompressionAlgorithm, compression_time: float):
        """Record compression statistics."""
        self.total_requests += 1
        self.compressed_requests += 1
        self.total_original_size += original_size
        self.total_compressed_size += compressed_size
        self.compression_times.append(compression_time)
        self.algorithm_usage[algorithm] += 1
        
        # Keep only last 1000 compression times
        if len(self.compression_times) > 1000:
            self.compression_times = self.compression_times[-1000:]
    
    def record_skipped(self):
        """Record a request that was not compressed."""
        self.total_requests += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        if self.total_requests == 0:
            return {"compression_ratio": 0, "compression_rate": 0}
        
        compression_rate = self.compressed_requests / self.total_requests
        
        if self.total_original_size > 0:
            compression_ratio = 1 - (self.total_compressed_size / self.total_original_size)
        else:
            compression_ratio = 0
        
        avg_compression_time = (
            sum(self.compression_times) / len(self.compression_times)
            if self.compression_times else 0
        )
        
        return {
            "total_requests": self.total_requests,
            "compressed_requests": self.compressed_requests,
            "compression_rate": compression_rate,
            "compression_ratio": compression_ratio,
            "total_bytes_saved": self.total_original_size - self.total_compressed_size,
            "avg_compression_time_ms": avg_compression_time * 1000,
            "algorithm_usage": {alg.value: count for alg, count in self.algorithm_usage.items()},
            "avg_original_size": (
                self.total_original_size / self.compressed_requests 
                if self.compressed_requests > 0 else 0
            ),
            "avg_compressed_size": (
                self.total_compressed_size / self.compressed_requests 
                if self.compressed_requests > 0 else 0
            )
        }


class AdvancedCompressionMiddleware(BaseHTTPMiddleware):
    """Advanced compression middleware with adaptive algorithms."""
    
    def __init__(self, app, config: Optional[CompressionConfig] = None):
        super().__init__(app)
        self.config = config or CompressionConfig()
        self.stats = CompressionStats()
        
        # Compression functions mapping
        self.compressors = {
            CompressionAlgorithm.GZIP: self._compress_gzip,
            CompressionAlgorithm.DEFLATE: self._compress_deflate,
        }
        
        if BROTLI_AVAILABLE:
            self.compressors[CompressionAlgorithm.BROTLI] = self._compress_brotli
        
        if ZSTD_AVAILABLE:
            self.compressors[CompressionAlgorithm.ZSTD] = self._compress_zstd
    
    async def dispatch(self, request: Request, call_next):
        """Process request with compression."""
        start_time = time.time()
        
        # Skip compression for excluded paths
        if str(request.url.path) in self.config.excluded_paths:
            self.stats.record_skipped()
            return await call_next(request)
        
        # Get client accepted encodings
        accept_encoding = request.headers.get("accept-encoding", "")
        supported_algorithms = self._parse_accept_encoding(accept_encoding)
        
        if not supported_algorithms:
            self.stats.record_skipped()
            return await call_next(request)
        
        # Call the endpoint
        response = await call_next(request)
        
        # Check if response should be compressed
        if not self._should_compress_response(response):
            self.stats.record_skipped()
            return response
        
        # Get response body
        response_body = b""
        if hasattr(response, 'body'):
            response_body = response.body
        elif hasattr(response, 'content'):
            if isinstance(response.content, bytes):
                response_body = response.content
            elif isinstance(response.content, str):
                response_body = response.content.encode('utf-8')
        
        if not response_body or len(response_body) < self.config.min_size:
            self.stats.record_skipped()
            return response
        
        # Select best compression algorithm
        algorithm = self._select_compression_algorithm(
            supported_algorithms, len(response_body)
        )
        
        if not algorithm:
            self.stats.record_skipped()
            return response
        
        # Compress the response
        try:
            compression_start = time.time()
            compressed_body = await self._compress_async(response_body, algorithm)
            compression_time = time.time() - compression_start
            
            # Only use compression if it reduces size significantly
            if len(compressed_body) >= len(response_body) * 0.95:
                logger.debug(f"Compression not beneficial: {len(response_body)} -> {len(compressed_body)}")
                self.stats.record_skipped()
                return response
            
            # Create compressed response
            compressed_response = Response(
                content=compressed_body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.headers.get("content-type")
            )
            
            # Update headers
            compressed_response.headers["content-encoding"] = algorithm.value
            compressed_response.headers["content-length"] = str(len(compressed_body))
            compressed_response.headers["vary"] = "Accept-Encoding"
            
            # Add compression metadata
            compressed_response.headers["x-compression-algorithm"] = algorithm.value
            compressed_response.headers["x-compression-ratio"] = f"{len(compressed_body)/len(response_body):.3f}"
            compressed_response.headers["x-compression-time"] = f"{compression_time*1000:.2f}ms"
            compressed_response.headers["x-original-size"] = str(len(response_body))
            compressed_response.headers["x-compressed-size"] = str(len(compressed_body))
            
            # Record statistics
            self.stats.record_compression(
                len(response_body), len(compressed_body), algorithm, compression_time
            )
            
            total_time = time.time() - start_time
            logger.debug(
                f"Compressed response: {len(response_body)} -> {len(compressed_body)} bytes "
                f"({algorithm.value}, {compression_time*1000:.2f}ms compression, {total_time*1000:.2f}ms total)"
            )
            
            return compressed_response
            
        except Exception as e:
            logger.error(f"Compression failed with {algorithm.value}: {e}")
            self.stats.record_skipped()
            return response
    
    def _parse_accept_encoding(self, accept_encoding: str) -> List[Tuple[CompressionAlgorithm, float]]:
        """Parse Accept-Encoding header and return supported algorithms with quality scores."""
        encodings = []
        
        for encoding in accept_encoding.split(','):
            encoding = encoding.strip()
            if ';' in encoding:
                name, params = encoding.split(';', 1)
                name = name.strip()
                
                # Parse quality value
                quality = 1.0
                for param in params.split(';'):
                    param = param.strip()
                    if param.startswith('q='):
                        try:
                            quality = float(param[2:])
                        except ValueError:
                            quality = 1.0
            else:
                name = encoding.strip()
                quality = 1.0
            
            # Map to our compression algorithms
            algorithm = None
            if name == "gzip":
                algorithm = CompressionAlgorithm.GZIP
            elif name == "br" and BROTLI_AVAILABLE:
                algorithm = CompressionAlgorithm.BROTLI
            elif name == "zstd" and ZSTD_AVAILABLE:
                algorithm = CompressionAlgorithm.ZSTD
            elif name == "deflate":
                algorithm = CompressionAlgorithm.DEFLATE
            
            if algorithm and quality > 0:
                encodings.append((algorithm, quality))
        
        # Sort by quality (highest first)
        return sorted(encodings, key=lambda x: x[1], reverse=True)
    
    def _should_compress_response(self, response: Response) -> bool:
        """Check if response should be compressed."""
        # Check status code
        if response.status_code < 200 or response.status_code >= 300:
            return False
        
        # Check if already compressed
        if response.headers.get("content-encoding"):
            return False
        
        # Check content type
        content_type = response.headers.get("content-type", "").split(';')[0].strip()
        if content_type not in self.config.compressible_types:
            return False
        
        # Check size limits
        content_length = response.headers.get("content-length")
        if content_length:
            try:
                size = int(content_length)
                if size < self.config.min_size or size > self.config.max_size:
                    return False
            except ValueError:
                pass
        
        return True
    
    def _select_compression_algorithm(self, supported_algorithms: List[Tuple[CompressionAlgorithm, float]], 
                                   response_size: int) -> Optional[CompressionAlgorithm]:
        """Select the best compression algorithm based on client support and adaptive logic."""
        if not supported_algorithms:
            return None
        
        if not self.config.adaptive_compression:
            # Just return the highest quality supported algorithm
            return supported_algorithms[0][0]
        
        # Adaptive selection based on response size and performance characteristics
        for algorithm, quality in supported_algorithms:
            if algorithm == CompressionAlgorithm.BROTLI and response_size > 10000:
                # Brotli is great for larger responses
                return algorithm
            elif algorithm == CompressionAlgorithm.ZSTD and response_size > 5000:
                # Zstandard is fast and efficient for medium to large responses
                return algorithm
            elif algorithm == CompressionAlgorithm.GZIP:
                # Gzip is reliable and widely supported
                return algorithm
        
        return supported_algorithms[0][0] if supported_algorithms else None
    
    async def _compress_async(self, data: bytes, algorithm: CompressionAlgorithm) -> bytes:
        """Compress data asynchronously."""
        if len(data) > 100000:  # 100KB threshold for async processing
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self.compressors[algorithm], data
            )
        else:
            return self.compressors[algorithm](data)
    
    def _compress_gzip(self, data: bytes) -> bytes:
        """Compress data using gzip."""
        return gzip.compress(data, compresslevel=self.config.gzip_level)
    
    def _compress_deflate(self, data: bytes) -> bytes:
        """Compress data using deflate."""
        return zlib.compress(data, level=self.config.gzip_level)
    
    def _compress_brotli(self, data: bytes) -> bytes:
        """Compress data using Brotli."""
        if not BROTLI_AVAILABLE:
            raise RuntimeError("Brotli not available")
        return brotli.compress(data, quality=self.config.brotli_quality)
    
    def _compress_zstd(self, data: bytes) -> bytes:
        """Compress data using Zstandard."""
        if not ZSTD_AVAILABLE:
            raise RuntimeError("Zstandard not available")
        compressor = zstd.ZstdCompressor(level=self.config.zstd_level)
        return compressor.compress(data)
    
    def get_compression_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        return self.stats.get_stats()


# Factory function for easy middleware creation
def create_compression_middleware(
    min_size: int = 500,
    gzip_level: int = 6,
    brotli_quality: int = 4,
    excluded_paths: Optional[Set[str]] = None,
    adaptive_compression: bool = True
) -> AdvancedCompressionMiddleware:
    """Create compression middleware with custom configuration."""
    
    config = CompressionConfig(
        min_size=min_size,
        gzip_level=gzip_level,
        brotli_quality=brotli_quality,
        excluded_paths=excluded_paths,
        adaptive_compression=adaptive_compression
    )
    
    return AdvancedCompressionMiddleware(None, config)


# Global middleware instance
_compression_middleware: Optional[AdvancedCompressionMiddleware] = None


def get_compression_middleware() -> AdvancedCompressionMiddleware:
    """Get or create global compression middleware instance."""
    global _compression_middleware
    if _compression_middleware is None:
        _compression_middleware = create_compression_middleware()
    return _compression_middleware