"""
Text embeddings module for vector search functionality.
Provides text embedding generation for semantic search.
"""
import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class TextEmbedder:
    """Text embedding generator for semantic search."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2", device: str = "cpu"):
        """Initialize TextEmbedder with specified model."""
        self.model_name = model_name
        self.device = device
        self._model = None
        self.embedding_dimension = 384  # Default for MiniLM

    def _load_model(self):
        """Lazy load the embedding model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name, device=self.device)
                self.embedding_dimension = self._model.get_sentence_embedding_dimension()
                logger.info(f"Loaded embedding model: {self.model_name}")
            except ImportError:
                logger.warning("sentence-transformers not available, using mock embeddings")
                self._model = "mock"
            except Exception as e:
                logger.error(f"Failed to load embedding model: {e}")
                self._model = "mock"

    def encode(self, texts: list[str]) -> np.ndarray:
        """Generate embeddings for a list of texts."""
        self._load_model()

        if self._model == "mock":
            return self._generate_mock_embeddings(texts)

        try:
            embeddings = self._model.encode(texts)
            return embeddings
        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}")
            return self._generate_mock_embeddings(texts)

    def encode_single(self, text: str) -> np.ndarray:
        """Generate embedding for a single text."""
        return self.encode([text])[0]

    def _generate_mock_embeddings(self, texts: list[str]) -> np.ndarray:
        """Generate mock embeddings for testing."""
        np.random.seed(hash("".join(texts)) % 2**32)
        embeddings = np.random.randn(len(texts), self.embedding_dimension)
        # Normalize embeddings
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings

    def similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """Calculate cosine similarity between two embeddings."""
        return np.dot(embedding1, embedding2) / (np.linalg.norm(embedding1) * np.linalg.norm(embedding2))

    def batch_similarity(self, query_embedding: np.ndarray, document_embeddings: np.ndarray) -> np.ndarray:
        """Calculate similarities between a query and multiple documents."""
        query_norm = np.linalg.norm(query_embedding)
        doc_norms = np.linalg.norm(document_embeddings, axis=1)

        similarities = np.dot(document_embeddings, query_embedding) / (doc_norms * query_norm)
        return similarities

    def get_embedding_dimension(self) -> int:
        """Get the dimension of embeddings produced by this model."""
        return self.embedding_dimension

    def encode_for_search(self, texts: list[str], normalize: bool = True) -> list[list[float]]:
        """Encode texts for search indexing, returning as list of lists."""
        embeddings = self.encode(texts)

        if normalize:
            embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

        return embeddings.tolist()

    def preprocess_text(self, text: str) -> str:
        """Preprocess text before embedding."""
        # Basic text preprocessing
        text = text.lower().strip()
        # Remove extra whitespace
        text = " ".join(text.split())
        return text

    def encode_with_preprocessing(self, texts: list[str]) -> np.ndarray:
        """Encode texts with preprocessing."""
        preprocessed_texts = [self.preprocess_text(text) for text in texts]
        return self.encode(preprocessed_texts)


class MultilingualTextEmbedder(TextEmbedder):
    """Text embedder with multilingual support."""

    def __init__(self, model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", device: str = "cpu"):
        """Initialize with multilingual model."""
        super().__init__(model_name, device)
        self.embedding_dimension = 384  # Default for multilingual MiniLM

    def detect_language(self, text: str) -> str:
        """Simple language detection (mock implementation)."""
        # This is a simplified mock - in practice you'd use langdetect or similar
        common_english_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for']
        if any(word in text.lower() for word in common_english_words):
            return "en"
        return "unknown"

    def encode_multilingual(self, texts: list[str], languages: list[str] | None = None) -> np.ndarray:
        """Encode texts with language-aware preprocessing."""
        if languages is None:
            languages = [self.detect_language(text) for text in texts]

        # Language-specific preprocessing could be added here
        return self.encode(texts)


class DocumentEmbedder:
    """Document-level embedding generator with chunking support."""

    def __init__(self, text_embedder: TextEmbedder, chunk_size: int = 512, overlap: int = 50):
        """Initialize with text embedder and chunking parameters."""
        self.text_embedder = text_embedder
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk_text(self, text: str) -> list[str]:
        """Split text into overlapping chunks."""
        words = text.split()
        if len(words) <= self.chunk_size:
            return [text]

        chunks = []
        start = 0

        while start < len(words):
            end = min(start + self.chunk_size, len(words))
            chunk = " ".join(words[start:end])
            chunks.append(chunk)

            if end >= len(words):
                break

            start = end - self.overlap

        return chunks

    def encode_document(self, document: str) -> dict[str, Any]:
        """Encode a document with chunking."""
        chunks = self.chunk_text(document)
        chunk_embeddings = self.text_embedder.encode(chunks)

        # Create document-level embedding by averaging chunk embeddings
        document_embedding = np.mean(chunk_embeddings, axis=0)

        return {
            "document_embedding": document_embedding,
            "chunk_embeddings": chunk_embeddings,
            "chunks": chunks,
            "num_chunks": len(chunks)
        }

    def encode_documents(self, documents: list[str]) -> list[dict[str, Any]]:
        """Encode multiple documents."""
        return [self.encode_document(doc) for doc in documents]


def create_text_embedder(model_type: str = "default", **kwargs) -> TextEmbedder:
    """Factory function to create text embedder instances."""
    if model_type == "multilingual":
        return MultilingualTextEmbedder(**kwargs)
    elif model_type == "default":
        return TextEmbedder(**kwargs)
    else:
        raise ValueError(f"Unknown model type: {model_type}")


def calculate_text_similarity(text1: str, text2: str, embedder: TextEmbedder | None = None) -> float:
    """Calculate semantic similarity between two texts."""
    if embedder is None:
        embedder = TextEmbedder()

    embeddings = embedder.encode([text1, text2])
    return embedder.similarity(embeddings[0], embeddings[1])
