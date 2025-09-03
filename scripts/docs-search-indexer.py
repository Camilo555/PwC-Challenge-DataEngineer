#!/usr/bin/env python3
"""
Documentation search indexer.
Creates a searchable index of all documentation files.
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class DocumentationIndexer:
    """Documentation search indexer using SQLite FTS."""

    def __init__(self, db_path: str = "docs/static/search_index.db"):
        """Initialize the indexer with database path."""
        self.db_path = db_path
        self.ensure_db_directory()
        self.init_database()

    def ensure_db_directory(self):
        """Ensure the database directory exists."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def init_database(self):
        """Initialize SQLite database with FTS tables."""
        with sqlite3.connect(self.db_path) as conn:
            # Create FTS table for full-text search
            conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
                    title,
                    content,
                    file_path,
                    category,
                    tags,
                    last_modified
                )
            ''')

            # Create metadata table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS docs_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE,
                    title TEXT,
                    category TEXT,
                    word_count INTEGER,
                    last_modified TEXT,
                    file_hash TEXT,
                    tags TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create index stats table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS index_stats (
                    id INTEGER PRIMARY KEY,
                    total_docs INTEGER,
                    total_words INTEGER,
                    last_indexed TEXT,
                    index_version TEXT
                )
            ''')

            conn.commit()

    def get_file_hash(self, file_path: str) -> str:
        """Get MD5 hash of file content."""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except OSError:
            return ""

    def extract_metadata_from_content(self, content: str) -> dict[str, Any]:
        """Extract metadata from markdown content."""
        metadata = {
            'title': 'Untitled',
            'category': 'general',
            'tags': [],
            'description': ''
        }

        lines = content.split('\n')

        # Extract title from first H1 heading
        for line in lines[:20]:  # Look in first 20 lines
            if line.strip().startswith('# '):
                metadata['title'] = line.strip()[2:].strip()
                break

        # Extract category from file path or content
        # Look for category indicators
        if any(keyword in content.lower() for keyword in ['api', 'endpoint', 'swagger']):
            metadata['category'] = 'api'
        elif any(keyword in content.lower() for keyword in ['etl', 'pipeline', 'data']):
            metadata['category'] = 'data-engineering'
        elif any(keyword in content.lower() for keyword in ['deploy', 'kubernetes', 'docker']):
            metadata['category'] = 'operations'
        elif any(keyword in content.lower() for keyword in ['security', 'auth', 'rbac']):
            metadata['category'] = 'security'
        elif any(keyword in content.lower() for keyword in ['architecture', 'system', 'design']):
            metadata['category'] = 'architecture'

        # Extract tags from content (simple keyword extraction)
        tag_keywords = [
            'fastapi', 'graphql', 'postgresql', 'redis', 'kafka', 'airflow',
            'dbt', 'terraform', 'kubernetes', 'docker', 'monitoring',
            'datadog', 'security', 'authentication', 'etl', 'pipeline'
        ]

        content_lower = content.lower()
        found_tags = [tag for tag in tag_keywords if tag in content_lower]
        metadata['tags'] = list(set(found_tags))

        # Extract description from first paragraph
        paragraphs = [line.strip() for line in lines if line.strip() and not line.strip().startswith('#')]
        if paragraphs:
            metadata['description'] = paragraphs[0][:200] + ('...' if len(paragraphs[0]) > 200 else '')

        return metadata

    def clean_content_for_search(self, content: str) -> str:
        """Clean markdown content for better search results."""
        # Remove code blocks
        content = re.sub(r'```[\s\S]*?```', '', content)

        # Remove inline code
        content = re.sub(r'`[^`]+`', '', content)

        # Remove markdown links but keep text
        content = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', content)

        # Remove markdown formatting
        content = re.sub(r'[#*_`\[\]()]', '', content)

        # Remove extra whitespace
        content = re.sub(r'\s+', ' ', content).strip()

        return content

    def index_file(self, file_path: str) -> bool:
        """Index a single markdown file."""
        try:
            with open(file_path, encoding='utf-8') as f:
                content = f.read()
        except (OSError, UnicodeDecodeError) as e:
            print(f"Error reading {file_path}: {e}")
            return False

        # Get file info
        file_stat = os.stat(file_path)
        last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        file_hash = self.get_file_hash(file_path)

        # Check if file needs updating
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT file_hash FROM docs_metadata WHERE file_path = ?',
                (file_path,)
            )
            result = cursor.fetchone()

            if result and result[0] == file_hash:
                # File hasn't changed, skip indexing
                return True

        # Extract metadata
        metadata = self.extract_metadata_from_content(content)
        clean_content = self.clean_content_for_search(content)
        word_count = len(clean_content.split())

        # Determine category from file path if not already set
        if metadata['category'] == 'general':
            path_parts = Path(file_path).parts
            if 'api' in path_parts:
                metadata['category'] = 'api'
            elif 'data-engineering' in path_parts:
                metadata['category'] = 'data-engineering'
            elif 'operations' in path_parts:
                metadata['category'] = 'operations'
            elif 'security' in path_parts:
                metadata['category'] = 'security'
            elif 'architecture' in path_parts:
                metadata['category'] = 'architecture'

        # Insert/update in database
        with sqlite3.connect(self.db_path) as conn:
            # Insert into FTS table
            conn.execute('''
                INSERT OR REPLACE INTO docs_fts
                (title, content, file_path, category, tags, last_modified)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                metadata['title'],
                clean_content,
                file_path,
                metadata['category'],
                ', '.join(metadata['tags']),
                last_modified
            ))

            # Insert into metadata table
            conn.execute('''
                INSERT OR REPLACE INTO docs_metadata
                (file_path, title, category, word_count, last_modified, file_hash, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_path,
                metadata['title'],
                metadata['category'],
                word_count,
                last_modified,
                file_hash,
                ', '.join(metadata['tags'])
            ))

            conn.commit()

        return True

    def index_all_docs(self, docs_path: str = "docs") -> dict[str, Any]:
        """Index all markdown files in the documentation directory."""
        start_time = datetime.now()

        # Find all markdown files
        md_files = []
        for root, dirs, files in os.walk(docs_path):
            # Skip hidden directories and common excludes
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['node_modules', 'venv']]

            for file in files:
                if file.endswith('.md'):
                    md_files.append(os.path.join(root, file))

        # Also include root-level markdown files
        root_md_files = [f for f in os.listdir('.') if f.endswith('.md')]
        md_files.extend(root_md_files)

        # Index each file
        indexed_count = 0
        skipped_count = 0
        error_count = 0

        for md_file in md_files:
            if self.index_file(md_file):
                indexed_count += 1
                print(f"✅ Indexed: {md_file}")
            else:
                error_count += 1
                print(f"❌ Error indexing: {md_file}")

        # Update index statistics
        with sqlite3.connect(self.db_path) as conn:
            total_words = conn.execute('SELECT SUM(word_count) FROM docs_metadata').fetchone()[0] or 0
            total_docs = conn.execute('SELECT COUNT(*) FROM docs_metadata').fetchone()[0] or 0

            conn.execute('''
                INSERT OR REPLACE INTO index_stats (id, total_docs, total_words, last_indexed, index_version)
                VALUES (1, ?, ?, ?, ?)
            ''', (total_docs, total_words, datetime.now().isoformat(), "1.0.0"))

            conn.commit()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        stats = {
            'total_files_found': len(md_files),
            'indexed_successfully': indexed_count,
            'skipped': skipped_count,
            'errors': error_count,
            'duration_seconds': duration,
            'files_per_second': len(md_files) / duration if duration > 0 else 0
        }

        return stats

    def search(self, query: str, limit: int = 10, category: str = None) -> list[dict[str, Any]]:
        """Search the documentation index."""
        with sqlite3.connect(self.db_path) as conn:
            # Build search query
            sql_query = '''
                SELECT
                    title,
                    file_path,
                    category,
                    tags,
                    last_modified,
                    snippet(docs_fts, 1, '<mark>', '</mark>', '...', 20) as snippet,
                    rank
                FROM docs_fts
                WHERE docs_fts MATCH ?
            '''

            params = [query]

            if category:
                sql_query += ' AND category = ?'
                params.append(category)

            sql_query += ' ORDER BY rank LIMIT ?'
            params.append(limit)

            cursor = conn.execute(sql_query, params)
            results = []

            for row in cursor.fetchall():
                results.append({
                    'title': row[0],
                    'file_path': row[1],
                    'category': row[2],
                    'tags': row[3].split(', ') if row[3] else [],
                    'last_modified': row[4],
                    'snippet': row[5],
                    'relevance_score': row[6]
                })

            return results

    def get_stats(self) -> dict[str, Any]:
        """Get indexing statistics."""
        with sqlite3.connect(self.db_path) as conn:
            stats_row = conn.execute('SELECT * FROM index_stats WHERE id = 1').fetchone()

            if not stats_row:
                return {'status': 'No index found'}

            categories = conn.execute('''
                SELECT category, COUNT(*) as count
                FROM docs_metadata
                GROUP BY category
                ORDER BY count DESC
            ''').fetchall()

            top_tags = conn.execute('''
                SELECT tags, COUNT(*) as frequency
                FROM docs_metadata
                WHERE tags != ''
                GROUP BY tags
                ORDER BY frequency DESC
                LIMIT 10
            ''').fetchall()

            return {
                'total_documents': stats_row[1],
                'total_words': stats_row[2],
                'last_indexed': stats_row[3],
                'index_version': stats_row[4],
                'categories': [{'name': cat[0], 'count': cat[1]} for cat in categories],
                'top_tags': [{'tag': tag[0], 'frequency': tag[1]} for tag in top_tags]
            }

    def export_search_api(self, output_path: str = "docs/static/search-api.js"):
        """Export search functionality as JavaScript API."""
        js_code = '''
/**
 * Documentation Search API
 * Auto-generated search functionality for documentation
 */

class DocumentationSearch {
    constructor(indexUrl = '/docs/static/search_index.json') {
        this.indexUrl = indexUrl;
        this.searchIndex = null;
        this.loadIndex();
    }

    async loadIndex() {
        try {
            const response = await fetch(this.indexUrl);
            this.searchIndex = await response.json();
            console.log('Documentation search index loaded');
        } catch (error) {
            console.error('Failed to load search index:', error);
        }
    }

    search(query, options = {}) {
        if (!this.searchIndex) {
            return [];
        }

        const {
            limit = 10,
            category = null,
            fuzzy = true
        } = options;

        const queryLower = query.toLowerCase();
        const results = [];

        for (const doc of this.searchIndex.documents) {
            let score = 0;

            // Title match (highest weight)
            if (doc.title.toLowerCase().includes(queryLower)) {
                score += 10;
            }

            // Category filter
            if (category && doc.category !== category) {
                continue;
            }

            // Content match
            const contentMatches = this.countMatches(doc.content.toLowerCase(), queryLower);
            score += contentMatches * 2;

            // Tags match
            if (doc.tags.some(tag => tag.toLowerCase().includes(queryLower))) {
                score += 5;
            }

            if (score > 0) {
                results.push({
                    ...doc,
                    score,
                    snippet: this.generateSnippet(doc.content, queryLower)
                });
            }
        }

        return results
            .sort((a, b) => b.score - a.score)
            .slice(0, limit);
    }

    countMatches(text, query) {
        return (text.match(new RegExp(query, 'gi')) || []).length;
    }

    generateSnippet(content, query, maxLength = 200) {
        const index = content.toLowerCase().indexOf(query.toLowerCase());
        if (index === -1) {
            return content.substring(0, maxLength) + '...';
        }

        const start = Math.max(0, index - 50);
        const end = Math.min(content.length, index + query.length + 150);

        let snippet = content.substring(start, end);
        if (start > 0) snippet = '...' + snippet;
        if (end < content.length) snippet = snippet + '...';

        // Highlight the query
        snippet = snippet.replace(
            new RegExp(`(${query})`, 'gi'),
            '<mark>$1</mark>'
        );

        return snippet;
    }

    getCategories() {
        if (!this.searchIndex) return [];

        const categories = new Set();
        this.searchIndex.documents.forEach(doc => {
            categories.add(doc.category);
        });

        return Array.from(categories).sort();
    }

    getStats() {
        if (!this.searchIndex) return null;
        return this.searchIndex.stats;
    }
}

// Export for use in Node.js and browsers
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DocumentationSearch;
} else if (typeof window !== 'undefined') {
    window.DocumentationSearch = DocumentationSearch;
}
'''

        with open(output_path, 'w') as f:
            f.write(js_code)

        print(f"✅ Search API exported to {output_path}")

    def export_json_index(self, output_path: str = "docs/static/search_index.json"):
        """Export search index as JSON for client-side search."""
        with sqlite3.connect(self.db_path) as conn:
            # Get all documents
            cursor = conn.execute('''
                SELECT title, file_path, category, tags, last_modified,
                       (SELECT content FROM docs_fts WHERE docs_fts.file_path = docs_metadata.file_path) as content
                FROM docs_metadata
            ''')

            documents = []
            for row in cursor.fetchall():
                documents.append({
                    'title': row[0],
                    'file_path': row[1],
                    'category': row[2],
                    'tags': row[3].split(', ') if row[3] else [],
                    'last_modified': row[4],
                    'content': row[5][:500] if row[5] else ''  # Truncate content for client-side
                })

            # Get stats
            stats = self.get_stats()

            index_data = {
                'generated_at': datetime.now().isoformat(),
                'stats': stats,
                'documents': documents
            }

            with open(output_path, 'w') as f:
                json.dump(index_data, f, indent=2)

            print(f"✅ JSON index exported to {output_path}")


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description='Documentation Search Indexer')
    parser.add_argument('--index', action='store_true', help='Index all documentation')
    parser.add_argument('--search', type=str, help='Search query')
    parser.add_argument('--category', type=str, help='Filter by category')
    parser.add_argument('--export-json', action='store_true', help='Export JSON index')
    parser.add_argument('--export-api', action='store_true', help='Export JavaScript API')
    parser.add_argument('--stats', action='store_true', help='Show index statistics')
    parser.add_argument('--db-path', default='docs/static/search_index.db', help='Database path')

    args = parser.parse_args()

    indexer = DocumentationIndexer(args.db_path)

    if args.index:
        print("🔄 Starting documentation indexing...")
        stats = indexer.index_all_docs()
        print("\n📊 Indexing completed!")
        print(f"   Files found: {stats['total_files_found']}")
        print(f"   Successfully indexed: {stats['indexed_successfully']}")
        print(f"   Errors: {stats['errors']}")
        print(f"   Duration: {stats['duration_seconds']:.2f} seconds")
        print(f"   Speed: {stats['files_per_second']:.2f} files/second")

    if args.search:
        print(f"🔍 Searching for: '{args.search}'")
        results = indexer.search(args.search, category=args.category)

        if not results:
            print("No results found.")
        else:
            print(f"Found {len(results)} results:")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. {result['title']}")
                print(f"   File: {result['file_path']}")
                print(f"   Category: {result['category']}")
                if result['snippet']:
                    print(f"   Snippet: {result['snippet']}")

    if args.export_json:
        indexer.export_json_index()

    if args.export_api:
        indexer.export_search_api()

    if args.stats:
        stats = indexer.get_stats()
        print("📊 Index Statistics:")
        print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
