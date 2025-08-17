"""SQLite metadata database operations."""

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional
from contextlib import contextmanager

from models.file_metadata import FileMetadata
from models.search_models import SearchResult, MatchType


logger = logging.getLogger(__name__)


class MetadataDatabase:
    """SQLite database for exact metadata matching."""
    
    def __init__(self, db_path: str):
        """Initialize the metadata database.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Ensure database and tables exist."""
        try:
            with self._get_connection() as conn:
                self._create_tables(conn)
            logger.info(f"✅ Metadata database ready: {self.db_path}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            raise
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with automatic cleanup."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create database tables."""
        cursor = conn.cursor()
        
        # Main files table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                file_path TEXT UNIQUE NOT NULL,
                creation_time TIMESTAMP,
                extracted_date TEXT,
                content_preview TEXT
            )
        ''')
        
        # Tags table (many-to-many with files)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag_name TEXT UNIQUE NOT NULL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_tags (
                file_id INTEGER,
                tag_id INTEGER,
                FOREIGN KEY (file_id) REFERENCES files (id),
                FOREIGN KEY (tag_id) REFERENCES tags (id),
                PRIMARY KEY (file_id, tag_id)
            )
        ''')
        
        # Create indexes for performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_filename ON files (filename)',
            'CREATE INDEX IF NOT EXISTS idx_extracted_date ON files (extracted_date)',
            'CREATE INDEX IF NOT EXISTS idx_creation_time ON files (creation_time)',
            'CREATE INDEX IF NOT EXISTS idx_tags ON tags (tag_name)'
        ]
        
        for index in indexes:
            cursor.execute(index)
        
        conn.commit()
    
    def insert_file_metadata(self, metadata: FileMetadata, content_preview: str) -> bool:
        """Insert or update file metadata.
        
        Args:
            metadata: FileMetadata object
            content_preview: Preview of file content
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Insert or replace file metadata
                cursor.execute('''
                    INSERT OR REPLACE INTO files 
                    (filename, file_path, creation_time, extracted_date, content_preview)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    metadata.filename,
                    metadata.file_path,
                    metadata.creation_time,
                    metadata.extracted_date,
                    content_preview
                ))
                
                file_id = cursor.lastrowid
                
                # Handle tags - first clear existing tags for this file
                cursor.execute('DELETE FROM file_tags WHERE file_id = ?', (file_id,))
                
                # Insert new tags
                for tag in metadata.tags:
                    # Insert tag if doesn't exist
                    cursor.execute('INSERT OR IGNORE INTO tags (tag_name) VALUES (?)', (tag,))
                    
                    # Get tag ID
                    cursor.execute('SELECT id FROM tags WHERE tag_name = ?', (tag,))
                    tag_id = cursor.fetchone()[0]
                    
                    # Link file to tag
                    cursor.execute('INSERT INTO file_tags (file_id, tag_id) VALUES (?, ?)', (file_id, tag_id))
                
                conn.commit()
                logger.debug(f"✅ Inserted metadata for: {metadata.filename}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error inserting metadata for {metadata.filename}: {e}")
            return False
    
    def search_by_date(self, date_query: str) -> List[SearchResult]:
        """Search files by date patterns.
        
        Args:
            date_query: Date pattern to search for
            
        Returns:
            List of SearchResult objects
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Build search patterns based on date format
                patterns = self._build_date_patterns(date_query)
                
                results = []
                seen_files = set()
                
                for pattern in patterns:
                    cursor.execute('''
                        SELECT filename, file_path, extracted_date, creation_time, content_preview
                        FROM files 
                        WHERE (extracted_date LIKE ? OR filename LIKE ?)
                        ORDER BY 
                            CASE WHEN extracted_date = ? THEN 1 
                                 WHEN extracted_date LIKE ? THEN 2 
                                 ELSE 3 END,
                            filename
                    ''', (pattern, pattern, date_query, f"{date_query}%"))
                    
                    for row in cursor.fetchall():
                        if row[1] not in seen_files:  # file_path not seen
                            seen_files.add(row[1])
                            results.append(SearchResult(
                                filename=row[0],
                                file_path=row[1],
                                extracted_date=row[2],
                                creation_time=row[3],
                                content_preview=row[4],
                                match_type=MatchType.DATE
                            ))
                
                logger.debug(f"📅 Date search '{date_query}': {len(results)} results")
                return results
                
        except Exception as e:
            logger.error(f"❌ Error in date search: {e}")
            return []
    
    def search_by_tags(self, tag_query: str) -> List[SearchResult]:
        """Search files by tags.
        
        Args:
            tag_query: Tag to search for
            
        Returns:
            List of SearchResult objects
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT f.filename, f.file_path, f.extracted_date, f.creation_time, f.content_preview
                    FROM files f
                    JOIN file_tags ft ON f.id = ft.file_id
                    JOIN tags t ON ft.tag_id = t.id
                    WHERE t.tag_name LIKE ?
                    ORDER BY f.filename
                ''', (f"%{tag_query}%",))
                
                results = []
                for row in cursor.fetchall():
                    results.append(SearchResult(
                        filename=row[0],
                        file_path=row[1],
                        extracted_date=row[2],
                        creation_time=row[3],
                        content_preview=row[4],
                        match_type=MatchType.TAG
                    ))
                
                logger.debug(f"🏷️ Tag search '{tag_query}': {len(results)} results")
                return results
                
        except Exception as e:
            logger.error(f"❌ Error in tag search: {e}")
            return []
    
    def search_by_filename(self, filename_query: str) -> List[SearchResult]:
        """Search files by filename patterns.
        
        Args:
            filename_query: Filename pattern to search for
            
        Returns:
            List of SearchResult objects
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT filename, file_path, extracted_date, creation_time, content_preview
                    FROM files 
                    WHERE filename LIKE ?
                    ORDER BY 
                        CASE WHEN filename = ? THEN 1 ELSE 2 END,
                        LENGTH(filename)
                ''', (f"%{filename_query}%", filename_query))
                
                results = []
                for row in cursor.fetchall():
                    results.append(SearchResult(
                        filename=row[0],
                        file_path=row[1],
                        extracted_date=row[2],
                        creation_time=row[3],
                        content_preview=row[4],
                        match_type=MatchType.FILENAME
                    ))
                
                logger.debug(f"📄 Filename search '{filename_query}': {len(results)} results")
                return results
                
        except Exception as e:
            logger.error(f"❌ Error in filename search: {e}")
            return []
    
    def search_by_content(self, content_query: str) -> List[SearchResult]:
        """Search files by content preview.
        
        Args:
            content_query: Content to search for
            
        Returns:
            List of SearchResult objects
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT filename, file_path, extracted_date, creation_time, content_preview
                    FROM files 
                    WHERE content_preview LIKE ?
                    ORDER BY LENGTH(filename)
                ''', (f"%{content_query}%",))
                
                results = []
                for row in cursor.fetchall():
                    results.append(SearchResult(
                        filename=row[0],
                        file_path=row[1],
                        extracted_date=row[2],
                        creation_time=row[3],
                        content_preview=row[4],
                        match_type=MatchType.CONTENT
                    ))
                
                logger.debug(f"📝 Content search '{content_query}': {len(results)} results")
                return results
                
        except Exception as e:
            logger.error(f"❌ Error in content search: {e}")
            return []
    
    def _build_date_patterns(self, date_query: str) -> List[str]:
        """Build search patterns for different date formats."""
        import re
        
        patterns = []
        
        if re.match(r'\d{4}-\d{2}$', date_query):  # YYYY-MM format
            patterns.extend([
                f"{date_query}%",  # 2025-01-XX
                f"{date_query}",   # exact match
                date_query.replace('-', ''),  # 202501
            ])
        elif re.match(r'\d{4}-\d{2}-\d{2}$', date_query):  # Full date
            patterns.extend([
                date_query,
                f"{date_query}%",
            ])
        elif re.match(r'\d{4}$', date_query):  # Year only
            patterns.extend([
                f"{date_query}-%",
                f"{date_query}",
            ])
        else:
            # Fallback patterns
            patterns.extend([
                f"{date_query}%",
                f"%{date_query}%",
            ])
        
        return patterns
