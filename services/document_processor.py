"""Document processing service for YAML frontmatter and metadata extraction."""

import os
import re
import yaml
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

from llama_index.core import Document

from models.file_metadata import FileMetadata


logger = logging.getLogger(__name__)


class DocumentProcessor:
    """Process documents and extract metadata for hybrid RDBMS + semantic search."""
    
    @staticmethod
    def parse_yaml_frontmatter(content: str) -> Tuple[Dict, str]:
        """Parse YAML frontmatter from Obsidian markdown files.
        
        Args:
            content: Raw file content
            
        Returns:
            Tuple of (frontmatter_dict, content_without_frontmatter)
        """
        if not content.startswith('---'):
            return {}, content
            
        try:
            # Find the end of frontmatter
            end_marker = content.find('\n---\n', 3)
            if end_marker == -1:
                end_marker = content.find('\n---', 3)
                if end_marker == -1:
                    return {}, content
                    
            # Extract frontmatter
            frontmatter_text = content[3:end_marker]
            remaining_content = content[end_marker + 4:].strip()
            
            # Parse YAML
            frontmatter = yaml.safe_load(frontmatter_text) or {}
            return frontmatter, remaining_content
            
        except yaml.YAMLError as e:
            logger.warning(f"Could not parse YAML frontmatter: {e}")
            return {}, content
    
    @staticmethod
    def extract_hashtags_from_content(content: str) -> List[str]:
        """Extract hashtags from content (after frontmatter removal).
        
        Args:
            content: Content to extract hashtags from
            
        Returns:
            List of hashtags found
        """
        tag_pattern = r'#([a-zA-Z0-9_-]+)'
        return list(set(re.findall(tag_pattern, content)))
    
    @staticmethod
    def extract_all_tags(frontmatter: Dict, content: str) -> List[str]:
        """Extract tags from both YAML frontmatter and content hashtags.
        
        Args:
            frontmatter: Parsed YAML frontmatter
            content: Content without frontmatter
            
        Returns:
            List of all tags found
        """
        tags = []
        
        # Get tags from YAML frontmatter
        yaml_tags = frontmatter.get('tags', [])
        if isinstance(yaml_tags, str):
            # Handle single tag as string
            tags.append(yaml_tags)
        elif isinstance(yaml_tags, list):
            # Handle list of tags
            tags.extend([str(tag) for tag in yaml_tags])
            
        # Get hashtags from content
        content_tags = DocumentProcessor.extract_hashtags_from_content(content)
        tags.extend(content_tags)
        
        # Clean and deduplicate
        cleaned_tags = []
        for tag in tags:
            clean_tag = str(tag).strip().lower()
            if clean_tag and clean_tag not in cleaned_tags:
                cleaned_tags.append(clean_tag)
                
        return cleaned_tags
    
    @staticmethod
    def extract_date_from_frontmatter(frontmatter: Dict) -> str:
        """Extract date from YAML frontmatter.
        
        Args:
            frontmatter: Parsed YAML frontmatter
            
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        if 'date' not in frontmatter:
            return None
            
        date_value = frontmatter['date']
        
        if isinstance(date_value, str):
            # Try to parse date string
            try:
                parsed_date = datetime.strptime(date_value[:10], '%Y-%m-%d')
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"Could not parse date string: {date_value}")
                return None
        elif hasattr(date_value, 'strftime'):
            # It's already a date object
            return date_value.strftime('%Y-%m-%d')
        
        return None
    
    @staticmethod
    def extract_date_from_filename(filename: str) -> str:
        """Extract date from filename pattern.
        
        Args:
            filename: Name of the file
            
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        return date_match.group(1) if date_match else None
    
    @classmethod
    def extract_file_metadata(cls, doc: Document) -> FileMetadata:
        """Extract structured metadata from document including YAML frontmatter.
        
        Args:
            doc: LlamaIndex Document object
            
        Returns:
            FileMetadata object with extracted information
        """
        filename = doc.metadata.get('file_name', '')
        file_path = doc.metadata.get('file_path', '')
        
        # Parse YAML frontmatter
        frontmatter, content_without_frontmatter = cls.parse_yaml_frontmatter(doc.text)
        
        # Get creation time from file system
        creation_time = datetime.now()
        try:
            if file_path and os.path.exists(file_path):
                stat = os.stat(file_path)
                creation_time = datetime.fromtimestamp(stat.st_ctime)
        except Exception as e:
            logger.warning(f"Could not get file stats for {file_path}: {e}")
        
        # Extract date - try frontmatter first, then filename
        extracted_date = cls.extract_date_from_frontmatter(frontmatter)
        if not extracted_date:
            extracted_date = cls.extract_date_from_filename(filename)
        
        # Extract tags from both frontmatter and content
        tags = cls.extract_all_tags(frontmatter, content_without_frontmatter)
        
        return FileMetadata(
            filename=filename,
            file_path=file_path,
            creation_time=creation_time,
            extracted_date=extracted_date,
            tags=tags
        )
    
    @classmethod
    def create_enhanced_document(cls, doc: Document, metadata: FileMetadata) -> Document:
        """Create enhanced document with metadata for semantic search.
        
        Args:
            doc: Original document
            metadata: Extracted metadata
            
        Returns:
            Enhanced document with metadata prepended
        """
        enhanced_text = f"File: {metadata.filename}\n"
        
        if metadata.extracted_date:
            enhanced_text += f"Date: {metadata.extracted_date}\n"
        
        if metadata.tags:
            enhanced_text += f"Tags: {', '.join(metadata.tags)}\n"
        
        enhanced_text += f"\nContent:\n{doc.text}"
        
        return Document(text=enhanced_text, metadata=doc.metadata)
    
    @classmethod
    def create_content_preview(cls, doc: Document, max_length: int = 1000) -> str:
        """Create content preview for database storage.
        
        Args:
            doc: Document to create preview from
            max_length: Maximum length of preview
            
        Returns:
            Content preview string
        """
        # For content preview, we want the full text including frontmatter
        # as it contains important metadata like title and description
        preview = doc.text[:max_length]
        
        # Ensure we don't cut off in the middle of a word
        if len(doc.text) > max_length:
            last_space = preview.rfind(' ')
            if last_space > max_length * 0.8:  # Only if we're not cutting too much
                preview = preview[:last_space] + "..."
        
        return preview
