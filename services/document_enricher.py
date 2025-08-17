"""Document enrichment service for adding LLM-generated frontmatter to Obsidian documents."""

import os
import yaml
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from threading import Lock

from llama_index.core.settings import Settings

from services.document_processor import DocumentProcessor

logger = logging.getLogger(__name__)


class DocumentEnricher:
    """Service for enriching Obsidian documents with LLM-generated frontmatter."""
    
    def __init__(self, max_workers: int = 16):
        """Initialize the document enricher.
        
        Args:
            max_workers: Maximum number of worker threads for parallel processing
        """
        self.document_processor = DocumentProcessor()
        self.max_workers = max_workers
        self._stats_lock = Lock()
    
    def enrich_file(self, file_path: str, force_update: bool = False) -> Dict[str, Any]:
        """Enrich a single file and return a status dictionary."""
        try:
            if not os.path.exists(file_path):
                logger.error(f"❌ File does not exist: {file_path}")
                return {"status": "failed", "reason": "File not found"}
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not content.strip():
                logger.warning(f"⚠️ File is empty: {file_path}")
                return {"status": "skipped", "reason": "Empty file"}
            
            frontmatter, content_without_frontmatter = self.document_processor.parse_yaml_frontmatter(content)
            
            if frontmatter and not force_update:
                logger.info(f"ℹ️ Skipping file with existing frontmatter: {os.path.basename(file_path)}")
                return {"status": "skipped", "reason": "Existing frontmatter"}
            
            file_stat = os.stat(file_path)
            modification_time = datetime.fromtimestamp(file_stat.st_mtime)
            created_date = modification_time.strftime('%Y-%m-%d')
            
            enriched_frontmatter = self._generate_frontmatter(
                content_without_frontmatter, 
                created_date,
                existing_frontmatter=frontmatter
            )
            
            if not enriched_frontmatter:
                logger.error(f"❌ Failed to generate frontmatter for: {os.path.basename(file_path)}")
                return {"status": "failed", "reason": "Frontmatter generation failed"}
            
            frontmatter_yaml = yaml.dump(enriched_frontmatter, default_flow_style=False, sort_keys=False)
            new_content = f"---\n{frontmatter_yaml}---\n\n{content_without_frontmatter}"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.info(f"✅ Enriched file: {os.path.basename(file_path)}")
            return {"status": "success"}
            
        except Exception as e:
            logger.error(f"❌ Error enriching file {file_path}: {e}")
            return {"status": "failed", "reason": str(e)}
    
    def enrich_directory(self, directory_path: str, force_update: bool = False, max_files: int = None, parallel: bool = True) -> Dict[str, Any]:
        """Enrich all markdown files in a directory."""
        directory = Path(directory_path)
        if not directory.exists():
            logger.error(f"❌ Directory does not exist: {directory_path}")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}
        
        md_files = list(directory.glob("**/*.md"))
        if max_files:
            md_files = md_files[:max_files]
        
        if not md_files:
            logger.info("ℹ️ No markdown files found to process")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}
        
        logger.info(f"🚀 Starting enrichment of {len(md_files)} files in {directory_path}")
        
        if parallel and len(md_files) > 1:
            logger.info(f"⚡ Using parallel processing with {self.max_workers} threads")
            return self._enrich_directory_parallel(md_files, force_update)
        else:
            logger.info("📝 Using sequential processing")
            return self._enrich_directory_sequential(md_files, force_update)
    
    def _enrich_directory_sequential(self, md_files: List[Path], force_update: bool) -> Dict[str, Any]:
        """Sequential enrichment implementation."""
        stats = {"success": 0, "failed": 0, "skipped": 0, "total": len(md_files)}
        
        for file_path in tqdm(md_files, desc="Enriching files sequentially"):
            try:
                result = self.enrich_file(str(file_path), force_update)
                status = result.get("status", "failed")
                stats[status] += 1
            except Exception as e:
                logger.error(f"❌ Error processing {file_path}: {e}")
                stats["failed"] += 1
        
        logger.info(f"📊 Enrichment complete: {stats['success']} success, {stats['failed']} failed, {stats['skipped']} skipped, {stats['total']} total")
        return stats
    
    def _enrich_directory_parallel(self, md_files: List[Path], force_update: bool) -> Dict[str, Any]:
        """Parallel enrichment implementation using ThreadPoolExecutor."""
        stats = {"success": 0, "failed": 0, "skipped": 0, "total": len(md_files)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.enrich_file, str(file_path), force_update): file_path for file_path in md_files}
            
            for future in tqdm(as_completed(future_to_file), total=len(md_files), desc="Enriching files in parallel"):
                try:
                    result = future.result()
                    stats[result["status"]] += 1
                except Exception as e:
                    file_path = future_to_file[future]
                    logger.error(f"❌ Error processing {file_path}: {e}")
                    stats["failed"] += 1
        
        logger.info(f"📊 Parallel enrichment complete: {stats['success']} success, {stats['failed']} failed, {stats['skipped']} skipped, {stats['total']} total")
        return stats
    
    def _generate_frontmatter(self, content: str, created_date: str, existing_frontmatter: Dict = None) -> Optional[Dict[str, Any]]:
        """Generate frontmatter using LLM.
        
        Args:
            content: Content of the document
            created_date: Creation date in YYYY-MM-DD format
            existing_frontmatter: Existing frontmatter to preserve
            
        Returns:
            Dictionary with generated frontmatter or None if failed
        """
        if not Settings.llm:
            logger.error("❌ LLM not available for frontmatter generation")
            return None
        
        # Prepare content preview for LLM (limit to avoid token limits)
        content_preview = content.strip()[:2000]
        if len(content) > 2000:
            content_preview += "..."
        
        prompt = f"""Analyze the following document content and generate YAML frontmatter metadata.

Document content:
{content_preview}

Generate a JSON object with the following fields:
- "title": A concise, descriptive title for the document (max 60 chars)
- "description": A brief description summarizing the content (1-2 sentences)
- "tags": An array of relevant tags (3-8 tags, use lowercase with hyphens)

Guidelines:
- Make the title specific and informative
- Keep descriptions concise but meaningful
- Use specific, relevant tags that categorize the content
- For daily notes, include activity-based tags like "work", "personal", "learning"
- For technical content, include technology/topic tags
- Avoid generic tags like "note" or "document"

Return ONLY a valid JSON object with these exact field names."""

        try:
            response = Settings.llm.complete(prompt)
            response_text = response.text.strip()
            
            logger.debug(f"LLM frontmatter response: {response_text}")
            
            # Clean up response
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            
            response_text = response_text.strip()
            
            # Find JSON object
            start = response_text.find('{')
            end = response_text.rfind('}') + 1
            if start >= 0 and end > start:
                response_text = response_text[start:end]
            
            # Parse LLM response
            import json
            llm_data = json.loads(response_text)
            
            # Build frontmatter
            frontmatter = {}
            
            # Preserve existing fields or set defaults
            if existing_frontmatter:
                frontmatter.update(existing_frontmatter)
            
            # Set standard fields
            frontmatter['created'] = frontmatter.get('created', created_date)
            
            # Set LLM-generated fields
            frontmatter['title'] = llm_data.get('title', 'No content provided')
            frontmatter['description'] = llm_data.get('description', 'No information available to create a description.')
            frontmatter['tags'] = llm_data.get('tags', [])
            
            return frontmatter
            
        except Exception as e:
            logger.error(f"❌ Error generating frontmatter with LLM: {e}")
            
            # Fallback frontmatter
            fallback = {
                'created': created_date,
                'description': 'No information available to create a description.',
                'title': 'No content provided',
                'tags': []
            }
            
            if existing_frontmatter:
                fallback.update(existing_frontmatter)
                
            return fallback
