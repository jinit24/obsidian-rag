"""Main RAG service orchestrating all components."""

import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from llama_index.core import SimpleDirectoryReader, Document
from llama_index.core.settings import Settings

from config.settings import load_config
from database.metadata_db import MetadataDatabase
from models.search_models import SearchResult, QueryAnalysis
from services.document_processor import DocumentProcessor
from services.query_parser import parse_query_with_llm


logger = logging.getLogger(__name__)


class RAGService:
    """Main RAG service using RDBMS for exact matching."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize RAG service.
        
        Args:
            config: Application configuration dictionary
        """
        self.config = config
        
        # Initialize components
        self.metadata_db = MetadataDatabase(config['paths']['metadata_db_path'])
        self.document_processor = DocumentProcessor()
        
        # Document storage
        self.documents: List[Document] = []
        
        logger.info("🚀 RAG Service initialized (RDBMS-only)")
    
    def load_and_process_documents(self) -> bool:
        """Load documents from vault and populate RDBMS database.
        
        Returns:
            True if successful, False otherwise
        """
        vault_path = Path(self.config['paths']['vault_path'])
        
        if not vault_path.exists():
            logger.error(f"❌ Vault directory does not exist: {vault_path}")
            return False
        
        logger.info(f"📂 Loading documents from: {vault_path}")
        
        try:
            # Load documents
            reader = SimpleDirectoryReader(
                input_dir=str(vault_path),
                required_exts=[".md"],
                recursive=True
            )
            self.documents = reader.load_data()
            logger.info(f"📚 Loaded {len(self.documents)} documents")
            
            # Process documents for RDBMS
            for doc in self.documents:
                # Extract metadata
                metadata = self.document_processor.extract_file_metadata(doc)
                
                # Create content preview for RDBMS
                content_preview = self.document_processor.create_content_preview(
                    doc, self.config['search']['content_preview_length']
                )
                
                # Store in metadata database
                success = self.metadata_db.insert_file_metadata(metadata, content_preview)
                if not success:
                    logger.warning(f"⚠️ Failed to store metadata for: {metadata.filename}")
            
            logger.info("✅ All documents processed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing documents: {e}")
            return False
    
    def search(self, query: str) -> Dict[str, Any]:
        """Perform RDBMS search for exact matching.
        
        Args:
            query: User search query
            
        Returns:
            Dictionary with search results and metadata
        """
        logger.info(f"🔍 RDBMS SEARCH: '{query}'")
        
        # Parse query
        llm_result = parse_query_with_llm(query)
        analysis = QueryAnalysis.from_llm_result(query, llm_result)
        logger.info(f"📊 Query Analysis: {analysis.search_strategy.value}")
        logger.debug(f"   Parsed: dates={analysis.parsed_query.dates}, tags={analysis.parsed_query.tags}")
        
        # RDBMS Search (exact matching)
        rdbms_results = self._perform_rdbms_search(analysis)
        logger.info(f"🗃️ RDBMS found {len(rdbms_results)} results")
        
        results = {
            'rdbms_results': rdbms_results,
            'query_analysis': analysis,
            'total_results': len(rdbms_results)
        }
        
        logger.info(f"📊 Total results: {results['total_results']}")
        return results
    
    def generate_response(self, query: str, search_results: List[SearchResult]) -> str:
        """Generate response using LLM with search results.
        
        Args:
            query: Original user query
            search_results: List of search results
            
        Returns:
            Generated response string
        """
        if not search_results:
            return f"No relevant documents found for: {query}"
        
        logger.info(f"📋 Generating response from {len(search_results)} results")
        
        # Create context from search results
        context = self._create_context(search_results)
        
        # Generate response with LLM
        try:
            if not Settings.llm:
                return f"Found {len(search_results)} documents but LLM not available for response generation"
                
            prompt = f"""You are answering the user's question: "{query}"

The following documents contain relevant information. Each document's filename indicates the date of the activities described within it.

For example:
- "2025-01-02.md" contains activities from January 2, 2025
- "2024-08-28.md" contains activities from August 28, 2024
- Files dated 2025-01-XX represent activities from January 2025

Documents:
{context}

Based on the activities and information in these dated documents, provide a comprehensive answer to the user's question. Focus on summarizing what was accomplished, planned, or discussed during the relevant time period."""
            
            response = Settings.llm.complete(prompt)
            return response.text.strip()
            
        except Exception as e:
            logger.error(f"❌ Error generating response: {e}")
            return f"Found {len(search_results)} relevant documents but error generating response: {str(e)}"
    
    def _perform_rdbms_search(self, analysis: QueryAnalysis) -> List[SearchResult]:
        """Perform RDBMS search based on query analysis.
        
        Args:
            analysis: Query analysis results
            
        Returns:
            List of search results
        """
        results = []
        
        # Search by dates
        for date in analysis.parsed_query.dates:
            date_results = self.metadata_db.search_by_date(date)
            results.extend(date_results)
            logger.debug(f"📅 Date '{date}': {len(date_results)} results")
        
        # Search by tags
        for tag in analysis.parsed_query.tags:
            tag_results = self.metadata_db.search_by_tags(tag)
            results.extend(tag_results)
            logger.debug(f"🏷️ Tag '{tag}': {len(tag_results)} results")
        
        # Search by filenames
        for filename in analysis.parsed_query.filenames:
            filename_results = self.metadata_db.search_by_filename(filename)
            results.extend(filename_results)
            logger.debug(f"📄 Filename '{filename}': {len(filename_results)} results")
        
        # If no structured results, perform content search
        if not results and not analysis.parsed_query.has_structured_data:
            # Extract keywords from original query for content search
            content_query = analysis.original_query.strip()
            # Remove common query words
            stop_words = {'what', 'is', 'are', 'the', 'how', 'when', 'where', 'who', 'why', 'about', 'find', 'search'}
            keywords = [word for word in content_query.lower().split() if word not in stop_words and len(word) > 2]
            
            for keyword in keywords:
                content_results = self.metadata_db.search_by_content(keyword)
                results.extend(content_results)
                logger.debug(f"📝 Content '{keyword}': {len(content_results)} results")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_results = []
        for result in results:
            key = (result.filename, result.match_type.value)
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        return unique_results
    

    
    def _create_context(self, search_results: List[SearchResult]) -> str:
        """Create context string from search results.
        
        Args:
            search_results: List of search results
            
        Returns:
            Formatted context string
        """
        context_parts = []
        max_docs = min(len(search_results), self.config['search']['max_rdbms_results'])
        
        for i, result in enumerate(search_results[:max_docs], 1):
            content = result.content or result.content_preview or "No content available"
            
            # Try to load full content if preview is insufficient
            if len(content.strip()) < 50 and result.file_path and os.path.exists(result.file_path):
                try:
                    with open(result.file_path, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                    content = file_content[:self.config['search']['content_preview_length']]
                    logger.debug(f"📂 Loaded full content for: {result.filename}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not load file {result.file_path}: {e}")
            
            # Limit content length for context
            if len(content) > 1000:
                content = content[:1000] + "..."
            
            context_parts.append(f"Document {i} ({result.match_type.value}): {result.filename}\n{content}")
        
        return "\n\n".join(context_parts)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get system statistics.
        
        Returns:
            Dictionary with system stats
        """
        return {
            'documents_loaded': len(self.documents),
            'vault_path': self.config['paths']['vault_path'],
            'config': {
                'max_rdbms_results': self.config['search']['max_rdbms_results'],
                'content_preview_length': self.config['search']['content_preview_length']
            }
        }
