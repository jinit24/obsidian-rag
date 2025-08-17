"""Simple entry point for RAG Obsidian system."""

import logging
import sys
import time
from typing import Optional

import typer
from llama_index.core.settings import Settings
from llama_index.llms.ollama import Ollama

from config.settings import load_config
from services.rag_service import RAGService
from services.document_enricher import DocumentEnricher

# Create the main typer app
app = typer.Typer(
    name="rag-obsidian",
    help="Search and enrich your Obsidian vault with AI-powered RAG",
    add_completion=False
)


def setup_logging(debug: bool = False):
    """Setup basic logging."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)-5s | %(message)s',
        datefmt='%H:%M:%S'
    )
    # Silence noisy libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def configure_llm(config):
    """Configure LlamaIndex LLM."""
    Settings.llm = Ollama(
        model=config['models']['llm_model'],
        request_timeout=config['models']['llm_timeout']
    )


def interactive_mode(rag_service):
    """Simple interactive question-answering."""
    print("\\n🚀 RAG Obsidian")
    print("Type 'quit' to exit, 'stats' for statistics")
    print("-" * 40)
    
    while True:
        try:
            query = input("\\nQuestion: ").strip()
            
            if not query:
                continue
            if query.lower() in ['quit', 'exit', 'q']:
                break
            if query.lower() == 'stats':
                stats = rag_service.get_stats()
                print(f"\\n📊 {stats['documents_loaded']} documents loaded")
                continue
            
            # Search and respond
            search_results = rag_service.search(query)
            response = rag_service.generate_response(query, search_results['rdbms_results'])
            print(f"\\n📝 {response}")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error: {e}")


@app.command()
def enrich(
    force_update: bool = typer.Option(False, "--force-update", help="Force update existing frontmatter"),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="Maximum number of files to process (for testing)"),
    max_workers: int = typer.Option(16, "--max-workers", help="Maximum number of worker threads for parallel processing"),
    sequential: bool = typer.Option(False, "--sequential", help="Use sequential processing instead of parallel"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging")
):
    """Enrich Obsidian documents with LLM-generated frontmatter."""
    setup_logging(debug)
    
    typer.echo("🚀 RAG Obsidian - Document Enrichment Mode")
    typer.echo("=" * 50)
    
    try:
        # Load config and setup
        config = load_config()
        configure_llm(config)
        
        # Initialize enricher with worker configuration
        enricher = DocumentEnricher(max_workers=max_workers)
        
        # Get vault path from config
        vault_path = config['paths']['vault_path']
        typer.echo(f"📂 Processing documents in: {vault_path}")
        
        if max_files:
            typer.echo(f"⚠️ Limited to {max_files} files for testing")
        
        if force_update:
            typer.echo("⚠️ Force update enabled - will overwrite existing frontmatter")
        
        if sequential:
            typer.echo("📝 Sequential processing enabled")
        else:
            typer.echo(f"⚡ Parallel processing with {max_workers} workers")
        
        typer.echo("-" * 50)
        
        # Start timing
        start_time = time.time()
        
        # Enrich documents
        stats = enricher.enrich_directory(vault_path, force_update, max_files, parallel=not sequential)
        
        # Calculate timing
        end_time = time.time()
        duration = end_time - start_time
        
        typer.echo("=" * 50)
        typer.echo(f"✅ Enrichment Complete!")
        typer.echo(f"📊 Success: {stats['success']}")
        typer.echo(f"❌ Failed: {stats['failed']}")
        typer.echo(f"📈 Total: {stats['total']}")
        typer.echo(f"⏱️ Duration: {duration:.2f} seconds")
        
        if stats['total'] > 0:
            avg_time = duration / stats['total']
            typer.echo(f"📈 Average: {avg_time:.2f} seconds per file")
        
    except Exception as e:
        typer.echo(f"❌ Error in enrichment mode: {e}")
        raise typer.Exit(1)


@app.command()
def rag(
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging")
):
    """Start interactive RAG search mode."""
    setup_logging(debug)
    
    try:
        # Load config and setup
        config = load_config()
        configure_llm(config)
        
        # Initialize RAG service
        rag_service = RAGService(config)
        typer.echo("🚀 Loading documents...")
        
        if not rag_service.load_and_process_documents():
            typer.echo("❌ Failed to load documents")
            raise typer.Exit(1)
        
        # Start interactive mode
        interactive_mode(rag_service)
        
    except Exception as e:
        typer.echo(f"❌ Error in RAG mode: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\n👋 Goodbye!")
        raise typer.Exit(0)
