"""LLM-powered query parsing to extract structured data from natural language."""

import json
import logging
from typing import Dict, Any, List

from llama_index.core.settings import Settings

logger = logging.getLogger(__name__)


def parse_query_with_llm(query: str) -> Dict[str, Any]:
    """Parse query using LLM and return structured data.
    
    Requires LLM to be configured - will raise ValueError if LLM is unavailable.
    """
    # Ensure an LLM is configured - REQUIRED, no fallback
    try:
        llm_instance = Settings.llm
        if llm_instance is None:
            raise ValueError("LLM is required but Settings.llm is None")
        logger.info(f"LLM available: {type(llm_instance).__name__}")
    except Exception as e:
        raise ValueError(f"LLM is required but not available: {e}")

    prompt = f"""You must analyze this query EXACTLY and return JSON: "{query}"

STRICT RULES:
1. DATES array: Add dates ONLY if the query contains month names (January, March), years (2024, 2025), quarters (Q1, Q2), or specific dates. If NO date words exist, dates must be empty [].
2. TAGS array: Extract the main topic mentioned and add related words.

Template: {{"dates": [], "tags": []}}

CRITICAL: For "{query}" - scan for date words first:
- Does it contain months? (January, February, March, etc.)
- Does it contain years? (2024, 2025, etc.) 
- Does it contain quarters? (Q1, Q2, etc.)
- If NO date words found, dates MUST be []

Examples:
- "jan 2023" → {{"dates": ["2023-01"], "tags": []}}
- "Q1 2025" → {{"dates": ["2025-01", "2025-02", "2025-03"], "tags": []}}
- "what is kubernetes?" → {{"dates": [], "tags": ["kubernetes", "k8s", "container", "orchestration"]}}
- "what do I know about stripe?" → {{"dates": [], "tags": ["stripe", "payment", "payments", "billing", "api"]}}
- "notes about AI" → {{"dates": [], "tags": ["ai", "artificial intelligence", "machine learning", "ml"]}}

Return ONLY JSON for: "{query}"""

    try:
        response = Settings.llm.complete(prompt)
        response_text = response.text.strip()
        
        logger.info(f"Raw LLM response: {repr(response_text)}")
        
        # Clean up response
        if response_text.startswith('```json'):
            response_text = response_text[7:]
        if response_text.startswith('```'):
            response_text = response_text[3:]
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        # Find JSON with better error handling
        start = response_text.find('{')
        end = response_text.rfind('}') + 1
        if start >= 0 and end > start:
            response_text = response_text[start:end]
        
        if not response_text.startswith('{'):
            # Try to extract JSON using regex as fallback
            import re
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group()
            else:
                # Create fallback result
                logger.warning(f"Could not extract JSON from response: {response_text[:100]}...")
                return {"dates": [], "tags": []}
        
        # Try to parse JSON with comprehensive error recovery
        try:
            parsed = json.loads(response_text)
        except json.JSONDecodeError as e:
            # Try multiple fixes for common JSON issues
            import re
            
            # Fix 1: Replace single quotes with double quotes
            fixed_json = response_text.replace("'", '"')
            
            # Fix 2: Fix unquoted property names
            fixed_json = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', fixed_json)
            
            # Fix 3: Fix unquoted string values in arrays
            fixed_json = re.sub(r':\s*\[([^\]]*)\]', lambda m: '[' + re.sub(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*([,\]])', r'"\1"\2', m.group(1)) + ']', fixed_json)
            
            # Fix 4: Fix trailing commas
            fixed_json = re.sub(r',\s*}', '}', fixed_json)
            fixed_json = re.sub(r',\s*]', ']', fixed_json)
            
            try:
                parsed = json.loads(fixed_json)
            except json.JSONDecodeError:
                # Fix 5: Try to extract just the structure we need
                try:
                    dates_match = re.search(r'"dates"\s*:\s*\[([^\]]*)\]', fixed_json)
                    tags_match = re.search(r'"tags"\s*:\s*\[([^\]]*)\]', fixed_json)
                    
                    dates = []
                    tags = []
                    
                    if dates_match:
                        dates_str = dates_match.group(1)
                        dates = [d.strip().strip('"\'') for d in dates_str.split(',') if d.strip()]
                    
                    if tags_match:
                        tags_str = tags_match.group(1)
                        tags = [t.strip().strip('"\'') for t in tags_str.split(',') if t.strip()]
                    
                    parsed = {"dates": dates, "tags": tags}
                except Exception:
                    logger.warning(f"Could not parse JSON even after comprehensive fixing: {response_text[:100]}...")
                    return {"dates": [], "tags": []}
        
        # Ensure result has required fields
        if not isinstance(parsed, dict):
            parsed = {"dates": [], "tags": []}
        
        if "dates" not in parsed:
            parsed["dates"] = []
        if "tags" not in parsed:
            parsed["tags"] = []
        
        logger.debug(f"Parsed result: {parsed}")
        return parsed
        
    except Exception as e:
        raise ValueError(f"LLM parsing failed: {e}")