"""Search-related data models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Any
from enum import Enum


class SearchType(Enum):
    """Types of search strategies."""
    RDBMS = "rdbms"


class MatchType(Enum):
    """Types of matches found."""
    DATE = "date"
    DATE_RANGE = "date_range"
    TAG = "tag"
    FILENAME = "filename"
    CONTENT = "content"


@dataclass
class SearchQuery:
    """Represents a user search query."""
    raw_query: str
    dates: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    filenames: List[str] = field(default_factory=list)
    search_type: SearchType = SearchType.RDBMS
    
    @property
    def has_structured_data(self) -> bool:
        """Check if query has any structured search criteria."""
        return bool(self.dates or self.tags or self.filenames)


@dataclass
class SearchResult:
    """Represents a search result."""
    filename: str
    file_path: str
    extracted_date: Optional[str] = None
    creation_time: Optional[datetime] = None
    content_preview: str = ""
    content: str = ""
    match_type: MatchType = MatchType.CONTENT
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'filename': self.filename,
            'file_path': self.file_path,
            'extracted_date': self.extracted_date,
            'creation_time': self.creation_time.isoformat() if self.creation_time else None,
            'content_preview': self.content_preview,
            'content': self.content,
            'match_type': self.match_type.value
        }


@dataclass
class QueryAnalysis:
    """Analysis results from query parsing."""
    original_query: str
    parsed_query: SearchQuery
    search_strategy: SearchType = SearchType.RDBMS
    reasoning: str = ""
    
    @classmethod
    def from_llm_result(cls, query: str, llm_result: Dict[str, Any]) -> 'QueryAnalysis':
        """Create QueryAnalysis from LLM parsing result."""
        parsed_query = SearchQuery(
            raw_query=query,
            dates=llm_result.get('dates', []),
            tags=llm_result.get('tags', []),
            filenames=llm_result.get('filenames', []),
            search_type=SearchType.RDBMS
        )
        
        return cls(
            original_query=query,
            parsed_query=parsed_query,
            search_strategy=SearchType.RDBMS,
            reasoning=llm_result.get('reasoning', '')
        )
