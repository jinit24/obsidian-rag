"""File metadata models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class FileMetadata:
    """Simplified file metadata for RDBMS storage."""
    filename: str
    file_path: str
    creation_time: datetime
    extracted_date: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Post-initialization validation."""
        if self.tags is None:
            self.tags = []
    
    @property
    def is_dated(self) -> bool:
        """Check if file has an extracted date."""
        return self.extracted_date is not None
    
    def has_tag(self, tag: str) -> bool:
        """Check if file has a specific tag."""
        return tag.lower() in [t.lower() for t in self.tags]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'filename': self.filename,
            'file_path': self.file_path,
            'creation_time': self.creation_time.isoformat(),
            'extracted_date': self.extracted_date,
            'tags': self.tags
        }