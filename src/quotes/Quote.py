from dataclasses import dataclass, field
from typing import List, Type, Optional
from src.quotes.Tag import Tag


@dataclass
class Quote:
    quote: str
    author: str
    telegram_id: int
    quote_id: int = None
    translation: str = None
    quote_ita: str = None
    last_random_tme: int = None
    private: bool = True
    created: int = None
    last_modified: int = None
    tags: List[Tag] = field(default_factory=lambda: [])

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

