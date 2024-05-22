from dataclasses import dataclass, field
from typing import List, Type, Optional


@dataclass
class Note:
    note: str
    telegram_id: int
    note_id: int = None
    is_book: int = False
    last_random_time: int = 1
    private: bool = True
    book: str = None
    pag: int = None
    volume: str = None
    part: str = None
    chapter: str = None
    section: str = None
    paragraph: str = None
    created: int = None
    last_modified: int = None
    tags: List[str] = field(default_factory=lambda: [])
