from dataclasses import dataclass, field, asdict
from typing import List, Type, Optional
from quotes import Tag


@dataclass
class Note:
    TYPE_NAME = 'Note'

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
    tags: List[Tag] = field(default_factory=lambda: [])

    def to_dict(self):
        result = asdict(self)
        for key, value in result.items():
            if hasattr(value, 'to_dict'):
                result[key] = value.to_dict()
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def get_list_tags(self) -> List[str]:
        return [x.tag for x in self.tags]

