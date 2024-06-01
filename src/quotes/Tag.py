from dataclasses import dataclass, field
from typing import List, Type, Optional


@dataclass
class Tag:
    tag: str
    note_id: int = None
    quote_id: int = None

    @classmethod
    def from_dict(cls, data):
        return cls(**data)
