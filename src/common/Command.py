from dataclasses import dataclass, field
from typing import Type

from src.common.functions.Function import Function


@dataclass
class Command:
    alias: list[str]
    admin: bool
    function: Type[Function]
    restricted: bool = False
