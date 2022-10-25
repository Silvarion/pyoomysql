import json
from . import *

class Column:
    # Constructor
    def __init__(self, table: Table, name: str, ordinal_position: int, type: str, length: int, default = None, nullable: bool = True, autoincrement: bool = False, charset: str = None, collation: str = None):
        self.table = table
        self.name = name
        self.ordinal_position = ordinal_position
        self.type = type
        self.length = length
        self.default = default
        self.nullable = nullable
        self.autoincrement = autoincrement
        if charset is None:
            charset = self.table.schema.charset
        else:
            self.charset = charset

    # Python Object Overrides
    def __str__(self):
        return json.dumps({
            "name": self.name,
            "ordinal_position": self.ordinal_position,
            "type": self.type,
            "length": self.length,
            "default": self.default,
            "nullable": self.nullable,
            "autoincrement": self.autoincrement,
            "charset": self.charset
        })

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']
