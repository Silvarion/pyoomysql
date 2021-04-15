import json
from .Table import Table

class Column:
    # Constructor
    def __init__(self, table: Table, name: str, length: int, precision: int, nullable: bool = True, autoincrement: bool = False, charset: str = None):
        self.table = table
        self.name = name
        self.length = length
        self.precision = precision
        self.nullable = nullable
        self.autoincrement = autoincrement
        if charset is None:
            charset = self.table.schema.charset
        else:
            self.charset = charset

    # Python Object Overrides
    def __str__(self):
        pass

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']
