from .Table import Table

class Column:
    # Constructor
    def __init__(self, table: Table, name: str, length: int, precision: int, nullable: bool, autoincrement: bool, charset: str = self.table.charset):
        pass

    # Python Object Overrides
    def __str__(self):
        pass

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']
