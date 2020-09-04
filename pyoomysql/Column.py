class Column:
    # Constructor
    def __init__(self, name: str, length: int, precision: int, nullable: bool, autoincrement: bool):
        pass

    # Python Object Overrides
    def __str__(self):
        pass

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']
