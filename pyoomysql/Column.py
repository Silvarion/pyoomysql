import 
import json

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
        return(json.dumps(self.json_dump))

    # Attributes and methods getters
    def get_attributes(self):
        return ['name', 'length', 'precision', 'nullable', 'autoincrement']

    def get_methods(self):
        return ['get_attributes', 'get_columns']

    # JSON Methods
    def json_load(self, json_data: str):
        for key in json_data:
            if key == "name":
                self.name = json_data[key]
            elif key == "position":
                self.position = json_data[key]
            elif key == "datatype":
                self.datatype = json_data[key]
            elif key == "length":
                self.length = json_data[key]
            elif key == "precision":
                self.precision = json_data[key]
            elif key == "nullable":
                self.nullable = json_data[key]
            elif key == "autoincrement":
                self.autoincrement = json_data[key]
            elif key == "charset":
                self.charset = json_data[key]

    def json_dump(self):
        return {
            "name": self.name,
            "position": self.position,
            "datatype": self.datatype,
            "length": self.length,
            "presicion": self.precision,
            "nullable": self.nullable,
            "autoincrement": self.autoincrement
        }