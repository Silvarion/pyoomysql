from .Database import Database
from .Schema import Schema
from _io import TextIOWrapper
import json

class Model:
    # Constructor
    def __init__(self,database: Database, name: str, encoding = "utf8mb4"):
        self.database = database
        self.name = name
        self.encoding = encoding
        self.schemas = []

    # String
    def __str__(self):
        pass

    # Load from JSON file
    def load(self, source, path):
        try:
            with open(path) as json_file:
                model_json = json.load(json_file)
                for key in model_json.keys():
                    if key == "name":
                        self.name = model_json[key]
                    elif key == "author":
                        self.author = model_json[key]
                    elif key == "maintainer":
                        self.maintainer = model_json[key]
                    elif key == "schemas":
                        for schema in json_file[key]:
                            loaded_schema = Schema(database=self.database,name="new")
                            loaded_schema.load_from_json(schema)
                            self.schemas.append()
        except Exception as err:
            print(err)

    # Dump to file
    def dump(self, path):
        pass

    # Deploy to Database
    def deploy(self, target: Database):
        pass

    # Parse SQL to Model
    def parse_sql(self, sql):
        if type(sql) is TextIOWrapper:
            all_lines = sql.readlines()
        elif type(sql) is str:
            all_lines = sql.split("\n")
        script=[]
        sql = ""
        for line in all_lines:
            sql += line
            if line.strip(" ")[(len(line)-1):] == ';':
                script.append(sql)
                sql = ""
        for sql in script:
            pass