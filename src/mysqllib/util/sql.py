# Methods
def list_to_sql(items):
    result = ""
    for item in items:
        result += f"'{item}',"
    result = result[:len(result)-1]
    return result
