# Methods
def list_to_sql(items):
    result = "("
    for item in items:
        if type(item) is str:
            result += f"'{item}',"
        else:
            result += f"{item},"
    result = result[:len(result)-1]
    result+=")"
    return result
