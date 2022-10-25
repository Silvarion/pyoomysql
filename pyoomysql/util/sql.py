import logging

# Methods
def list_to_column_list(columns):
    result = ""
    for item in columns:
        result += f"{item},"
    result = result[:len(result)-1]
    result+=""
    return result

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

"""
Condition format:
{
    'prefix': ['and', 'or'],
    'column': <column name>,
    'operator': ['=', '<', '>', '<=', '>=', 'LIKE', 'BETWEEN', 'IN']
    'values': value or list of values as a list or set
}
"""
def parse_condition(condition: dict):
    logger = logging.getLogger(name="pyoomysql:util:sql:parse_condition")
    logger.debug("Entering function")
    result=""
    if "prefix" in condition.keys():
        result = f"{condition['prefix']} "
    result+=f"{condition['column']} {condition['operator']} "
    if type(condition["value"]) is str:
        result+=f"'{condition['value']}' "
    elif type(condition["value"]) is list and condition["operator"].upper() == "IN":
        result+=f"{list_to_sql(condition['value'])} "
    elif type(condition["value"]) is list and condition["operator"].upper() == "BETWEEN":
        result+=f"{list_to_sql(condition['value'][0])} AND {list_to_sql(condition['value'][1])} "
    else:
        result+=f"{condition['value']} "
    return result

def grant_to_dict(grant: str):
    logger = logging.getLogger(name="pyoomysql:util:sql:grant_to_dict")
    logger.debug("Entering function")
    grant = grant.lower()
    logger.debug(f"Processing: {grant}")
    privs = grant[(grant.find("grant ")+5):grant.find(" on ")].strip().replace("`","")
    logger.debug(f"Privs: {privs}")
    obj = grant[(grant.find(" on ")+4):grant.find(" to ")].strip().replace("`","")
    logger.debug(f"Object: {obj}")
    grant = {
        "privs": privs.lower(),
        "object": obj
    }
    return grant