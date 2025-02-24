from quality_checks.signalling_rules_datamodel import get_signalling_checks_datamodel

def get_signalling_rules(table_name:str)-> list:

    if table_name.split('_')[-1] == 'datamodel':
        return get_signalling_checks_datamodel(table_name)
    else:
        return []
