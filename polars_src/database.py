"""
   This module retrieves the table definition from a predefined dictionary of schemas and their tables.

   Args:
      schema_name (str): The name of the layer to look up in the dictionary (e.g., landingzone, raw, datamodel)
      table_name (str): The name of the table within the specified schema to retrieve.

   Returns:
      dict: The definition of the specified table within the given schema.

   Raises:
      ValueError: If the specified schema_name does not exist in the dictionary, or
                  if the table_name is not provided or does not exist within the specified schema.

   Note:
      - The function requires 'all_table_definitions' to be a predefined dictionary
         where the key is the schema name and the value is another dictionary representing
         tables and their definitions stored in other modules.
      - The table definitions are returned as dictionaries.

"""
from schema.schema_landingzone import get_landingzone_schema
from schema.schema_raw import get_raw_schema
from schema.schema_monitoring import get_monitoring_schema
from schema.schema_datamodel import get_datamodel_schema
from schema.schema_enriched import get_enriched_schema

def get_table_definition(schema_name: str, table_name: str = '') -> dict:

    if table_name in ['monitoring_values', 'record_tracing']:
        schema_name = 'monitoring_layer'

    if schema_name not in ['monitoring_layer','landingzone_layer','raw_layer','datamodel_layer','enriched_layer']:
        raise ValueError(f"Layer {schema_name} does not eixst.")

    # Check if layer exists in the dictionary
    if schema_name == 'monitoring_layer':
        return get_monitoring_schema(table_name)
    if schema_name == 'landingzone_layer':
        return get_landingzone_schema(table_name)
    if schema_name == 'raw_layer':
        return get_raw_schema(table_name)
    if schema_name == 'datamodel_layer':
        return get_datamodel_schema(table_name)
    if schema_name == 'enriched_layer':
        return get_enriched_schema(table_name)
