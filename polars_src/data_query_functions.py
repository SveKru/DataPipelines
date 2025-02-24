import polars as pl
from polars_src.custom_dataframes import CustomDF

def get_record_history(table_name:str, record_id: str, depth: int = 0,cache_dict: dict = {}):

    if table_name not in cache_dict:
        cache_dict[table_name] = CustomDF('record_tracing',partition_name=table_name).data
        
    df = cache_dict[table_name].filter(pl.col('target_RecordID')==record_id)

    print('\t'*depth+f"{table_name} - {record_id};")
    if df.height > 0:
        recursive_list = df.select("source_RecordID", "source_table_name").rows()
        for item in recursive_list:
            source_id, source_table = item
            get_record_history(source_table,source_id,depth=depth+1,cache_dict=cache_dict)
