import polars as pl
import json
from pathlib import Path
from typing import Dict, Optional
from jsonpath_ng import  parse

from polars_src.dataframe_helpers import clean_column_names, create_map_column

class DataReader:
    """
    A class to handle reading data from various sources using Polars.
    
    This class provides functionality to read CSV and Parquet files, handling
    different schemas and partitioning strategies.
    
    Attributes:
        _env (str): Environment setting (e.g., 'develop', 'production')
        _schema (dict): Schema definition for the data
        _partition_name (str): Name of the partition column if data is partitioned
        _history (str): History setting ('complete' or 'recent')
        _data_path (str): Full path to the data source
    """
    
    def __init__(
        self,
        env: str,
        schema: Dict,
        partition_name: Optional[str] = None,
        history: str = "recent"
    ):
        self._env = env
        self._schema = schema
        self._partition_name = partition_name
        self._history = history
        self._data_path = self._build_data_path()

        # if self._schema['partition_column'] and not self._partition_name:
        #     raise ValueError('A partitioned dataframe is being read without a specified partition')

    def _build_data_path(self) -> str:
        """
        Build the full path to the data source based on schema and environment settings.
        
        Returns:
            str: Full path to the data source
        """
        # Build the base path
        path_parts = [
            'data',
            self._env,
            self._schema["container"],
            self._schema["location"]
        ]
        
        # Add partition if specified
        # if self._partition_name:
        #     path_parts.append(f"{self._schema['partition_column']}={self._partition_name}")
            
        # Join path parts
        return str(Path(*path_parts))

    def read_source(self) -> pl.DataFrame:
        """
        Read data from the source based on the file format.
        
        Returns:
            pl.DataFrame: DataFrame containing the source data
        
        Raises:
            ValueError: If the file format is not supported
        """
        file_format = self._schema.get("file_format", "empty_file_format_field").lower()

        if not Path(self._data_path).exists():
            if self._schema['partition_column']:
                partition_columns = self._schema['columns']
                partition_columns[self._schema['partition_column']] = pl.String
                df = pl.DataFrame(schema=partition_columns)

            else:
                df = pl.DataFrame(schema=self._schema['columns'])

        else:
            if file_format == "csv":
                df =  self._read_csv()
            elif file_format == "parquet":
                df = self._read_parquet()
            elif file_format == "json":
                df = self._read_json()
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        
        if self._history == 'recent' and 'to_date' in df.columns:
            df = df.filter(pl.col('to_date') == pl.date(2099,12,31))

        # Replace empty values with None/null
        if self._schema['container'] == 'landingzone':
            replacement_dict = {'NA': None, 'nan': None}
            df = df.with_columns(pl.col(pl.String).replace(replacement_dict))

        df = clean_column_names(df)

        if self._schema['container'] not in ['landingzone', 'monitoring']:
            df = create_map_column(df, '_'.join(
                [self._schema['location'], self._schema['container']]))

        return df

    def _read_csv(self) -> pl.DataFrame:
        """
        Read data from CSV files.
        
        Returns:
            pl.DataFrame: DataFrame containing the CSV data
        """
        
        try:
            df = pl.read_csv(
                self._data_path,
                separator=",",
                has_header=True,
                schema=self._schema['columns'],
            )
            
            return df
            
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}") from e

    def _read_parquet(self) -> pl.DataFrame:
        """
        Read data from Parquet files.
        
        Returns:
            pl.DataFrame: DataFrame containing the Parquet data
        """
        
        path = Path(self._data_path)
        try:
            if self._schema['partition_column']:
                partition_columns = self._schema['columns']
                partition_columns[self._schema['partition_column']] = pl.String
                # Read and concatenate all parquet files
                df = pl.read_parquet(path, schema=partition_columns)
                if self._partition_name != 'all':
                    df = df.filter(pl.col(self._schema['partition_column'])==self._partition_name)
            else:
                # Read single parquet file
                df = pl.read_parquet(path, schema=self._schema['columns'])
            return df
            
        except Exception as e:
            raise ValueError(f"Error reading Parquet file: {str(e)}") from e
        
    def _read_json(self) -> pl.DataFrame:
        """
        Read data from JSON files.
        
        Returns:
            pl.DataFrame: DataFrame containing the JSON data
        """
        
        try:
            if self._data_path.endswith('.json'):
                df = pl.read_ndjson(
                    self._data_path,
                    schema=self._schema['columns'],
                )
                return df
            else:
                # Read and concatenate all JSON files in the directory
                all_data = []
                for json_file in Path(self._data_path).glob('*.json'):
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    data_path = self._schema.get('json_data_path', '')

                    if data_path:
                        json_data = parse(data_path).find(data)
                        for result in json_data:
                            if isinstance(result.value, list):
                                for subresult in result.value:
                                    sub_dict = {"file_name": json_file.name}
                                    for col_value in self._schema['columns'].keys():
                                        sub_dict[col_value] = subresult.get(col_value, None)
                                    all_data.append(sub_dict)
                            else:
                                sub_dict = {"file_name": json_file.name}
                                for col_value in self._schema['columns'].keys():
                                    sub_dict[col_value] = result.value.get(col_value, None)
                                all_data.append(sub_dict)
                    else:
                        all_data.append(data)
                schema_cols = self._schema['columns']
                schema_cols['file_name'] = pl.String
                df = pl.DataFrame(all_data, schema=schema_cols)
            
            return df
            
        except Exception as e:
            raise ValueError(f"Error reading JSON file: {str(e)}") from e

