import polars as pl
from pathlib import Path
from typing import Dict, Optional

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

        if self._schema['partition_column'] and not self._partition_name:
            raise ValueError('A partitioned dataframe is being read without a specified partition')

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
                df = pl.DataFrame([[] for _ in partition_columns],schema=partition_columns)

            else:
                df = pl.DataFrame([[] for _ in self._schema['columns']],schema=self._schema['columns'])

        else:
            if file_format == "csv":
                df =  self._read_csv()
            elif file_format == "parquet":
                df = self._read_parquet()
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

