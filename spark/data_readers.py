import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from spark.dataframe_helpers import get_data_location, create_table_name, clean_column_names, create_map_column


class DataReader:
    """
    A class used to read data from different file types.

    ...

    Attributes
    ----------
    _spark_session : SparkSession
        a SparkSession object to perform Spark operations
    _env : str
        the environment where the data is located
    _schema : dict
        the schema of the data to be read
    _partition_name : str
        the name of the partition (default is None)
    _history : str
        the history of the data to be read (default is 'recent')
    _data_path : str
        the path to the data
    _table_name : str
        the name of the table

    Methods
    -------
    read_source() -> DataFrame:
        Determines which file type to read and how to read it.
    read_csv_file() -> DataFrame:
        Reads a CSV file and returns a DataFrame.
    read_multiline_file() -> DataFrame:
        Reads a multiline file and returns a DataFrame.
    read_parquet_file() -> DataFrame:
        Reads a Parquet file and returns a DataFrame.
    read_delta_table() -> DataFrame:
        Reads a Delta table and returns a DataFrame.
    """

    def __init__(self, spark_session: SparkSession, environment: str, schema: dict, partition_name: str = None, history: str = 'recent'):
        self._spark_session = spark_session
        self._env = environment
        self._schema = schema
        self._partition_name = partition_name
        self._history = history
        self._data_path = get_data_location(
            self._schema, self._partition_name)
        self._table_name = create_table_name(
            self._env, self._schema['container'], self._schema['location'])

    def read_source(self) -> DataFrame:
        """
        This is the logic to determine which file type to read and how to read it.
        """

        implemented_file_types = ['csv', 'csv_pipe_sep', 'parquet',
                                  'delta', 'multiline']

        if self._schema['type'] not in implemented_file_types:
            raise NotImplementedError(
                f"The file type {self._schema['type']} is not implemented yet.")

        if self._history not in ['recent', 'complete']:
            raise ValueError(
                f"{self._history} is not a valid argument for the history of a table, use 'recent' or 'complete'")

        implemented_read_functions = {
            'csv': self.read_csv_file,
            'csv_pipe_sep': self.read_csv_pipe_sep_file,
            'parquet': self.read_parquet_file,
            'delta':  self.read_delta_table,
            'multiline': self.read_multiline_file
        }

        try:
            df = implemented_read_functions[self._schema['type']]()
            df.head()  # Force to load first record of the data to check if it throws an error
        except Exception as e:
            if "Path does not exist:" in str(e):
                # If the table does not exist yet, return an empty data frame
                df = self._spark_session.createDataFrame(
                    [], self._schema['columns'])
            else:
                raise  # If we encounter any other error, raise as the error

        if self._partition_name != '':
            df = df.withColumn(
                self._schema['partition_column'], F.lit(self._partition_name))

        if self._history == 'recent' and 'to_date' in df.columns:
            df = df.filter(F.col('to_date') == '2099-12-31')

        # Replace empty values with None/null
        if self._schema['container'] == 'landingzone':
            replacement_dict = {'NA': None, 'nan': None}
            df = df.replace(replacement_dict, subset=df.columns)

        df = clean_column_names(df)

        if self._schema['container'] not in ['landingzone', 'monitoring']:
            df = create_map_column(df, '_'.join(
                [self._schema['location'], self._schema['container']]))

        return df

    def read_csv_file(self):

        df = self._spark_session.read.format('csv').schema(self._schema['columns']).option('header', True).option(
            "quote", '"').option("multiline", 'True').load(self._data_path)

        return df
    
    def read_csv_pipe_sep_file(self):

        df = self._spark_session.read.format('csv').schema(self._schema['columns']).option(
            'header', True).option("quote", '"').option('delimiter', '|').option("multiline", 'True').load(self._data_path)
        
        return df

    def read_multiline_file(self):

        df = self._spark_session.read.format('csv').schema(self._schema['columns']).option(
            'header', True).option("quote", '~').option('delimiter', ';').option("multiline", 'True').load(self._data_path)

        return df

    def read_parquet_file(self):

        df = self._spark_session.read.format('parquet').schema(
            self._schema['columns']).option('header', True).load(self._data_path)

        return df

    def read_delta_table(self):

        df = self._spark_session.read.format(
            'delta').load(self._data_path)

        if self._partition_name:
            df = df.filter(
                F.col(self._schema['partition_column']) == self._partition_name)

        return df
