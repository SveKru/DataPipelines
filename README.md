# DataPipelines
A repo to create local data pipelines to process and structure data.

## Content
- Processing of data based on the Polars python package.
- Storage of data in a folder structure that is comparable to different layers in a database.
- Enforced schema of the tables in the different layers of the tables.
- Data quality checks that are run on every run of a table
- Slowly changing dimensions for all tables by default
- Record tracing, which mean that you can follow any record passing through the database from start to finish
- Testing of code and functionalities
- Deprecated pyspark processing, based on earlier versions