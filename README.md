# Project: Build a AWS Data Warehouse 

***

#### Project Overview

As Sparkify's Data Engineer, I am tasked with building an ETL pipeline that extracts data from our S3, then stage the data in Redshift. Our data then needs to transform into a set of dimensional tables for the analytics team. The Analytics team will use the dimensional tables to find insights in what songs users are listening to. Once completed, the database and ETL pipeline can be tested by running queries given to me by the analytics team. Queried results will then be compared to the analytics team expected results.

#### Data Overview

There are 2 Datasets, `(SONG_DATA and lOG_DATA)`, and they're located in an S3 bucket. Combined these datasets represent users song choice in JSON format.

###### `SONG_DATA` dataset contents:
- *num_songs | artist_id |artist_latitude | artist_longitude | artist_location | artist_name | song_id, title | duration | year*

###### `LOG_DATA` dataset contents:
- *artist | auth | firstName | gender | itemInSession | lastName | length | level | location | method | page | registration | sessionId | song | status | ts | userAgent | userId*

***

### Project Details

##### Files in project:
- **create_cluster.py**: Creates Redshift Cluster.
- **create_tables.py**: ```Drops Tables, Creates Tables```
- **etl.py**: ```Loads Staging Tables, Insert Tables```
- **sql_queries.py**: Contains all necessary queries for _create_tables.py_ and _etl.py_

##### Running project:

In CLI run commands in this order: 
- `python create_cluster.py`
- `python create_tables.py`
- `python etl.py`
***

### Schema
**Star Schema** created to optimize song play analysis.
![SparkifyERD](/images/Sparkify-ERD.png)
> Sparkify Entity Relationship Diagram



