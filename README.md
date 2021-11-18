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
## Schema
A **Star Schema** will be created to optimize song play analysis utilizing `SONG_DATA` and `LOG_DATA`.
![SparkifyERD](/images/Sparkify-ERD.png)
> Sparkify Entity Relationship Diagram


