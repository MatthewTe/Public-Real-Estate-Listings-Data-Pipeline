# Online Real Estate Listings Data ETL Pipeline 
This is a data pipeline designed to facilitate the creation and maintanence of a database of publicly listed Real Estate listings from various online RE listings websites. Currently the list of supported website data models is as follows:

- [Kijiji.ca](https://github.com/MatthewTe/ETL-Data-Models)

The Pipeline is written mainly in python with packages such as MySQLdb being used to connect to a MySQL server and execute SQL commands.

The three main processes of this ETL pipeline are described below:
- ### [Extract](https://github.com/MatthewTe/Public-Real-Estate-Listings-Data-Pipeline/blob/master/README.md#extract-1)
- ### [Transform](https://github.com/MatthewTe/Public-Real-Estate-Listings-Data-Pipeline/blob/master/README.md#transform-1)
- ### Load

## Extract
The extraction of raw RE listings data is done via the use of various web based data models that scrape their respective websites for listings data. A description of these data models can be found [here](https://github.com/MatthewTe/ETL-Data-Models). 

Once data is scraped from the web, it is then input into a [MySQL database python object](https://github.com/MatthewTe/Public-Real-Estate-Listings-Data-Pipeline/blob/master/RE_Listings_Pipeline/data_extraction/MySQL_database_connector.py) as a pandas dataframe. This dataframe is then processed to ensure that it only contains data that is unique to the SQL database and read into the appropriate MySQL table. The execution processes of collecting data from the web and storing it in an SQL table is as follows:

```python

import MySQLdb
import pandas as pd
from web_data_models import Kijiji
from MySQL_database_connector import Real_Estate_Listingsdb

# Extracting raw data from the web via the Kijiji data model:
Data = Kijiji(init_url, num_pages).data

# Accessing MySQL database: 
Data_table Real_Estate_Listingsdb(host, user, psswrd, db_name, table_name)

# Writing raw web data to the specified MySQL table:
Data_table.update_Kijiji(Data)
```
The raw data is stored in the SQL database in the following schema:

| Address| Price | Date           | Bedrooms        | Bathrooms      | Size           |
| -------| ----- | -------------- |-----------------|----------------|----------------|
| Text   | Text  |       Text     |      Text       | Text           | Text           |

## Transform
Raw data stored in the SQL database extracted by the web data models is read from the database using the [MySQL database python object](https://github.com/MatthewTe/Public-Real-Estate-Listings-Data-Pipeline/blob/master/RE_Listings_Pipeline/data_extraction/MySQL_database_connector.py) and input into the data transformation module. The main purpose of this data transformation is mainly standardization and geoprocessing. Using the geopy package a geoprocessor is used to correctly format the address data.

The Geoprocessor, in this case powered by the OpenCage geolocator's api then converts the address to a tuple of (lattitude, longnitude). This data is then stored as a tuple in an additional column. The finaly data transformation converts data types away from strings to the following schema:

|Address|Price|Date|Bedrooms|Bathrooms|Size |
|-------|-----|----|--------|---------|-----|
|Text   |Float|Date|Float   |Float    |Float|

The execution of extracting raw data from the SQL database and transforming it is as follows:
```python

# Importing database connector from data_extraction_pkg:
from RE_Listings_Pipeline.data_extraction_pkg.mySQL_database_connector import \
Real_Estate_Listingsdb as MySQLdb
from RE_Listings_Pipeline.data_extraction_pkg.web_data_models import Kijiji
from geoprocessor import data_transform

# Connecting to the raw data table:
raw_datatbl = MySQLdb(host, user, pswrd, db_name, table_name)

# Extracting raw data from tbl:
raw_data = raw_datatbl.read_data(num_rows)


# Feeding raw data into data transformation package:
geoprocessor = data_transform(raw_data, api_key) # The API key for the OpenCage geoprocessor API.

# Extracting the transformed data:
transformed_data = geoprocessor.data


# Writing transformed data into new SQL table:
transform_tbl = MySQLdb(host, user, pswrd, db_name, table_name) # Creating new connection to db to a new table.

transform_tbl.update_transformtbl(transformed_data)
```





