# Online Real Estate Listings Data ETL Pipeline 
This is a data pipeline designed to facilitate the creation and maintanence of a database of publicly listed Real Estate listings from various online RE listings websites. Currently the list of supported website data models is as follows:

- [Kijiji.ca](https://github.com/MatthewTe/ETL-Data-Models)

The Pipeline is written mainly in python with packages such as MySQLdb being used to connect to a MySQL server and execute SQL commands.

The three main processes of this ETL pipeline are described below:
- ### [Extract](https://github.com/MatthewTe/Public-Real-Estate-Listings-Data-Pipeline/blob/master/README.md#extract-1)
- ### Transform
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
The raw data is stored in the sql database in the following schema:

| Address| Price | Date           | Bedrooms        | Bathrooms      | Size           |
| -------| ----- | -------------- |-----------------|----------------|----------------|
| Text   | Text  |       Text     |      Text       | Text           | Text           |
