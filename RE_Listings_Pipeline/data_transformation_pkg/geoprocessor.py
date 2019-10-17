# Path hack:
import sys
sys.path.append("..")

# Importing database connector from data_extraction_pkg:
from RE_Listings_Pipeline.data_extraction_pkg.mySQL_database_connector import \
Real_Estate_Listingsdb as MySQL_db
