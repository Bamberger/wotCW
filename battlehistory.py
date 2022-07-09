import requests
import json
import pymongo
import time
import configparser
import argparse
from concurrent.futures import ThreadPoolExecutor

default_region = "region_sea"
conf_file = "config.ini"
_type = "battlehistory"

## Class to hold Methods
class wotdata:
    def battlehistory(province):
        province_history_params = {'date_from' : conf_date_from, 'date_to' : conf_date_to}
        uri_string = uri_province_history + province + '/battles_history'
        province_history = requests.get(url=uri_string, params=province_history_params).json() # Get province info from uri_province_info
        province_history['_type'] = _type
        db_coll.replace_one({'province.alias': province,'_type': _type},province_history, upsert=True) # Upsert the response
        print(str(time.time()) + ' Completed: ' + province)

## Parse arguments
args_parser = argparse.ArgumentParser()
args_parser.add_argument("-r","--region", help="Region to run against: sea, na, ru or eu (Default is sea)")
args = args_parser.parse_args()
if not args.region: # If region is not provided, use default
    region = default_region
elif args.region not in ["sea", "na", "ru", "eu"]: # If region is provided but invalid, error and exit
    print("Region not correct, please use: sea, na, ru or eu")
    exit()
else: # Else we are good
    region = "region_" + args.region

## Parse config file
conf_parser = configparser.ConfigParser() # Init ConfigParser
conf_parser.read(conf_file) # Parse the config file
conf_db_uri = conf_parser.get('global', 'db_uri') # Get Mongo uri from config
conf_db_db = conf_parser.get('global', 'db') # Get DB from config
conf_db_coll = conf_parser.get(region, 'db_coll') # Get collection from config
uri_province_history = conf_parser.get(region, 'uri_province_history') # Get province history API from config
conf_date_from = conf_parser.get('global', 'date_from')
conf_date_to = conf_parser.get('global', 'date_to')
thread_count = int(conf_parser.get('global', 'thread_count')) #Get thread count from config

## Start real work
print(str(time.time()) + ' STARTED, Region: ' + region)
client = pymongo.MongoClient(conf_db_uri) # Connect to DB
db = client[conf_db_db]   # Set DB
db_coll = db[conf_db_coll] # Set collection
executor = ThreadPoolExecutor(max_workers=thread_count)

for province in db_coll.distinct("province.neighbours.alias"): # Query DB for distinct province neighbours
    executor.submit(wotdata.battlehistory, (province)) # Call battlehistory

print(str(time.time()) + ' DONE')