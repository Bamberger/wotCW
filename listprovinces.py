## THIS IS A HACKJOB, JUST SPITS SOME DETAILS OF PROVINCES
import requests
import json
import pymongo
import time
import configparser
import argparse
from concurrent.futures import ThreadPoolExecutor

default_region = "region_sea"
default_mode = "update"
conf_file = "config.ini"
_type = "province"

## Class to hold Methods
class wotdata:
    def updateprovince(province):
        province_info_params = {'alias' : province}
        province_info = requests.get(url=uri_province_info, params=province_info_params).json() # Get province info from uri_province_info
        province_info['_type'] = _type
        if 'Province not found' not in str(province_info): # If we get a response and the province is used on this server
            db_coll.replace_one({'province.alias': province,'_type': _type},province_info, upsert=True) # Upsert the response
            # print(str(time.time()) + ' Completed: ' + province)
            # del province_info['province']['neighbours']
            # try:
            #     del province_info['province']['owner']
            # except KeyError:
            #     pass
            # del province_info['province']['bonuses']
            # print(province_info['province'])
            line = {}
            line['is_battle_offset'] = str(province_info['province']['is_battle_offset'])
            line['arena_name'] = str(province_info['province']['arena_name'])
            line['primetime'] = str(province_info['province']['primetime'])
            line['front_id'] = str(province_info['province']['front_id'])
            line['periphery'] = str(province_info['province']['periphery'])
            line['name'] = str(province_info['province']['name'])
            print(str(line))
            # print(province_info)

        else:
            print(str(time.time()) + ' Skipped: ' + province)

## Parse arguments
args_parser = argparse.ArgumentParser()
args_parser.add_argument("-r","--region", help="Region to run against: sea, na, ru or eu (Default is sea)")
args_parser.add_argument("-m","--mode", help="Mode to run in: initial or update (Default is update)")
args = args_parser.parse_args()
if not args.region: # If region is not provided, use default
    region = default_region
elif args.region not in ["sea", "na", "ru", "eu"]: # If region is provided but invalid, error and exit
    print("Region not correct, please use: sea, na, ru or eu")
    exit()
else: # Else we are good
    region = "region_" + args.region
if not args.mode: # If mode is not provided, use default
    mode = default_mode
elif args.mode not in ["initial", "update"]: # If mode is provided but invalid, error and exit
    print("Mode not correct, please use: initial or update")
    exit()
else: # Else we are good
    mode = args.mode

## Parse config file
conf_parser = configparser.ConfigParser() # Init ConfigParser
conf_parser.read(conf_file) # Parse the config file
uri_province_list = conf_parser.get(region, 'uri_province_list') # Get province list API from config
uri_province_info = conf_parser.get(region, 'uri_province_info') # Get province info API from config
conf_db_uri = conf_parser.get('global', 'db_uri') # Get Mongo uri from config
conf_db_db = conf_parser.get('global', 'db') # Get DB from config
conf_db_coll = conf_parser.get(region, 'db_coll') # Get collection from config
thread_count = int(conf_parser.get('global', 'thread_count')) #Get thread count from config

## Start real work
print(str(time.time()) + ' STARTED, Region: ' + region + ' Mode: ' + mode)
client = pymongo.MongoClient(conf_db_uri) # Connect to DB
db = client[conf_db_db]   # Set DB
db_coll = db[conf_db_coll] # Set collection
executor = ThreadPoolExecutor(max_workers=thread_count)

if mode == "initial":
    province_list = requests.get(url=uri_province_list).json() # Get province list from uri_province_list
    for element in province_list['locale_data']['messages']: # For each province in list
        # wotdata.newprovince(element) # Call newprovince
        province_initial = (province_list['locale_data']['messages'][element])
        if None in province_initial: # Test to ensure we have the right part of the API resposne, this is crude but works
            province = element.lstrip("province_") # Clean up the province alias
            executor.submit(wotdata.updateprovince, (province)) # Call updateprovince
elif mode == "update":
    for province in db_coll.distinct("province.neighbours.alias"): # Query DB for distinct province neighbours
        executor.submit(wotdata.updateprovince, (province)) # Call updateprovince
