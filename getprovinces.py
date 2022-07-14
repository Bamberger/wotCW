from pickle import FALSE, TRUE
import requests
import json
import pymongo
import time
import datetime
import configparser
import argparse
import csv
import os
from concurrent.futures import ThreadPoolExecutor

default_region = "region_sea"
default_mode = "update"
conf_file = "config.ini"
_type = "province"
csv_header = ["timestamp", "alias", "turns_till_primetime",
              "battles_running", "attackers_count", "owner"]

# Class to hold Methods


class wotdata:
    def updateprovince(province):
        province_info_params = {'alias': province}
        # Get province info from uri_province_info
        province_info = requests.get(
            url=uri_province_info, params=province_info_params).json()
        province_info['_type'] = _type
        # If we get a response and the province is used on this server
        if 'Province not found' not in str(province_info):
            db_coll.replace_one({'province.alias': province, '_type': _type},
                                province_info, upsert=True)  # Upsert the response
            if csv_mode == TRUE:
                with open(csv_file, 'a', encoding='UTF8', newline='') as file:
                    writer = csv.writer(file)
                # Write to CSV using writer
                    if province_info['owner'] == None:
                        owner_tag = ''
                    else:
                        owner_tag = province_info['owner']['tag']
                    writer.writerow(datetime.now(), [province, province_info['province']['turns_till_primetime'], province_info['province']['battles_running'],
                                    province_info['province']['attackers_count'], owner_tag])

                    # province_info['province']['owner']['tag']
            print(str(time.time()) + ' Completed: ' + province)

        else:
            print(str(time.time()) + ' Skipped: ' + province)


# Parse arguments
args_parser = argparse.ArgumentParser()
args_parser.add_argument(
    "-r", "--region", help="Region to run against: sea, na, ru or eu (Default is sea)")
args_parser.add_argument(
    "-m", "--mode", help="Mode to run in: initial or update (Default is update)")
args_parser.add_argument(
    "-c", "--csv", help="Specifies a CSV output file to write to")
args = args_parser.parse_args()
if not args.region:  # If region is not provided, use default
    region = default_region
# If region is provided but invalid, error and exit
elif args.region not in ["sea", "na", "ru", "eu"]:
    print("Region not correct, please use: sea, na, ru or eu")
    exit()
else:  # Else we are good
    region = "region_" + args.region
if not args.mode:  # If mode is not provided, use default
    mode = default_mode
# If mode is provided but invalid, error and exit
elif args.mode not in ["initial", "update"]:
    print("Mode not correct, please use: initial or update")
    exit()
else:  # Else we are good
    mode = args.mode
if not args.csv:  # If csv is not provided, use default
    csv_mode = FALSE
    print("Not in CSV mode")
else:
    csv_mode = TRUE
    csv_file = './csv/'+args.csv
    print(csv_file)
    if os.path.exists(csv_file):
        os.remove(csv_file)
    with open(csv_file, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(csv_header)


# Parse config file
conf_parser = configparser.ConfigParser()  # Init ConfigParser
conf_parser.read(conf_file)  # Parse the config file
# Get province list API from config
uri_province_list = conf_parser.get(region, 'uri_province_list')
# Get province info API from config
uri_province_info = conf_parser.get(region, 'uri_province_info')
conf_db_uri = conf_parser.get('global', 'db_uri')  # Get Mongo uri from config
conf_db_db = conf_parser.get('global', 'db')  # Get DB from config
conf_db_coll = conf_parser.get(region, 'db_coll')  # Get collection from config
thread_count = int(conf_parser.get('global', 'thread_count')
                   )  # Get thread count from config

# Start real work
print(str(time.time()) + ' STARTED, Region: ' + region + ' Mode: ' + mode)
client = pymongo.MongoClient(conf_db_uri)  # Connect to DB
db = client[conf_db_db]   # Set DB
db_coll = db[conf_db_coll]  # Set collection
executor = ThreadPoolExecutor(max_workers=thread_count)

if mode == "initial":
    # Get province list from uri_province_list
    province_list = requests.get(url=uri_province_list).json()
    # For each province in list
    for element in province_list['locale_data']['messages']:
        # wotdata.newprovince(element) # Call newprovince
        province_initial = (province_list['locale_data']['messages'][element])
        if None in province_initial:  # Test to ensure we have the right part of the API resposne, this is crude but works
            # Clean up the province alias
            province = element.lstrip("province_")
            # Call updateprovince
            executor.submit(wotdata.updateprovince, (province))
elif mode == "update":
    # Query DB for distinct province neighbours
    for province in db_coll.distinct("province.neighbours.alias"):
        # Call updateprovince
        executor.submit(wotdata.updateprovince, (province))
