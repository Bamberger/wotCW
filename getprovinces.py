from pickle import FALSE, TRUE
import requests
import pymongo
import time
from datetime import datetime
import configparser
import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import json

# If modifying these scopes, delete the file token.json.
# scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
scopes = ['https://www.googleapis.com/auth/spreadsheets']

# print(datetime.now())

default_region = "region_sea"
default_mode = "update"
conf_file = "config.ini"
_type = "province"
header = ["alias", "timestamp", "turns_till_primetime",
          "battles_running", "attackers_count", "owner", "clan_opportunity", "max_attackers"]
sheet_data = []
sheet_data.append(header)
province_data = []

# Class to hold Methods


class wotdata:
    def postprocess(province_info, sheet_data):

        province_cleanup = {}
        for province_mod in province_data:
            province_n = province_mod['province']
            province_cleanup[str(province_n)] = province_mod

        # Establish owners for each neighbour
        for province_c in province_cleanup:
            owners = []
            for idx, neighbour_c in enumerate(province_cleanup[province_c]['neighbours']):
                p = province_cleanup[province_c]['neighbours'][idx]['province']
                province_cleanup[province_c]['neighbours'][idx]['owner'] = province_cleanup[p]['owner']
                if not province_cleanup[p]['owner'] == '':
                    if province_cleanup[p]['owner'] not in owners:
                        owners.append(province_cleanup[p]['owner'])
                if province_cleanup[province_c]['neighbours'][idx]['owner'] == conf_clan_tag:
                    province_cleanup[province_c]['type'] = "neighbour"

            # TODO: I think we can check if we own the province here and mark as 'defender'
            if province_cleanup[province_c]['owner'] == conf_clan_tag:
                province_cleanup[province_c]['type'] = "defender"

            # If the province owner is in the owners item and province is owned by the same clan, remove it
            if province_cleanup[province_c]['owner'] in owners:
                owners.remove(province_cleanup[province_c]['owner'])
            if "" in owners:
                owners.remove(conf_clan_tag)

            province_cleanup[province_c]['unique_neighbour_owners'] = len(
                owners)

        # print(json.dumps(province_cleanup))

        for idx1, sheet_data_row in enumerate(sheet_data):
            # print("sheet_data_row " + str(sheet_data_row))
            max_attackers_province = sheet_data[idx1][0]
            # Item #6 is landing/auction battles, this is a bad hack but it will work for now
            if idx1 > 0:
                sheet_data[idx1][6] = province_cleanup[max_attackers_province]['type']
            for idx2, sheet_data_row_item in enumerate(sheet_data_row):
                # Find neighbours for this province if we are up to the max attackers field
                if sheet_data_row_item == "MAX_ATTACKERS_HOLDING":
                    max_attackers = province_cleanup[max_attackers_province]['unique_neighbour_owners'] + \
                        province_cleanup[max_attackers_province]['max_applications_number']
                    sheet_data[idx1][idx2] = max_attackers
                # print(str(idx2) + " sheet_data_row_item " + str(sheet_data[idx1][idx2]))

        print("Finished updating")
        try:
            service = build('sheets', 'v4', credentials=creds)
            spreadsheet_row = spreadsheet_range_name

            values = sheet_data
            body = {
                'values': values
            }
            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=spreadsheet_row,
                valueInputOption="RAW", body=body).execute()
            print(str(time.time()) + " Post-processing complete, " +
                  f"{result.get('updatedCells')} cells updated on sheet: " + str(spreadsheet_row))
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error

    def updateprovince(province):
        # print(str(time.time()) + ' Started: ' + province)
        province_info_params = {'alias': province}
        # Get province info from uri_province_info
        province_info = requests.get(
            url=uri_province_info, params=province_info_params).json()
        province_info['_type'] = _type
        province_info['timestamp'] = time.time()
        # If we get a response and the province is used on this server
        if 'Province not found' not in str(province_info):
            db_coll.replace_one({'province.alias': province, '_type': _type},
                                province_info, upsert=True)  # Upsert the response
            if province_info['owner']:
                owner_tag = province_info['owner']['tag']
            else:
                owner_tag = ''
            if province_info['province']['type']:
                type_tag = province_info['province']['type']
            else:
                type_tag = ''
            if province_info['province']['neighbours']:
                neighbours = []
                for neighbour in province_info['province']['neighbours']:
                    neighbour_data = {
                        'province': neighbour['alias'], 'owner': ""}
                    neighbours.append(neighbour_data)
            else:
                neighbours = ''

            free_applications = 0
            max_applications_number = 0
            try:
                max_applications_number = province_info['province']['max_applications_number']
            except:
                # TODO: should var this out - iron_age_sg_league3 = 8 iron_age_sg_league2 = 16 iron_age_sg_league1 = 32
                if province_info['province']['front_id'] == "iron_age_sg_league3" and type_tag == "auction":
                    max_applications_number = 8
                elif province_info['province']['front_id'] == "iron_age_sg_league2" and type_tag == "auction":
                    max_applications_number = 16
                else:
                    max_applications_number = 0
            if province_info['province']['free_applications']:
                free_applications = province_info['province']['free_applications']
                if province_info['province']['front_id'] == "iron_age_sg_league1":
                    free_applications = 0

            print(str(time.time()) + ' Completed: ' + province)

            if sheets_mode == TRUE:
                if mode == "update":
                    global p_count
                    global p_counter
                    opponents =  province_info['province']['attackers_count'] +  province_info['province']['free_bets_attackers_count']

                    sheet_data.append([province, province_info['timestamp'], province_info['province']['turns_till_primetime'], province_info['province']['battles_running'],
                                       opponents, owner_tag, type_tag, "MAX_ATTACKERS_HOLDING"])

                    record_data = {'province': province, 'owner': owner_tag, 'type': type_tag, 'neighbours': neighbours,
                                   'max_applications_number': max_applications_number, 'free_applications': free_applications}
                    province_data.append(record_data)

                    p_counter += 1
                    # At this point we have all provinces returned so we can do final processing, yes this is a bad hack
                    if p_counter == p_count:

                        print(str(time.time()) + ' Commencing post-processing')
                        wotdata.postprocess(province_data, sheet_data)

        else:
            print(str(time.time()) + ' Skipped: ' + province)


# Parse arguments
args_parser = argparse.ArgumentParser()
args_parser.add_argument(
    "-r", "--region", help="Region to run against: sea, na, ru or eu (Default is sea)")
args_parser.add_argument(
    "-m", "--mode", help="Mode to run in: initial or update (Default is update)")
args_parser.add_argument(
    "-s", "--sheets", action='store_true', help="Runs in Google Sheets mode, used for outputting province info to Google Sheets")
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
if not args.sheets:  # If sheets is not provided, use default
    sheets_mode = FALSE
else:
    sheets_mode = TRUE
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', scopes)
            creds = flow.run_local_server(port=0)
        #     creds = service_account.Credentials.from_service_account_file(
        # 'credentials.json', scopes=scopes)
        print(creds)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())


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
# The ID and range of a sample spreadsheet.
spreadsheet_id = conf_parser.get('global', 'spreadsheet_id')
spreadsheet_range_name = conf_parser.get('global', 'spreadsheet_range_name')
conf_battles_api = conf_parser.get('global', 'battles_api')
conf_battles_spreadsheet_range_name = conf_parser.get(
    'global', 'battles_spreadsheet_range_name')
conf_clan_tag = conf_parser.get('global', 'clan_tag')

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
            # wotdata.updateprovince, (province)
elif mode == "update":
    # Query DB for distinct province neighbours
    province_list_alias = db_coll.distinct("province.neighbours.alias")
    global p_count
    global p_counter
    p_count = len(province_list_alias)
    p_counter = 0
    for province in province_list_alias:
        # Call updateprovince
        executor.submit(wotdata.updateprovince, (province))
        # wotdata.updateprovince, (province)
if sheets_mode == TRUE:
    battles_list = requests.get(url=conf_battles_api).json()
    # print(battles_list)
    battles_header = ["province_id", "timestamp", "battle_time",
                      "turns_till_primetime", "battles_running", "attackers_count", "owner"]
    battles_sheet_data = []
    battles_sheet_data.append(battles_header)
    for battle in battles_list["planned_battles"]:
        # print(battle)
        battles_row = [
            battle["province_id"],
            time.time(),
            battle["battle_time"],
            battle["province_type"],
            battle["attack_type"],
            battle["is_attacker"]
        ]
        battles_sheet_data.append(battles_row)
        # print("DATA:")
        # print(battles_row)
    # print(battles_sheet_data)
    try:
        battles_service = build('sheets', 'v4', credentials=creds)

        # # Call the Sheets API
        body = {
            'values': battles_sheet_data
        }
        clear_values_request_body = {
            # TODO: Add desired entries to the request body.
        }

        clear_result = battles_service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,
                                                                     range=conf_battles_spreadsheet_range_name, body=clear_values_request_body).execute()

        result = battles_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=conf_battles_spreadsheet_range_name,
            valueInputOption="RAW", body=body).execute()
        print(f"{result.get('updatedCells')} cells updated.")
        # return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        # return error
