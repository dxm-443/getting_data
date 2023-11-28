#!/usr/bin/env python
# coding: utf-8

# PreInfo
# Geocoding/Inverse Geocoding API Documentation:
# https://lbs.amap.com/api/webservice/guide/api/georegeo
# poi_code information:
# 160101 The People's Bank of China | 160102 China Development Bank | 160103 Export-Import Bank of China
# 160104 Bank of China | 160105 Industrial and Commercial Bank of China | 160106 China Construction Bank
# 160107 Agricultural Bank of China | 160108 Bank of Communications

import requests
import logging
import pandas as pd

app_id = "xm_geo_decode"
app_key = "265515e4315e52ccbfd6a85e82113e34"

# Open the file containing the code and latitude/longitude of some listed companies
addr_file = pd.read_csv("/Users/mac/Desktop/firmLngLatData.csv")

# Process data to meet the requirements of Gaode api
addr_file.Lng, addr_file.Lat = round(addr_file.Lng, 6), round(addr_file.Lat, 6)


def data_req(url, params, method):
    """
    Returns:
    rsp: a response object
    """
    try:
        rsp = method(url, params=params)
        return rsp
    except:
        logging.error('Failed to get the data of %s status_code:%s reason:%s', params['location'],
                      rsp.status_code, rsp.reason, exc_info=True)


def parse_data(json_data):
    """
    Parse province, city, district, formatted_address, Lng, Lat, banks' num, banks' detailed data near the address
    
    Returns:
    data_dict: a dictionary including the infomation of interest
    """
    try:
        province = json_data['regeocode']['addressComponent']['province']
        city = json_data['regeocode']['addressComponent']['city']
        district = json_data['regeocode']['addressComponent']['district']
        formatted_address = json_data['regeocode']['formatted_address']
        Lng = params['location'].split(',')[0]
        Lat = params['location'].split(',')[1]
        bank_num = len(json_data['regeocode']['pois'])
        bank_data = parse_bank_data(json_data['regeocode']['pois'])
        data_dict = {"province": province, "city": city, "district": district, "formatted_address": formatted_address,
                     "Lng": Lng, "Lat": Lat, "bank_num": bank_num, "bank_data": bank_data}
        return data_dict
    except KeyError:
        logging.error('Failed to parse the data of %s', params['location'], exc_info=True)


def parse_bank_data(list_data):
    """
    Parse bank data near the address, including eight banks
    
    Returens:
    bank_dict: a dictionary including the numbers and detailed infomation of each bank
    """
    bank_dict = {'pbc': {'num': 0, 'extra': []}, 'cdb': {'num': 0, 'extra': []}, 'eib': {'num': 0, 'extra': []},
                 'bc': {'num': 0, 'extra': []}, 'icbc': {'num': 0, 'extra': []}, 'ccb': {'num': 0, 'extra': []},
                 'abc': {'num': 0, 'extra': []}, 'bcomu': {'num': 0, 'extra': []}}
    for item in list_data:
        tmp_type = item['type'].split(';')[2]
        if tmp_type == '中国人民银行':
            bank_dict['pbc']['num'] += 1
            bank_dict['pbc']['extra'].append(item)
        elif tmp_type == '国家开发银行':
            bank_dict['cdb']['num'] += 1
            bank_dict['cdb']['extra'].append(item)
        elif tmp_type == '中国进出口银行':
            bank_dict['eib']['num'] += 1
            bank_dict['eib']['extra'].append(item)
        elif tmp_type == '中国银行':
            bank_dict['bc']['num'] += 1
            bank_dict['bc']['extra'].append(item)
        elif tmp_type == '中国工商银行':
            bank_dict['icbc']['num'] += 1
            bank_dict['icbc']['extra'].append(item)
        elif tmp_type == '中国建设银行':
            bank_dict['ccb']['num'] += 1
            bank_dict['ccb']['extra'].append(item)
        elif tmp_type == '中国农业银行':
            bank_dict['abc']['num'] += 1
            bank_dict['abc']['extra'].append(item)
        elif tmp_type == '交通银行':
            bank_dict['bcomu']['num'] += 1
            bank_dict['bcomu']['extra'].append(item)
    return bank_dict


def save_data(df, data):
    """
    Save data as a dataframe
    
    Returns:
    result: a DataFrame contains data of interest
    """
    pd_data = pd.DataFrame()
    try:
        for key in data.keys():
            if key != 'bank_data':
                pd_data.loc[0, key] = f"{data.get(key)}"
            else:
                for sub_key_1 in data.get(key).keys():
                    for sub_key_2 in data.get(key).get(sub_key_1).keys():
                        col_name = f'{sub_key_1}_{sub_key_2}'
                        pd_data.loc[0, col_name] = f"{data.get(key).get(sub_key_1).get(sub_key_2)}"
    except:
        logging.error('Failed to add data of %s', params['location'], exc_info=True)
    result = pd.concat([df, pd_data])
    return result


if __name__ == '__main__':
    # Set the logging format
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

    # Reverse geocoding, parse and save data
    url = 'https://restapi.amap.com/v3/geocode/regeo?parameters'

    PoiDataInfo = pd.DataFrame()
    for i in range(0, len(addr_file)):
        logging.info(f"Getting the data of firm {addr_file.loc[i, 'code']}")
        params = {
            "key": "265515e4315e52ccbfd6a85e82113e34",
            "location": f"{addr_file.loc[i, 'Lng']},{addr_file.loc[i, 'Lat']}",
            "poitype": "160101|160102|160103|160104|160105|160106|160107|160108",
            "radius": "3000",
            "extensions": "all"
        }
        geodata = data_req(url, params=params, method=requests.get)
        logging.info(f"Parsing the data of firm {addr_file.loc[i, 'code']}")
        lc_data = parse_data(geodata.json())
        logging.info(f"Saving the data of firm {addr_file.loc[i, 'code']}")
        PoiDataInfo = save_data(PoiDataInfo, lc_data)
    logging.info('Finished getting and saving data!')

    # Merge company code data and parsing reverse geocoding data
    PoiDataInfo['Lng'], PoiDataInfo['Lat'] = PoiDataInfo['Lng'].astype('float64'), PoiDataInfo['Lat'].astype('float64')
    res_data = pd.merge(addr_file, PoiDataInfo, on=['Lng', 'Lat'])

    # Set the dataframe display format
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
