import json
from eurostatapiclient import EurostatAPIClient
import math
import pandas as pd
import numpy as np

# Choose service version : only v2.1 is currently available
VERSION = 'v2.1'

# Only json is currently available
FORMAT = 'json'

# Specify language : en, fr, de
LANGUAGE = 'en'

client = EurostatAPIClient(VERSION, FORMAT, LANGUAGE)

# Optionnal : working behing a proxy :
# client.set_proxy({'http':'my.proxy.com/8080', 'https':'my.proxy.com/8080'})

# Add some filters (only mono-filtering is available for now)
# params = [
#    ('geo', 'BE'),
#    ('geo', 'IT'),
# ]

# Note that some keys may be repeated in eurostat's api
# In that case, you will want to pass params as a list of tuples
# ex. :
# params = [
#  ('siec', 'TOTAL'),
#  ('precision', '1'),
#  ('unit', 'KTOE'),
#  ('nrg_bal', 'AFC'),
#  ('nrg_bal', 'DL'),
#  ('nrg_bal', 'EXP'),
#  ('nrg_bal', 'FC_E'),
#  ('nrg_bal', 'FEC2020-2030')]
# filtered_dataset = client.get_dataset('nrg_bal_c', params=params)

pop_ds = client.get_dataset('demo_gind')
pop_df = pop_ds.to_dataframe()
pop_df = pop_df[(pop_df.indic_de == 'JAN')]  # filter by indicator and time

dea_ds = client.get_dataset('demo_r_mwk_ts')
dea_df = dea_ds.to_dataframe()
dea_df = dea_df[(dea_df['sex'] == 'T') & (~dea_df['time'].str.contains('W99')) & (
    dea_df['geo'] != 'AD')]  # filter by sex, remove 'week unknown, remove andorra'
del dea_df['sex']
del dea_df['unit']
dea_df = dea_df.rename(columns={'values': 'deaths'})
dea_df = dea_df.reset_index()

dea_yea_ds = client.get_dataset('demo_mmonth')
dea_yea_df = dea_yea_ds.to_dataframe()
dea_yea_df = dea_yea_df[dea_yea_df['month'].isin(['M01', 'M02', 'M03', 'M04', 'M05'])]
dea_yea_df = dea_yea_df.groupby(['geo', 'time'])['values'].agg('sum', min_count=5).reset_index()
dea_yea_df = dea_yea_df.rename(columns={'values': 'deaths'})

country_set = set(dea_df.geo)


def generateJSON(df):

    def residents_fix(value, time, geo):
        geo_filtered = pop_df[pop_df['geo'] == geo]
        time_filtered = geo_filtered[geo_filtered['time'] == time[:4]]
        pop = time_filtered.iloc[0].values[0]
        if not pd.isna(value):
            deaths_per_million = round((value / pop) * 1000000, 2)
        else:
            deaths_per_million = np.nan
        print(time)
        print(geo)
        return deaths_per_million

    df['deaths_per_million'] = df.apply(lambda x: residents_fix(value=x['deaths'], time=x['time'], geo=x['geo']), axis=1)
    isWeekly = True if len(df.time[0]) != 4 else False

    path = 'country_data/weekly/' if isWeekly else 'country_data/yearly/'
    country_list = []

    with open('country_list_all.json') as f:
        country_list_all = json.load(f)

    for country_code in country_set:
        label = country_list_all[country_code]
        country_list.append({'label': label, 'value': country_code})
        country_data = df[df['geo'] == country_code]
        if not isWeekly:
            week_df = dea_df[dea_df['geo'] == country_code]
            deaths_2020 = week_df[(week_df['time'] >= '2020W01') & (week_df['time'] <= '2020W22')].sum().deaths
            pop_2020 = pop_df[(pop_df['geo'] == country_code) & (pop_df['time'] == '2020')].values[0][0]
            deaths_per_million_2020 = round((deaths_2020 / pop_2020) * 1000000, 2)
            country_data = country_data.append({'geo': country_code, 'time': '2020', 'deaths': deaths_2020, 'deaths_per_million': deaths_per_million_2020}, ignore_index=True)
        else:
            del country_data['index']

        del country_data['geo']
        country_data.loc[:, 'deaths_per_million_MA_5'] = country_data['deaths_per_million'].rolling(5).mean().round(decimals=2)
        country_data.loc[:, 'deaths_MA_5'] = country_data['deaths'].rolling(5).mean().round(decimals=2)
        country_data.loc[:, 'deaths_per_million_MA_10'] = country_data['deaths_per_million'].rolling(10).mean().round(decimals=2)
        country_data.loc[:, 'deaths_MA_10'] = country_data['deaths'].rolling(10).mean().round(decimals=2)

        country_data = country_data.fillna('null')
        country_data_dict = country_data.to_dict(orient='records')
        with open(path + country_code + '.json', 'w') as f:
            json.dump(country_data_dict, f, ensure_ascii=False, indent=4)

    with open('country_list.json', 'w') as f:
        json.dump(country_list, f, ensure_ascii=False, indent=4)


generateJSON(dea_df)
generateJSON(dea_yea_df)
