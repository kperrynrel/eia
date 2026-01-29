"""
Pull the EIA 923 data. This is monthly production data for all
generators in the United States.
"""


from bs4 import BeautifulSoup as bs
import requests, zipfile, io
import re
import glob
import pandas as pd
from collections import Counter
import datetime
import logging
import os

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='app.log', 
                    force=True,
                    filemode='a')

URL = "https://www.eia.gov/electricity/data/eia923/"

energy_code_df = pd.read_csv("eia_energy_code_key.csv")
energy_code_dict = dict(zip(energy_code_df['Energy Source Code'],
                            energy_code_df['Energy Source Description']))

prime_mover_df = pd.read_csv("Prime_Mover_Codes.csv")
prime_mover_dict = dict(zip(prime_mover_df['Prime Mover Code'],
                            prime_mover_df['Prime Mover Description']))

mer_code_df = pd.read_csv("eia_mer_fuel_type_codes.csv")
mer_code_dict = dict(zip(mer_code_df['fuel_code'],
                         mer_code_df['description']))


def filter_latest_year_data(df):
    # Delete all data from the latest year that is not from the most recent
    # data load (as this data can change before being finally recorded)
    df_copy = df.copy()
    df_copy['YEAR'] = pd.to_numeric(df_copy['YEAR'], errors='coerce')
    max_year_files = list(df_copy[df_copy['YEAR']== df_copy['YEAR'].max()
                             ]['file'].drop_duplicates())
    file_match_dict = dict()
    year = str(int(df_copy['YEAR'].max()))
    for file in max_year_files:
        match = re.search(f'M_(\d+)_{year}', file)
        file_match_dict[file] = int(match.group(1))
    # get the latest file in the sequence, and build list of file data
    # we want to remove
    max_value = max(file_match_dict.values())
    removal_files = [key for key, value in file_match_dict.items()
                     if value != max_value]
    return removal_files

def process_master_plant_data(df,
                              joiner_columns,
                              metadata_columns,
                              removal_columns,
                              data_type):
    plants = list(df["PLANT ID"].drop_duplicates())
    meta_df = df[metadata_columns].drop_duplicates()
    meta_df.to_csv("./923_metadata/" + data_type + "_923_metadata.csv", 
                   index=False)
    time_series_columns = list(Counter(list(df.columns)) - 
                               Counter(metadata_columns) + 
                               Counter(joiner_columns) - 
                               Counter(removal_columns))
    # Now remove all of the metadata columns with the exception of the joiner columns
    time_series_df = df[time_series_columns]
    # Generate data for each plant
    for plant_id in plants:
        plant_df = time_series_df[time_series_df['PLANT ID'] == plant_id]
        if len(plant_df) > 0:
            plant_df = pd.melt(plant_df,
                               id_vars=joiner_columns,
                               value_vars= list(Counter(plant_df.columns) -
                                              Counter(joiner_columns)))
            plant_df['MONTH'] = [x.split(" ")[-1] for x in 
                                 list(plant_df['variable'])]
            plant_df = plant_df[plant_df['YEAR']!='.']
            plant_df = plant_df[plant_df['MONTH']!='.']
            plant_df['measured_on'] = pd.to_datetime(
                plant_df["MONTH"] + " 1, " + plant_df["YEAR"].astype(str),
                errors='coerce')
            plant_df = plant_df.sort_values(by='measured_on')
            # Remove NaN values or "." values
            plant_df = plant_df[~plant_df['REPORTED FUEL TYPE CODE'].isin(["   "])]
            plant_df = plant_df[plant_df['value']!='.']
            plant_df = plant_df[~plant_df['value'].isna()]
            # Clean up Nuclear ID column, as that can delineate individual metrics
            plant_df.loc[(plant_df['NUCLEAR UNIT ID'] == '.') |
                         (plant_df['NUCLEAR UNIT ID'].isna()), 'NUCLEAR UNIT ID'] = ""
            plant_df['NUCLEAR UNIT ID'] = plant_df['NUCLEAR UNIT ID'].astype(str)
            if len(plant_df) > 0:
                plant_df['energy_type'] = [energy_code_dict[x] if
                                           x in energy_code_dict.keys() else None
                                           for x in 
                                           list(plant_df['REPORTED FUEL TYPE CODE'])]
                plant_df['prime_mover_type'] = [prime_mover_dict[x] if
                                           x in prime_mover_dict.keys() else None
                                           for x in 
                                           list(plant_df['REPORTED PRIME MOVER'])]
                plant_df['mer_type'] = [mer_code_dict[x] if
                                           x in mer_code_dict.keys() else None
                                           for x in 
                                           list(plant_df['MER FUEL TYPE CODE'])]
                # Get the associated sensor_name
                plant_df.loc[plant_df['variable'].str.contains("NETGEN"),
                             "common_name"] = 'generation'
                plant_df.loc[plant_df['variable'].str.contains("GROSSGEN"),
                             "common_name"] = 'gross-generation'
                plant_df.loc[plant_df['variable'].str.contains("ELEC MMBTU"),
                             "common_name"] = 'quantity-consumed-electricity'                
                plant_df.loc[plant_df['variable'].str.contains("TOT MMBTU"),
                             "common_name"] = 'total-fuel-consumed'
                # Build out common name
                plant_df.loc[(plant_df['variable'].str.contains("NETGEN")) &
                             (~plant_df['energy_type'].isna()),
                             "sensor_name"] = (plant_df['energy_type'] +" - "
                                               + plant_df['prime_mover_type'] +
                                               " - " + plant_df['mer_type'] +
                                               " - Generation")
                plant_df.loc[(plant_df['variable'].str.contains("GROSSGEN")) &
                             (~plant_df['energy_type'].isna()),
                             "sensor_name"] = (plant_df['energy_type'] + " - "
                                               + plant_df['prime_mover_type'] +
                                               " - " + plant_df['mer_type'] +
                                               " - Gross Generation")
                plant_df.loc[(plant_df['variable'].str.contains("ELEC MMBTU")) &
                             (~plant_df['energy_type'].isna()),
                             "sensor_name"] = (plant_df['energy_type'] +" - "
                                               + plant_df['prime_mover_type'] +
                                               " - " + plant_df['mer_type'] +
                                               " - Quantity Consumed For Electricity")
                plant_df.loc[(plant_df['variable'].str.contains("TOT MMBTU")) &
                             (~plant_df['energy_type'].isna()),
                             "sensor_name"] = (plant_df['energy_type'] +" - "
                                               + plant_df['prime_mover_type'] +
                                               " - " + plant_df['mer_type'] +
                                               " - Total Fuel Consumed")
                # ADD NUCLEAR UNIT ID IF NECESSARY
                plant_df.loc[plant_df['NUCLEAR UNIT ID'] != "",
                             "sensor_name"] = (plant_df['sensor_name'] + " Unit " + 
                                               plant_df['NUCLEAR UNIT ID'])
                plant_df = plant_df[~plant_df['sensor_name'].isna()
                                    ].drop_duplicates()
                
                plant_df = plant_df[~plant_df['sensor_name'].isna()]
                plant_df = plant_df[['measured_on', 'sensor_name', 'value']].drop_duplicates()
                # Pivot it
                plant_df = pd.pivot_table(plant_df, values='value', index=['measured_on'], columns=['sensor_name'])
                # Write to a csv file
                plant_df.to_csv("./923_monthly_production/" + str(plant_id) + 
                                ".csv")
    return

def get_soup(URL):
    return bs(requests.get(URL, verify=False).text, 'html.parser')

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    # for link in get_soup(URL).findAll("a", attrs={'href': re.compile(".zip")}):
    #     file_link = link.get('href')
    #     print(file_link)
    #     try:
    #         with open(link.text, 'wb') as file:
    #             response = requests.get(URL + file_link, verify=False)
    #             z = zipfile.ZipFile(io.BytesIO(response.content))
    #             z.extractall("./923_extracts/" + file_link.replace(".zip", ""))
    #     except:
    #         print("Could not process the following file: " + file_link)
            
    # Glob glob the dataset
    
    files = (glob.glob(r"./923_extracts/*/*/*.xls*")
             + glob.glob(r"./923_extracts/*/*/*/*.xls*"))
    
    master_generation_df = pd.DataFrame()
    master_pr_df = pd.DataFrame()
    master_energy_storage_df = pd.DataFrame()
    
    # Column mappings that we want to clean up
    column_mappings = {"JANUARY": "JAN",
                       "FEBRUARY": "FEB",
                       "MARCH":"MAR",
                       "APRIL": "APR",
                       "JUNE": "JUN",
                       "JULY": "JUL",
                       "AUGUST": "AUG",
                       "SEPTEMBER": "SEP",
                       "OCTOBER": "OCT",
                       "NOVEMBER": "NOV",
                       "DECEMBER": "DEC",
                       "ELECTRIC": "ELEC",
                       "&": "AND",
                       "MMBTUJAN": "MMBTU JAN",
                       "MMBTUFEB": "MMBTU FEB",
                       "MMBTUMAR": "MMBTU MAR",
                       "MMBTUAPR": "MMBTU APR",
                       "MMBTUMAY": "MMBTU MAY",
                       "MMBTUJUN": "MMBTU JUN",
                       "MMBTUJUL": "MMBTU JUL",
                       "MMBTUAUG": "MMBTU AUG",
                       "MMBTUSEP": "MMBTU SEP",
                       "MMBTUOCT": "MMBTU OCT",
                       "MMBTUNOV": "MMBTU NOV",
                       "MMBTUDEC": "MMBTU DEC",
                       "MMBTUPER": "MMBTU PER",
                       "MMBTUS": "MMBTU",
                       "NUCLEAR UNIT I.D.": "NUCLEAR UNIT ID",
                       "PLANT STATE": "STATE",
                       "RESERVED ": "RESERVED",
                       'AER FUEL TYPE CODE': 'MER FUEL TYPE CODE'
                       }
    
    for file in files:
        file = file.replace("~$", "")
        df = pd.ExcelFile(file)
        # Get the available sheet names
        print(file)
        sheet_names = df.sheet_names
        generation_sheet = [x for x in sheet_names if "generation" in x.lower()]
        if len(generation_sheet) > 0:
            generation_df = df.parse(generation_sheet[0])
            index_cutoff = generation_df[generation_df[
                generation_df.columns[1]].str.lower() == 'plant id']
            if len(index_cutoff) == 0:
                index_cutoff = generation_df[
                    generation_df[generation_df.columns[0]].str.lower() =='plant id']    
            index_cutoff = index_cutoff.index[0]
            generation_df.columns = [
                x.replace("\n", " ").replace("_", " ").upper() 
                for x in list(generation_df.iloc[
                index_cutoff])]
            for month in column_mappings:
                generation_df.columns = [x.replace(month, column_mappings[month]) 
                                         for x in list(generation_df.columns)]
            generation_df= generation_df[generation_df.index > index_cutoff]
            generation_df = generation_df.loc[
                :,~generation_df.columns.duplicated()].copy()
            generation_df['file'] = os.path.basename(file)
            master_generation_df = pd.concat([master_generation_df, generation_df],
                                             axis=0)
    files_remove = filter_latest_year_data(master_generation_df)
    master_generation_df = master_generation_df[~master_generation_df[
        'file'].isin(files_remove)]    
    # Now that we've got all of our data in standardized format, let's
    # split by system, and build individual time series for each plant
    # Master generation data
    generation_metadata_columns = ['PLANT ID', 'COMBINED HEAT AND POWER PLANT',
                                   'NUCLEAR UNIT ID', 'PLANT NAME', 'OPERATOR NAME',
                                   'OPERATOR ID', 'STATE', 'CENSUS REGION',
                                   'NERC REGION', 'RESERVED', 'NAICS CODE',
                                   'EIA SECTOR NUMBER', 'SECTOR NAME',
                                   'REPORTED PRIME MOVER',
                                   'REPORTED FUEL TYPE CODE', 
                                   'MER FUEL TYPE CODE',
                                   'BALANCING AUTHORITY CODE',
                                   'RESPONDENT FREQUENCY',
                                   'PHYSICAL UNIT LABEL']
    removal_columns = ['EARLY RELEASE DATA (JUN 2025).\xa0 NOT FULLY EDITED, USE WITH CAUTION.\xa0 DO NOT AGGREGATE TO STATE, REGIONAL, OR NATIONAL TOTALS.',
                       'TOTAL FUEL CONSUMPTION QUANTITY',
                       'ELEC FUEL CONSUMPTION QUANTITY',
                       'TOTAL FUEL CONSUMPTION MMBTU',
                       'ELEC FUEL CONSUMPTION MMBTU',
                       'NET GENERATION (MEGAWATTHOURS)'    
                        ]
    joiner_columns = ["PLANT ID", "PLANT NAME", 'REPORTED PRIME MOVER',
                      'REPORTED FUEL TYPE CODE', 'MER FUEL TYPE CODE', 
                      'NUCLEAR UNIT ID',
                      'COMBINED HEAT AND POWER PLANT', "YEAR", 'file']
    process_master_plant_data(df = master_generation_df,
                              joiner_columns = joiner_columns,
                              metadata_columns = generation_metadata_columns,
                              removal_columns = removal_columns,
                              data_type = "generation")
