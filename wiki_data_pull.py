from google.cloud.bigquery import dataset
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import json
import os
from google.cloud import bigquery

# Variables
url_2000s = "https://en.wikipedia.org/wiki/List_of_school_shootings_in_the_United_States"
url_1900s = "https://en.wikipedia.org/wiki/List_of_school_shootings_in_the_United_States_(before_2000)"
gcp_project = "twitter-project-328515"
bq_dataset = "wiki"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'sandbox/master_key.json'

# Connection
client = bigquery.Client(project=gcp_project)
dataset_ref = client.dataset(bq_dataset)

# Regular expressions to clean up the data
regex_pattern = '([A-Za-z, 0-9]+)\\n'
regex_loc = '([A-Za-z, .]*)\\n'
regex_num = '(^\d?\d?\d?\d)'
regex_des = '(?<![\\\[])[^\\\[\]]+'
regex_school = '(((([A-Z]\w+ )?([A-Z]\w+ )?([A-Z])(\w+-\w+)?(\w*\.\w*\. )?([^(a|A)t ]\w* )?([A-Z][\w+ )?([^at ]\w+\,? )?(\w\.)?(\w+\. )?)|([A-Z]\w+ )([A-Z]\w+ )?([A-Z]\w+ )?)(((e|E)lementary |(m|M)iddle |(h|H)igh )?((s|S)chool)|(c|C)ollege|(u|U)niversity|(c|C)ollegiate|(i|I)nstitute (of )? ([A-Z]\w+ )|(s|S)tadium|(C)ampus|(a|A)cademy (of )?([A-Z]\w+ )?([A-Z]\w+ ?)?([A-Z]\w+)?)|(school bus)|(([A-Z]\w+ )?((u|U)niversity |(c|C)ollege |(a|A)cademy |(i|I)nstitute )((of )(the )?(([A-Z])(\w+ ?))([A-Z]\w+ )?([A-Z]\w+)?))|(s|S)tadium|(\w+ )(Elementary)|(UCLA))'

# Function to extract info
def _extract_date(text):
    date = re.findall(regex_pattern, text)[0]
    date = datetime.strptime(date, "%B %d, %Y")
    return date

def _extract_location(text):
    location = re.findall(regex_loc, text)[0]
    return location

def _extract_death(text):
    death = re.findall(regex_num, text)[0]
    death = int(death)
    return death

def _extract_injury(text):
    injury = re.findall(regex_num, text)[0]
    injury = int(injury)
    return injury

def _extract_description(text):
    description = re.findall(regex_des, text)[0]
    return description

def _extract_school(text):
    school = re.findall(regex_school, text)
    return school

# Function to create dict
def parse_table_rows(table_rows):
    
    table_data_raw = {}
    table_data = {}
    
    try:
        for ix, tr in enumerate(table_rows[1:], start = 0):
            data_cols = tr.find_all("td")
            if data_cols == []:
                print(f"No Data on {ix}th entry")
            else:
                date_raw = data_cols[0].text
                date = _extract_date(date_raw)
                
                location_raw = data_cols[1].text
                location = _extract_location(location_raw)
                
                school = _extract_school(data_cols[4].text)
                if school == []:
                    print(f"No Data on {ix}th entry for school")
                    school = pd.NA
                else:
                    school = school[0][0]
#                     print(school)

                death_raw = data_cols[2].text
                injury_raw = data_cols[3].text
                description_raw = data_cols[4].text
            
                if "n 1" in death_raw and "n 1" in injury_raw:
                    DIS = True
                    IIS = True
                elif "n 1" in death_raw:
                    DIS = True
                    IIS = False
                elif "n 1" in injury_raw:
                    IIS = True
                    DIS = False
                else:
                    DIS = False
                    IIS = False
                death = _extract_death(death_raw)
                if injury_raw == "?\n":
                    injury = pd.NA
                else:
                    injury = _extract_injury(injury_raw)
                description = _extract_description(description_raw)

                row_data_raw = {'Date': date_raw, 
                                'Location': location_raw,
                                'Death': death_raw,
                                'Injury': injury_raw,
                                'Description': description_raw}
                table_data_raw[ix] = row_data_raw
                
                row_data = {'Date': date, 
                            'Location': location, 
                            'School': school,
                            'Death': death,
                            'Death_Including_Suspect': DIS,
                            'Injury': injury, 
                            'Injury_Including_Suspect': IIS,
                            'Description': description}
                table_data[ix] = row_data
                
        return table_data, table_data_raw
    except:
        print("No data")


response_19 = requests.get(url_1900s)
response_20 = requests.get(url_2000s)

soup_19 = BeautifulSoup(response_19.text, 'html.parser')
soup_20 = BeautifulSoup(response_20.text, 'html.parser')

table_rows_19 = soup_19.find_all(name="tr")
table_rows_20 = soup_20.find_all(name="tr")

t_s_19, t_s_19_raw = parse_table_rows(table_rows_19)
t_s_20, t_s_20_raw = parse_table_rows(table_rows_20)

df_19_raw = pd.DataFrame.from_dict(t_s_19_raw, orient = 'index')
df_20_raw = pd.DataFrame.from_dict(t_s_20_raw, orient = 'index')

df_19 = pd.DataFrame.from_dict(t_s_19, orient = 'index')
df_20 = pd.DataFrame.from_dict(t_s_20, orient = 'index')

df_20_raw.to_gbq(destination_table="wiki.wiki_20_raw", project_id=gcp_project, if_exists="replace")
df_19_raw.to_gbq(destination_table="wiki.wiki_19_raw", project_id=gcp_project, if_exists="replace")

df_19.to_gbq(destination_table="wiki.wiki_19", project_id=gcp_project, if_exists="replace")
df_20.to_gbq(destination_table="wiki.wiki_20", project_id=gcp_project, if_exists="replace")