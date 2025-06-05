#############################################################
#Script for fetching data for local business insights project
#This script will download all Scottish authority data and
#produce a single SQLite database
#Author: Conor Davidson
#Date: 02-06-2025
##############################################################

import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import requests
from pathlib import Path
import json

#Use for loop to down load each file
urls = ["https://ratings.food.gov.uk/api/open-data-files/FHRS760en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS761en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS762en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS763en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS768en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS791en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS770en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS772en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS764en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS771en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS781en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS787en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS773en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS774en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS775en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS776en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS777en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS778en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS783en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS784en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS765en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS779en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS785en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS786en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS788en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS767en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS789en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS766en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS780en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS790en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS769en-GB.xml",
        "https://ratings.food.gov.uk/api/open-data-files/FHRS782en-GB.xml"
       ]

#Use postcode data to fill in missing Longitude and latitude
def enrich_coordinates_in_dataframe(df, cache_file="postcode_cache.json"):
    import requests
    import json
    from pathlib import Path

    #Load or initialise cache
    if Path(cache_file).exists():
        with open(cache_file, "r") as f:
            postcode_cache = json.load(f)
    else:
        postcode_cache = {}

    #Clean postcode column
    df['PostCode'] = df['PostCode'].astype(str).str.strip().str.upper()

    #Identify postcodes needing lookup
    missing_coords = df[df['Latitude'].isnull() & df['PostCode'].notnull()]
    unique_postcodes = missing_coords['PostCode'].dropna().unique()
    postcodes_to_query = [pc for pc in unique_postcodes if pc not in postcode_cache]

    #Bulk fetch from API
    def bulk_lookup_postcodes(postcodes):
        url = "https://api.postcodes.io/postcodes"
        results = {}
        for i in range(0, len(postcodes), 100):
            batch = postcodes[i:i+100]
            try:
                res = requests.post(url, json={"postcodes": batch})
                if res.status_code == 200:
                    for item in res.json()['result']:
                        pc = item['query'].strip().upper()
                        if item['result'] is not None:
                            coords = {
                                'latitude': item['result']['latitude'],
                                'longitude': item['result']['longitude']
                            }
                            results[pc] = coords
            except Exception as e:
                print(f"Error during batch starting at index {i}: {e}")
        return results

    new_coords = bulk_lookup_postcodes(postcodes_to_query)
    postcode_cache.update(new_coords)

    # Save updated cache
    with open(cache_file, "w") as f:
        json.dump(postcode_cache, f, indent=2)
        
    # Track which rows were enriched
    enriched_mask = df['Latitude'].isnull() & df['PostCode'].isin(postcode_cache.keys())
    print("Original df is shape: ", df.shape)
    print("The enriched mask is: ", enriched_mask, " with shape: ", enriched_mask.shape, 
         " and count of True: ", enriched_mask.value_counts())

    # Map coordinates back into DataFrame
    def get_lat(row):
        coords = postcode_cache.get(row['PostCode'])
        return coords['latitude'] if coords else row['Latitude']

    def get_lon(row):
        coords = postcode_cache.get(row['PostCode'])
        return coords['longitude'] if coords else row['Longitude']

    df['Latitude'] = df.apply(get_lat, axis=1)
    df['Longitude'] = df.apply(get_lon, axis=1)
    
    #Add LocationApprox column
    df['LocationApprox'] = enriched_mask

    return df

####### BEGINNING DOWNLOAND OF XML FILES ##################
headers = {"User-Agent": "Mozilla/5.0"} #setup up the headers
for url in urls:
    response = requests.get(url, headers=headers)
    
    #save to file
    with open("master_fhrs.xml", "wb") as f:
        f.write(response.content)

    #Parse the XML
    tree = ET.parse("master_fhrs.xml")
    root = tree.getroot()

    #Find all EstablishmentDetail entries
    records = []
    for est in root.findall("./EstablishmentCollection/EstablishmentDetail"):
        entry = {}
        for elem in est:
            # Nested elements like <Geocode> contain children
            if len(elem):
                for sub_elem in elem:
                    entry[sub_elem.tag] = sub_elem.text
            else:
                entry[elem.tag] = elem.text
        records.append(entry)

    #Convert to DataFrame
    df = pd.DataFrame(records)
    
    #Add coordinates if null and Post Code is available
    df = enrich_coordinates_in_dataframe(df)

    #Display result
    print("For url: ", url)
    print(f"Total local authority records: {len(df)}")
    print("Fields:", df.columns.tolist())
    print(df[['BusinessName', 'BusinessType', 'PostCode', 'RatingValue']].head())

    # Save as sqlite3 database for easy analysis
    conn = sqlite3.connect("master_hygiene.db")
    df.to_sql("establishments", conn, if_exists='append', index=False)
    print("Database download complete for url: ", url)
    conn.close()

