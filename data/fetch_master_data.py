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
