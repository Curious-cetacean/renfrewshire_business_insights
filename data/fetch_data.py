#############################################################
#Script for fetching data for local business insights project
#Author: Conor Davidson
#Date: 28-05-2025
##############################################################

import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import requests

# Step 1: Download the XML file
url = "https://ratings.food.gov.uk/api/open-data-files/FHRS788en-GB.xml"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)

# Save to file
with open("renfrewshire_fhrs.xml", "wb") as f:
    f.write(response.content)

# Step 2: Parse the XML
tree = ET.parse("renfrewshire_fhrs.xml")
root = tree.getroot()

# Step 2: Find all EstablishmentDetail entries
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

# Step 3: Convert to DataFrame
df = pd.DataFrame(records)

# Step 4: Display result
print(f"Total Renfrewshire records: {len(df)}")
print("Fields:", df.columns.tolist())
print(df[['BusinessName', 'BusinessType', 'PostCode', 'RatingValue']].head())

# Save as sqlite3 database for easy analysis
conn = sqlite3.connect("renfrewshire_hygiene.db")
df.to_sql("establishments", conn, if_exists='replace', index=False)
conn.close()