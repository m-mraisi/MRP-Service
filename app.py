from pyclbr import Function
from unittest.mock import call
from flask import Flask, request, render_template, make_response, jsonify
import json
import os
import requests
# import psycopg2
# from psycopg2 import Error
import datetime as DT
import time
import threading
import pandas as pd 
import re
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'


app = Flask(__name__)

def cleansing_function(df):
    df = df.copy()
    abnType = df['type'].unique()[5] ## taking out the abnormal hosue type
    df = df[df['type']!= abnType]
    ## checking all the cities that are in the data

    '''
    we are going to split the data using the "," and then split based on teh space and get the city from each one,

    This will be added to a new column called "city"
    '''
    df["city"] = df["full_address"].apply(lambda x: x.split(",")[1].split(" ")[1].upper())
    ## now we will exclude the only row with the value "OUT" as city

    df = df[df["city"] != "OUT"]
    ## now let's clean the bedrooms column

    df["bedrooms"] = df["bedrooms"].apply(lambda x: ' '.join(x.split(" ")))

    for i,n in enumerate(df["bedrooms"]):
        df["bedrooms"].iloc[i] = max([int(s) for s in n.split() if s.isdigit()])
    ## now let's clean the bathrooms column

    df["bathrooms"] = df["bathrooms"].apply(lambda x: int(''.join(x.lower()).replace("baths", '')))
    ## now let's clean the parking column
    df["parking"] = df["parking"].apply(lambda x:    str(x).replace('no parking', '0')  )
    df["parking"] = df["parking"].apply(lambda x:    int(str(x).replace(' parking', '')  ))
    
    ## cleaning the type column

    df["type"] = df["type"].str.upper()

    for i,t in enumerate(df["type"]):
        if "APT" in t:
            df["type"].iloc[i] = "APT"
        elif "CONDO" in t:
            df["type"].iloc[i] = "CONDO"
        elif "DETACHED" in t:
            df["type"].iloc[i] = "DETACHED_HOUSE"
        elif "TWNHOUSE" in t:
            df["type"].iloc[i] = "TWNHOUSE"
    df["type"] = df["type"][df["type"] != "COTTAGE"][df["type"] != "FOURPLEX"]
    
    ## counting out rows that have bathrooms more than bedrooms
    df = df.query("bedrooms >= bathrooms")
    
    ## cleaning the sqft column and applying the average on each value
    sqft = df["sqft"].str.split(" ", 1).apply(lambda x: x[0])
    sqft.replace({"N/A": "0-0"}, inplace=True)
    sqft = sqft.apply(lambda x: re.split('–|-',x))

    sqft = sqft.apply(lambda x: min(int(x[0]) , int(x[1])))
    sqft.replace({0: np.nan}, inplace=True)
    df["sqft"] = sqft
    
    frames = []
    for room in list(set(df['bedrooms'])):
        df_rooms = df[df['bedrooms']== room]
        df_rooms['sqft'].fillna(df_rooms['sqft'].min(),inplace = True)
        frames.append(df_rooms)
    final_df = pd.concat(frames)
    final_df = final_df.reset_index().drop(columns=['index', 'level_0'])
    final_df["sqft"] = final_df["sqft"].round()
    df = final_df
    print("df.shape", df.shape)
    return df.head(300)


@app.route('/')  # home page
def index():
    return jsonify({"test":True})

@app.route('/getData') 
def getData():
    df = pd.read_csv("https://raw.githubusercontent.com/slavaspirin/Toronto-housing-price-prediction/master/houses.csv")
    original_data = df.copy()
    clean_data = cleansing_function(df)
    array = []
    array.append(clean_data.to_json())
    print(clean_data.to_json())
    # data = json.loads(jsonify(array))
    return make_response(clean_data.to_json())

if __name__ == "__main__":
    app.run()
