#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 23:50:35 2019

@author: jed
"""

import boto3
from zipfile import ZipFile
import pandas as pd
import json
import gzip
import fastparquet
from pathlib import Path


#  extracting data from s3
def download():
    s3 = boto3.client('s3')
    data = s3.download_file('blossom-data-eng-jedidiah', 'free-7-million-company-dataset.zip', 'free-7-million-company-dataset.zip')
    paths = str(Path(__file__).parent)
    sly = '/free-7-million-company-dataset.zip'
    pathsAdd = paths +  sly
    data = pathsAdd
    return data



#  unzipping zipped extracted data   
def extract(data):
    with ZipFile(data, 'r') as zipObj:
        zipObj.extractall()
        paths = str(Path(__file__).parent)
        sly = '/companies_sorted.csv'
        pathsAdd = paths +  sly
        extracted = pathsAdd
        print (extracted)
    return extracted

#  Filter out companies without a domain name using pandas
def country_no_domain(data):
    df = pd.read_csv(data) 
    a = df[df["domain"].isnull()]
    countries = a['country'].dropna()
    countriess = countries.to_csv(r'free-7-million-company-dataset.csv', index=None, header=True)
    paths = str(Path(__file__).parent)
    sly = '/free-7-million-company-dataset.csv'
    pathsAdd = paths +  sly
    countriess = pathsAdd
    return countriess

#  converting data into gzip 
def json_gzip(data):
    with gzip.open('free-7-million-company-dataset.json.gzip', 'wt') as f:
        gg = f.write(data)
    return gg


#  converting data into parquet   
def parquett(data):
    df = pd.read_csv(data)
    bb = df.to_parquet('b.parquet.gzip', compression='parquet')
    return bb




# upload file into s3
def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


#  automating etl process in a function
def main():
    data = download()
    print ('done')
    own = extract(data)
    print ('done1')
    use = country_no_domain(own)
    print ('done2')
    ggzzip = json_gzip(use)
    parquettss = parquett(use)
    upload_file(ggzzip, bucket)
    upload_file(parquettss, bucket)
    
    
if __name__ == '__main__':
    main()
   
      
