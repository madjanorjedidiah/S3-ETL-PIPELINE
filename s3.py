#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 02:31:37 2019

@author: jed
"""

import boto3
from zipfile import ZipFile
import pandas as pd
import json
import gzip
import fastparquet
import avro.schema as pdx
from pathlib import Path

def download():
    s3 = boto3.resource('s3')
    data = s3.Bucket('blossom-data-eng-jedidiah').download_file('free-7-million-company-dataset.zip', '/home/jed/blossom/blossom works/free-7-million-company-dataset.zip')
    paths = str(Path(__file__).parent)
    sly = '/free-7-million-company-dataset.zip'
    pathsAdd = paths +  sly
    data = pathsAdd
    return data


def extract(data):
    with ZipFile(data, 'r') as zipObj:
        zipObj.extractall()
        paths = str(Path(__file__).parent)
        sly = '/companies_sorted.csv'
        pathsAdd = paths +  sly
        extracted = pathsAdd
        print (extracted)
    return extracted




def country_no_domain(data):
    df = pd.read_csv(data) 
    a = df[df["domain"].isnull()]
    countries = a['country'].dropna()
    countriess = countries.to_csv(r'/home/jed/blossom/blossom works/free-7-million-company-dataset.csv', index=None, header=True)
    paths = str(Path(__file__).parent)
    sly = '/countryNoDomain.csv'
    pathsAdd = paths +  sly
    countriess = pathsAdd
    return countriess



    
def json_gzip(data):
    with gzip.open('/home/jed/blossom/blossom works/free-7-million-company-dataset.json.gzip', 'wt') as f:
        gg = f.write(data)
    return gg
   
def parquett(data):
    df = pd.read_csv(data)
    bb = df.to_parquet('free-7-million-company-dataset.parquet.gzip', compression='gzip')
    return bb

def avronn(data):
    df = pd.read_csv(data)
    aa= df.to_json(r'/home/jed/blossom/blossom works/free-7-million-company-dataset.json')
    pdx.parse(countryNoDomain.json)
#    
#    
def main():
    data = download()
    own = extract(data)
    use = country_no_domain(own)
    ggzzip = json_gzip(use)
    parquettss = parquett(use)
    aavvrroo = avronn(use)
#    
    
if __name__ == '__main__':
    main()
   
    