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
import avro.schema as pdx
from pathlib import Path

def download():
    s3 = boto3.client('s3')
    data = s3.download_file('blossom-data-eng-jedidiah', 'free-7-million-company-dataset.zip', 'free-7-million-company-dataset.zip')
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
    countriess = countries.to_csv(r'free-7-million-company-dataset.csv', index=None, header=True)
    paths = str(Path(__file__).parent)
    sly = '/free-7-million-company-dataset.csv'
    pathsAdd = paths +  sly
    countriess = pathsAdd
    return countriess



    
def json_gzip(data):
    with gzip.open('free-7-million-company-dataset.json.gzip', 'wt') as f:
        gg = f.write(data)
    return gg
   
def parquett(data):
    df = pd.read_csv(data)
    bb = df.to_parquet('free-7-million-company-dataset.parquet.gzip', compression='gzip')
    return bb





def main():
    data = download()
    own = extract(data)
    use = country_no_domain(own)
    ggzzip = json_gzip(use)
    parquettss = parquett(use)
    
    
if __name__ == '__main__':
    main()
      