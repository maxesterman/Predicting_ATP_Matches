#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 11:02:37 2019

@author: maxsterman
"""
import pandas as pd
import os

folderDict = {4:'rankings',
              5:'players'}
dfDict = dict()

csv_url = r'../csv'


for key, value in folderDict.items():
    if key == 4:
        fName = csv_url + (r'/{}_{}/{}_column_titles.txt'.format(key,value,value))
    elif key == 5:
        fName = csv_url + (r'/{}_{}/player_overviews_column_titles.txt'.format(key,value))
        
    with open(fName,'r') as text_file_writer:
        column_names = text_file_writer.read()
        column_names = column_names.split('\n')
        column_names_dict = {i:column_names[i] for i in range(len(column_names))}
        
    if key == 4:
        yearlyPds = []
        
        for year in range(1991,2018):
            print(year)
            
            yearlyPds.append(pd.read_csv(csv_url + (r'/{}_{}/{}_{}.csv'.format(key,value,value,year)),header = -1))
                
        dfDict[value] = pd.concat(yearlyPds)
    elif key == 5:
        dfDict[value] = pd.read_csv(csv_url + (r'/{}_{}/player_overviews_UNINDEXED.csv'.format(key,value,value)),header = -1)
        
    dfDict[value].rename_axis(column_names_dict,axis = 1, inplace = True)
    dfDict[value].drop_duplicates(inplace = True)
    if "match_id" in dfDict[value].columns:
        dfDict[value].drop_duplicates(subset = "match_id",inplace = True)
