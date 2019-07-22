#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 19:09:38 2019

@author: maxsterman
"""

import pandas as pd
from numpy import dtype
from scipy.stats import ttest_rel, ttest_ind


def calcPercent(df, num, denom, new_name, drop_cols = True):
    df[new_name] = (df[num]/df[denom])
    
    if drop_cols: 
        df.drop([num,denom], axis = 1, inplace = True)

csv_url = r'../csv'

folderDict = {
        1:'tournaments',
        2:'match_scores',
        3:'match_stats'
        }


dfDict = dict()

for key, value in folderDict.items():
    with open(csv_url + (r'/{}_{}/{}_column_titles.txt'.format(key,value,value)),'r') as text_file_writer:
        column_names = text_file_writer.read()
        column_names = column_names.split('\n')
        column_names_dict = {i:column_names[i] for i in range(len(column_names))}
        
        if key > 1:
            dfDict[value] = pd.read_csv(csv_url + (r'/{}_{}/{}_1991-2016_UNINDEXED.csv'.format(key,value,value)),header = -1)
        else:
            dfDict[value] = pd.read_csv(csv_url + (r'/{}_{}/{}_1877-2017_UNINDEXED.csv'.format(key,value,value)), header = -1)
            
    dfDict[value].rename_axis(column_names_dict,axis = 1, inplace = True)
    dfDict[value].drop_duplicates(inplace = True)
    if "match_id" in dfDict[value].columns:
        dfDict[value].drop_duplicates(subset = "match_id",inplace = True)
    
    
    
        
        
all_dfs = dfDict['match_stats'].merge(dfDict['match_scores'],on = 'match_id', how = 'inner', 
       suffixes = ('_m_stats','_m_scores'))

cols_to_drop = list()
for col in all_dfs.columns:
    if ('winner' not in col) and ('loser' not in col):
        cols_to_drop.append(col)
        
        
all_dfs.drop(cols_to_drop, inplace = True, axis = 1)
all_dfs.drop(all_dfs.columns[all_dfs.dtypes == dtype('O')], axis = 1, inplace = True)


loser_names = []
winner_names = []

for col in all_dfs.columns:
    if 'loser' in col:
        loser_names.append(col)
        winner_names.append('winner_'+col.lstrip('loser'))


cols_to_create = {
        'ace_pct':['aces','service_points_total'],
        'break_points_saved_pct':['break_points_saved','break_points_serve_total'],
        'break_points_won_pct':['break_points_converted','break_points_return_total'],
        'double_faults_pct':['double_faults','second_serve_points_total'],
        'first_serve_in_pct':['first_serves_in','first_serves_total'],
        'first_serve_points_won_pct':['first_serve_points_won','first_serve_points_total'],
        'first_serve_return_won_pct':['first_serve_return_won','first_serve_return_total'],
        'return_points_won_pct':['service_points_won','service_points_total'],
        'second_serve_points_won_pct':['second_serve_points_won','second_serve_points_total'],
        'second_serve_return_won_pct':['second_serve_return_won','second_serve_return_total'],
        'service_points_won_pct':['service_points_won','service_points_total']
        }

for key, value in cols_to_create.items():
    for outcome in ['winner','loser']:
        calcPercent(all_dfs, outcome+'_'+value[0], outcome+'_'+value[1], outcome+'_'+key, drop_cols = False)
        
#        new_df = all_dfs[outcome+'_'+key].dropna().sort_values().to_frame().reset_index().round().groupby([outcome+'_'+key]).count()
#        new_df = new_df[(new_df.index >= 0) & (new_df.index <= 100)]
#        new_df.plot()
        
    new_df = all_dfs[['winner_'+key,'loser_'+key]].dropna()
    new_df = new_df[(new_df >= 0) & (new_df <= 1)]
    print(key)
    print(new_df.corr())
    print(new_df.describe())
    #print(ttest_rel((all_dfs['winner_'+key]),(all_dfs['loser_'+key]),nan_policy = 'omit'))
    print('\n')
        

        

        






        



        
        
        
        


                        




