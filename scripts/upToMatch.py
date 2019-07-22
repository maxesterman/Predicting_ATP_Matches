#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 20 13:19:37 2019

@author: maxsterman
"""

import pandas as pd
from numpy import dtype
import numpy as np
from scipy.stats import ttest_rel, ttest_ind

from sklearn.linear_model import RidgeClassifierCV
from sklearn import preprocessing
import random

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
    
    
    
        
tourney_df = dfDict['tournaments'][['tourney_year','tourney_order','tourney_year_id']]
        
all_dfs = dfDict['match_stats'].merge(dfDict['match_scores'],on = 'match_id', how = 'inner', 
       suffixes = ('_m_stats','_m_scores'))

all_dfs = all_dfs.merge(tourney_df, on = 'tourney_year_id', how = 'inner')


all_dfs = all_dfs[~(all_dfs.tourney_round_name.str.contains('Round Robin'))]
all_dfs = all_dfs[~(all_dfs.tourney_round_name.str.contains('Olympic Bronze'))]

tourney_round_enum = {
        '1st Round Qualifying':0,
        '2nd Round Qualifying':1,
        '3rd Round Qualifying':2,
        'Round of 128':3,
        'Round of 64':4,
        'Round of 32':5,
        'Round of 16':6,
        'Quarter-Finals':7,
        'Semi-Finals':8,
        'Finals':9
        }

all_dfs.tourney_round_name.replace(to_replace=tourney_round_enum, inplace = True)

all_dfs['tourney_order_round_name'] = all_dfs['tourney_order']*10+all_dfs['tourney_round_name']



winner_column_list = list()
loser_column_list = list()
winner_column_dict = dict()
loser_column_dict = dict()

for i in range(len(all_dfs.columns)):
    if 'loser' not in all_dfs.columns[i]:
        
        winner_column_list.append(all_dfs.columns[i])
        winner_column_dict[all_dfs.columns[i]] = all_dfs.columns[i].replace('winner_','')
    
    if 'winner' not in all_dfs.columns[i]:
        loser_column_list.append(all_dfs.columns[i])
        loser_column_dict[all_dfs.columns[i]] = all_dfs.columns[i].replace('loser_','')
        
winner_df = all_dfs[winner_column_list]
loser_df = all_dfs[loser_column_list]

winner_df.rename_axis(winner_column_dict,axis = 1, inplace = True)
loser_df.rename_axis(loser_column_dict, axis = 1, inplace = True)

winner_df.sort_index(axis = 1, inplace = True)
loser_df.sort_index(axis = 1, inplace = True)

complete_df = pd.concat([winner_df,loser_df])
complete_df.sort_values(['tourney_order_round_name'],inplace =True)
#complete_df.set_index(['player_id','tourney_year','tourney_order_round_name','match_id'], inplace = True)
#complete_df.set_index(['player_id','tourney_year','tourney_order_round_name','match_id'], inplace = True)

complete_df.set_index(['player_id','tourney_order_round_name','match_id'], inplace = True)

complete_df.dtypes.to_dict()
dTypeDict = complete_df.dtypes.to_dict()
keep_list = []





for series, var_type in dTypeDict.items():
    if var_type != dtype('O'):
        keep_list.append(series)
        
complete_df = complete_df[keep_list]
import time



start_time = time.time()
i = 0
player_names = complete_df.index.levels[0]
cumSum_df_list = []
for player in player_names:
    i+=1
    if (i % 100) == 0:
        print(i)
        print("%.2f seconds" %(time.time()-start_time))
        start_time = time.time()
    curr_df = complete_df.loc[player,:]
    new_dfs = [curr_df.shift(i).rename_axis({col:(col+'_t-{}'.format(i))\
                             for col in curr_df.columns},axis =1) for i in range(1,11)]
    
    new_dfs += [curr_df.rolling(10).mean().shift(1).rename_axis({col:(col+'_10MMA'.format(10))\
                             for col in curr_df.columns},axis =1)]
    new_df = pd.concat(new_dfs, axis = 1)
    new_df['player_id'] = player
    new_df.reset_index(drop = False, inplace = True)
    cumSum_df_list.append(new_df)
    
    
    '''
    years = curr_df.index.levels[0]


    for year in years:
        use_df = curr_df.loc[year,:]
        use_df = use_df[keep_list].cumsum(axis = 0)
        use_df = use_df.shift(1)
        use_df['player_id'] = player
        use_df['tourney_year'] = year
        use_df.reset_index(drop = False, inplace = True)
        cumSum_df_list.append(use_df)
    '''        
        
cumSumDf = pd.concat(cumSum_df_list)
cumSumDf.reset_index(drop = True)
winnerPctDf = winner_df[['player_id','match_id']].merge(cumSumDf, on = ['player_id','match_id'], how = 'inner')
loserPctDf = loser_df[['player_id','match_id']].merge(cumSumDf, on = ['player_id','match_id'], how = 'inner') 
completePctDf = winnerPctDf.merge(loserPctDf,on = 'match_id', how = 'inner', 
       suffixes = ('_winner','_loser'))
completePctDf.dropna(inplace = True)
completePctDf.reset_index(drop = True,inplace = True)


listOfSetOfCols = list(map(set,cols_to_create.values()))
setOfCols = set()

for cols in listOfSetOfCols:
    setOfCols = setOfCols.union(cols)
listOfCols = list(setOfCols)
winnerMap = lambda name: name+'_10MMA_winner'
loserMap = lambda name: name+'_10MMA_loser'

columnsToKeep  = list(map(winnerMap,listOfCols)) + list(map(loserMap,listOfCols))
columnsToKeep  = ['match_id'] + columnsToKeep

numericMatrix = completePctDf[columnsToKeep]

sampleToWin = random.sample(range(numericMatrix.shape[0]), 
                            (numericMatrix.shape[0]//2))
sampleToLose = list(set(numericMatrix.index)-set(sampleToWin))

numericMatrixWin = numericMatrix.loc[sampleToWin,:]
numericMatrixLose = numericMatrix.loc[sampleToLose,:]

def winnerMatMapper(name):
    if '_winner' in name:
        return name.replace('_winner','_Player_1')
    elif '_loser' in name:
        return name.replace('_loser','_Player_2')
    else:
        return name

def loserMatMapper(name):
    if '_winner' in name:
        return name.replace('_winner','_Player_2')
    elif '_loser' in name:
        return name.replace('_loser','_Player_1')
    else:
        return name


numericMatrixWin.rename_axis({name:winnerMatMapper(name) for name in numericMatrixWin},
                             axis = 1, inplace = True)
numericMatrixLose.rename_axis({name:loserMatMapper(name) for name in numericMatrixLose},
                             axis = 1, inplace = True)
numericMatrixWin['Player_1_Win'] = 1
numericMatrixLose['Player_1_Win'] = 0

wholeMatrixToUse = pd.concat([numericMatrixWin,numericMatrixLose],axis = 0)
wholeMatrixToUse.sort_index()

for outcome in ['Player_1','Player_2']:
    for key, value in cols_to_create.items():
        calcPercent(wholeMatrixToUse, value[0]+'_10MMA_'+outcome, 
                    value[1]+'_10MMA_'+outcome, key+'_10MMA_'+outcome, 
                    drop_cols = False)
    colsToDrop = [col+'_10MMA_'+outcome for col in listOfCols]
    wholeMatrixToUse.drop(colsToDrop, axis = 1, inplace = True)
    
    
wholeMatrixToUse.set_index('match_id', inplace = True)
wholeMatrixToUse.dropna(inplace = True)

XVars = list(wholeMatrixToUse.columns)
XVars.remove('Player_1_Win')
X = wholeMatrixToUse[XVars]
Y = wholeMatrixToUse[['Player_1_Win']]

varsAsList = list(cols_to_create.keys())
X_Player_1 = X[[varsAsList[i]+'_10MMA_Player_1' for i in range(len(varsAsList))]]
X_Player_2 = X[[varsAsList[i]+'_10MMA_Player_2' for i in range(len(varsAsList))]]
X_Player_1.columns = varsAsList
X_Player_2.columns = varsAsList
X = X_Player_2 - X_Player_1
X = pd.DataFrame(data = preprocessing.scale(X), columns = X.columns, index = X.index)
X = (0.5*(0.01*np.tanh(X)+1))


ridge_Settings = RidgeClassifierCV(alphas = tuple([(i/20) for i in range(200)]), cv = 10)
ridge_Settings.fit(X,Y)
    
                
        



