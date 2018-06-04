import pandas as pd
import sys


nhd = pd.read_csv('new_neighbor_CLV.csv', 
    names = ['c_rest', 'n_rest']) # The name of csv needs to be changed manually 
cols = ['business_id', 'city', 'stars', 'review_count', 'is_open']
neighbor = list(nhd['n_rest'].unique()) + list(nhd['c_rest'].unique())
restaurant = pd.read_csv('restaurant.csv', usecols = cols)
open_rev = restaurant[restaurant['is_open'] == 1]
notopen_rev = restaurant[restaurant['is_open'] == 0]

def compute_success_score(neighbor, df): 
    '''
    Compute success score for each neighborhood restaurant within the city 
    Inputs:
        neighbor: numpy array - identified restaurants for all neighborhoods 
        within the city  
        df: pandas dataframe 
    Outputs:
    df_n: dataframe with success score 
    '''
    df_n = df[df['business_id'].isin(neighbor)]
    if df_n.shape[0] >= 4:
        quant_rev = df_n[['review_count']].quantile([.25, .5, .75, 1])
        df_n = df_n.assign(review_scale="")
        df_n = df_n.assign(score="")
        df_n.loc[df_n['review_count'] <= quant_rev.review_count.iloc[0], 'review_scale'] = 1
        df_n.loc[(df_n['review_count'] <= quant_rev.review_count.iloc[1]) & \
            (df_n['review_count'] > quant_rev.review_count.iloc[0]), 'review_scale'] = 2 
        df_n.loc[(df_n['review_count'] <= quant_rev.review_count.iloc[2]) & \
            (df_n['review_count'] > quant_rev.review_count.iloc[1]), 'review_scale'] = 3
        df_n.loc[df_n['review_count'] > quant_rev.review_count.iloc[2], 'review_scale'] = 4
        df_n['score'] = df_n['stars'] * df_n['review_scale']
            
    return df_n

df_total = compute_success_score(neighbor, restaurant)
df_total.to_csv('success_score_CLV.csv', encoding='utf-8', index=False)



