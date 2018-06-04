
# Regress success score on average rating(neighborhood), 
# average price range(neighborhood), average number of reviews (neighborhood), 
# average sentiment score (neighborhood), and average review similarity (neighborhood)
# by OLS.

import pandas as pd 
import numpy as np 
from sklearn import linear_model 
import statsmodels.api as sm

for city in ['PNX', 'MAD', 'MAK', 'CLV']:
	df_psr = pd.read_csv('{}_price_star_review.csv'.format(city), sep=',')
	df_psr.columns = ['business_id', 'avr_rating', 'avr_num_review']
	df_psr['avr_price']=df_psr['business_id'] 
	df_psr['business_id'] = df_psr['business_id'].apply(lambda x: x.split('\t')[0][2:-1])
	df_psr['avr_price'] = df_psr['avr_price'].apply(lambda x: float(x.split('\t')[1][1:]))
	df_psr['avr_num_review'] = df_psr['avr_num_review'].apply(lambda x: float(x[:-1]))
	df_reviews = pd.read_csv('sim_nbr_{}.csv'.format(city), sep='\t')
	df_reviews.columns = ['business_id', 'avr_re_sim']
	df_reviews['avr_re_sim'] = df_reviews['avr_re_sim'].apply(float)

	cols = ['business_id', 'score']
	df_score = pd.read_csv('~/WACC/Success_score/success_score_{}.csv'.format(city),\
	sep=',', usecols = cols)
	df_score['business_id'] = df_score['business_id'].apply(lambda x: x[2:-1])


	df_all = df_score.join(df_psr, rsuffix='_psr')
	df_all = df_all.join(df_reviews, rsuffix='_rvw')
	df_all.drop(['business_id_psr', 'business_id_rvw'], axis=1, inplace=True)

	df_all['const'] = 1

	reg1 = sm.OLS(endog=df_all['score'].astype(float), exog=df_all[['const', 'avr_rating', 
		'avr_price', 'avr_num_review', 'avr_re_sim']].astype(float), missing='drop')
	#'avr_sentiment',

	results1 = reg1.fit()
	print(results1.summary())