
# Regress success score on average rating(neighborhood),
# average price range(neighborhood), average number of reviews (neighborhood),
# average review similarity (neighborhood), average category similarity (neighborhood),
# average sentiment score (neighborhood) by OLS.

import pandas as pd
import numpy as np
import statsmodels.api as sm


######################################################################
# Run OLS regression for four cities separately

######################################################################

def go():
    params = {}

    for city in ['PNX', 'MAD', 'MAK', 'CLV']:

        df_psr = pd.read_csv('~/WACC/Price_Star_Review/{}_price_star_review.csv'.format(city), sep=',')
        df_psr.columns = ['business_id', 'avr_rating', 'avr_num_review']
        df_psr['avr_price']=df_psr['business_id']
        df_psr['business_id'] = df_psr['business_id'].apply(lambda x: x.split('\t')[0][2:-1])
        df_psr['avr_price'] = df_psr['avr_price'].apply(lambda x: float(x.split('\t')[1][1:]))
        df_psr['avr_num_review'] = df_psr['avr_num_review'].apply(lambda x: float(x[:-1]))

        df_reviews = pd.read_csv('~/WACC/Reviews_similarity/sim_nbr_{}.csv'.format(city), sep='\t')
        df_reviews.columns = ['business_id', 'avr_re_sim']
        df_reviews['avr_re_sim'] = df_reviews['avr_re_sim'].apply(float)

        df_cate = pd.read_csv('~/WACC/Category/category_{}.csv'.format(city), sep='\t',\
                 names=['business_id', 'avr_cate_sim', 'overlap'])
        df_cate['business_id'] = df_cate['business_id'].apply(lambda x:x[2:-1])



        cols = ['business_id', 'score']
        df_score = pd.read_csv('~/WACC/Success_score/success_score_{}.csv'.format(city),\
        sep=',', usecols = cols)
        df_score['business_id'] = df_score['business_id'].apply(lambda x: x[2:-1])

        df_sent = pd.read_csv('avg_sent.csv', \
                                    names=['business_id', 'avr_sent_score'])


        df_all = pd.merge(df_score, df_psr,on=['business_id'],how='left')
        df_all = pd.merge(df_all, df_reviews, on=['business_id'], how='left')
        df_all = pd.merge(df_all, df_cate, on=['business_id'], how='left')
        df_all = pd.merge(df_all, df_sent, on=['business_id'], how='left')

        df_all['const'] = 1

        reg1 = sm.OLS(endog=df_all['score'].astype(float), \
            exog=df_all[['const', 'avr_rating','avr_price', 'avr_num_review', \
            'avr_re_sim', 'avr_cate_sim', 'avr_sent_score']].astype(float),\
                missing='drop')

        results1 = reg1.fit()
        print(results1.summary())
        print()
        params[city] = results1.params

    print('Coefficient Report')
    print(pd.DataFrame(params))



if __name__ == '__main__':
    go()
