import pandas as pd
import sys


def success_score():

    cols = ['business_id', 'city', 'stars', 'review_count', 'categories', 'is_open']
    city = ["b'Las Vegas'", "b'Phoenix'", "b'Toronto'", "b'Montreal'"]
    restaurant = pd.read_csv('restaurant.csv', usecols = cols)
    #restaurant=restaurant.rename(columns = {'is_open':'open'})
    open_rev = restaurant[restaurant['is_open'] == 1]
    for c in city:
        df = open_rev[open_rev['city'] == c]
        quant_rev = df[['review_count']].quantile([.25, .5, .75, 1])
        df = df.sort_values(by=['review_count'])
        df = df.assign(review_scale="")
        df = df.assign(score="")
        df.loc[df['review_count'] <= quant_rev.review_count.iloc[0], "review_scale"] = 1
        df.loc[(df['review_count'] <= quant_rev.review_count.iloc[1]) & (df['review_count'] > quant_rev.review_count.iloc[0]), "review_scale"] = 2 
        df.loc[(df['review_count'] <= quant_rev.review_count.iloc[2]) & (df['review_count'] > quant_rev.review_count.iloc[1]), "review_scale"] = 3
        df.loc[df['review_count'] > quant_rev.review_count.iloc[2], 'review_scale'] = 4
        #df['review_scale'] = df.apply(rescale_ratings, axis=1)
        df["score"] = df["stars"] * df["review_scale"]
        df.to_csv( c.strip("b'") + '.csv', encoding='utf-8')



