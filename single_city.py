import pandas as pd
df = pd.read_csv('/Users/shuting/WACC/Data/restaurant.csv')
restaurants_df = df[df.categories.str.contains('Restaurants')]
count_res = restaurants_df.city.value_counts()
city_100 = count_res[count_res >= 100]
for c in city_100:
	df1 = restaurants_df[restaurants_df['city'] == c]
	df2 = df1[["business_id", "latitude", "longitude", "city"]]
	df2.to_csv( c.strip("b'"), encoding='utf-8', header=False, index=False)

