import pandas as pd

###############################################################
# This file generates restaurant reviews csv file of the four given cities

###############################################################

def go():
    rvw = pd.read_csv('rvw_groupby_rest.csv')
    for city in ["PNX", "CLV", "MAD", "MAK"]:
        C = pd.read_csv('new_neighbor_{}.csv'.format(city), \
            names=['id1','id2'])

        id1 = pd.DataFrame(C['id1'].unique())
        id1.columns =['business_id']
        a = id1.join(rvw, rsuffix='_rvw')
        a.drop('business_id_rvw',inplace=True, axis=1)
        a['business_id'] = a['business_id'].apply(lambda x: x[2:-1])

        a.to_csv('{}_rvw.csv'.format(city), header=True, index=False)




if __name__ == '__main__':
    go()
