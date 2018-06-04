import pandas as pd
import json


######################################################################
# Convert review.json file to csv file

######################################################################


def get_column_name(first_line_contents):

    dict_df = {}
    keys = list(first_line_contents.keys())
    for k in keys:
        dict_df[k] = []

    return dict_df

def clean_rvw(x):
    try:
        return x.replace('\n', ' ')
    except:
        return x

def join_rvw(x):
    try:
        return '|||'.join(x)
    except:
        return x

def load_json_file():
    content = set()
    restaurant_id = pd.read_csv('restaurant_id.csv',index_col=0)
    restaurant_id = list(restaurant_id['0'])

    with open('review.json', encoding='utf8') as fin:
        i = 0
        for line in fin:
            line_contents = json.loads(line)
            # print(line_contents['business_id'])
            if i == 0:
                dict_df = get_column_name(line_contents)
                i += 1
            if line_contents['business_id'] in restaurant_id:
                # print(line_contents['business_id'])
                for k, v in line_contents.items():
                    dict_df[k].append(v)


    df = pd.DataFrame(dict_df)
    df.to_csv('review_rest.csv')
    df['text'] = df['text'].apply(clean_rvw)
    biz_text = df['text'].groupby('business_id').apply(join_rvw)
    pd.DataFrame(biz_text).to_csv('biz_text.csv')

if __name__ == '__main__':
    load_json_file()
