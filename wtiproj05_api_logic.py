import wtiproj01_client
import wtiproj03_ETL
import lab04
import json
import pandas as pd

genres = wtiproj03_ETL.get_genres_list()


def fill_redis(df):
    i = 0
    for index, row in df.iterrows():
        if i > 1000:
            return
        j = row.to_json(orient='index')
        wtiproj01_client.add_to_queue('ratings', j)
        i += 1


def delete_ratings():
    wtiproj01_client.delete_all()


def add_rating(rate):
    wtiproj01_client.add_to_queue('ratings', rate)


def get_all_ratings():
    ratings_list = wtiproj01_client.get_whole_queue('ratings')
    dict_list = []
    for i in ratings_list:
        dict_list.append(json.loads(i))
    return pd.DataFrame.from_records(dict_list)


def get_avg_for_user(user_id):
    return lab04.calc_avg_for_user(get_all_ratings(), genres, user_id)


def get_avg_for_all():
    return lab04.calc_all_avg(get_all_ratings(), genres)


def get_user_ratings(user_id):
    df = get_all_ratings()
    return df.loc[df['userID'] == int(user_id)].to_json(orient='index')
