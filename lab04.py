import pandas as pd
import numpy as np
import wtiproj03_ETL


def df_to_json(df):
    return df.to_dict(orient='records')


def json_to_df(json):
    return pd.DataFrame.from_dict(json)


def nan_to_zero(dict):
    for k, v in dict.items():
        if np.isnan(v):
            dict[k] = 0.0
        else:
            dict[k] = v


def calc_all_avg(df, genres_list):
    avg = {}
    for genre in genres_list:
        avg[genre] = df['rating'][df[genre] == 1].mean()

    nan_to_zero(avg)

    return avg


def calc_avg_for_user(df, genres_list, userID):
    df_for_user = df.loc[df['userID'] == int(userID)]
    x = calc_all_avg(df_for_user, genres_list)
    x['userID'] = userID
    return x


def user_dif(user_mean, all_mean):
    new_mean = {}
    for genre, mean in user_mean.items():
        if genre == 'userID':
            continue
        elif mean > 0.0:
            new_mean[genre] = all_mean[genre] - mean
        else:
            new_mean[genre] = mean
    pom = [v for v in new_mean.values()]
    pom = np.array(pom)
    nans = np.isnan(pom)
    pom[nans] = 0
    return new_mean, pom


if __name__ == '__main__':
    df, genres = wtiproj03_ETL.get_df()
    json = df_to_json(df)
    df2 = json_to_df(json)
    print(df.equals(df2))
    df.sort_index(axis=1, inplace=True)
    df2.sort_index(axis=1, inplace=True)
    print(df.equals(df2))

    avg_genres = calc_all_avg(df, genres)
    avg_for_user = calc_avg_for_user(df, genres, 78)
    new_avg_for_user, ar = user_dif(avg_for_user, avg_genres)
