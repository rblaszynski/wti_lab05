import pandas as pd
from pandas import DataFrame


def prepare():
    genres_columns = {'movieID': int, 'genre': str}
    genres = pd.read_csv('/home/robert/Wti/wtiproj01/movie_genres.dat', sep='\t', dtype=genres_columns,
                         encoding='latin-1')
    movie_genres = genres.astype(object)

    ratings_columns = {'userID': int, 'movieID': int, 'rating': float}
    ratings = pd.read_csv('/home/robert/Wti/wtiproj01/user_ratedmovies.dat', sep='\t', dtype=ratings_columns,
                          encoding='latin-1', usecols=ratings_columns)
    rated_movies = ratings.astype(object)

    movie_genres['dummyColumn'] = 1
    movies_pivoted = movie_genres.pivot_table(index="movieID", columns="genre", values='dummyColumn').add_prefix(
        "genre-")
    movies_pivoted = movies_pivoted.fillna(0)

    result = pd.merge(rated_movies, movie_genres, on="movieID")
    result = DataFrame(result, columns=['userID', 'movieID', 'rating'])

    result = pd.merge(result, movies_pivoted, on='movieID')
    result = result.drop_duplicates()
    result["movieID"] = pd.to_numeric(result["movieID"])
    result["userID"] = pd.to_numeric(result["userID"])
    result["rating"] = pd.to_numeric(result["rating"])

    return result.head(100)


def get_df():
    df1 = pd.read_csv('/home/robert/Wti/wtiproj01/user_ratedmovies.dat', sep="\t",
                      usecols=["userID", "movieID", "rating"], nrows=100)
    df2 = pd.read_csv('/home/robert/Wti/wtiproj01/movie_genres.dat', sep="\t", usecols=["movieID", "genre"])
    df2["dummy_column"] = 1
    df_pivoted = df2.pivot_table(index="movieID", columns="genre", values="dummy_column")
    df_pivoted = df_pivoted.fillna(0)
    df = pd.merge(df1, df_pivoted, on=["movieID"])
    df.columns = ["genre" + name if name not in df.columns[:3] else name for name in df.columns]
    genres_list = list(df)
    genres_list = [i for i in genres_list if i.startswith('genre')]
    return df, genres_list


def get_genres_list():
    data, genres_list = get_df()
    return genres_list


if __name__ == '__main__':
    df = prepare()
    # df_table = df.to_dict('records')
    # df2 = pd.DataFrame(df_table)

    df["movieID"] = pd.to_numeric(df["movieID"])
    df["userID"] = pd.to_numeric(df["userID"])
    df["rating"] = pd.to_numeric(df["rating"])
    print(df.groupby('movieID')['rating'].mean())
