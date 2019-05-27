from cassandra.cluster import Cluster
from cassandra.query import dict_factory

import wtiproj06_cassandra_client
import wtiproj03_ETL
import lab04
import pandas as pd

genres = ['genreaction', 'genreadventure', 'genreanimation', 'genrechildren',
          'genrecomedy', 'genrecrime', 'genredocumentary', 'genredrama',
          'genrefantasy', 'genrefilmnoir', 'genrehorror', 'genreimax',
          'genremusical', 'genremystery', 'genreromance', 'genrescifi',
          'genreshort', 'genrethriller', 'genrewar', 'genrewestern']


class WtiProj06:
    def __init__(self):
        self.genres = wtiproj03_ETL.get_genres_list()
        self.keyspace = "user_ratings"
        self.rating_table = "rating"
        self.user_avg_table = "user_avg"
        self.all_avg_table = "all_avg"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        wtiproj06_cassandra_client.create_keyspace(self.session, self.keyspace)
        wtiproj06_cassandra_client.create_rating_table(self.session, self.keyspace, self.rating_table)
        wtiproj06_cassandra_client.create_user_avg_table(self.session, self.keyspace, self.user_avg_table)
        wtiproj06_cassandra_client.create_all_avg_table(self.session, self.keyspace, self.all_avg_table)
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory

    def add_rating(self, rate):
        wtiproj06_cassandra_client.push_rating(self.session, self.keyspace, self.rating_table, rate['userID'],
                                               rate['movieID'],
                                               rate['rating'], rate['genre-Action'],
                                               rate['genre-Adventure'], rate['genre-Animation'],
                                               rate['genre-Children'],
                                               rate['genre-Comedy'],
                                               rate['genre-Crime'], rate['genre-Documentary'], rate['genre-Drama'],
                                               rate['genre-Fantasy'],
                                               rate['genre-Film-Noir'], rate['genre-Horror'], rate['genre-IMAX'],
                                               rate['genre-Musical'],
                                               rate['genre-Mystery'], rate['genre-Romance'], rate['genre-Sci-Fi'],
                                               rate['genre-Short'],
                                               rate['genre-Thriller'], rate['genre-War'], rate['genre-Western'])

    def delete_ratings(self):
        print()  # wtiproj06_cassandra_client

    def add_avg_for_all(self):
        avg = self.calc_avg_for_all()
        wtiproj06_cassandra_client.add_all_avg(self.session, self.keyspace, self.all_avg_table,
                                               avg['genreaction'],
                                               avg['genreadventure'], avg['genreanimation'],
                                               avg['genrechildren'],
                                               avg['genrecomedy'],
                                               avg['genrecrime'], avg['genredocumentary'], avg['genredrama'],
                                               avg['genrefantasy'],
                                               avg['genrefilmnoir'], avg['genrehorror'], avg['genreimax'],
                                               avg['genremusical'],
                                               avg['genremystery'], avg['genreromance'], avg['genrescifi'],
                                               avg['genreshort'],
                                               avg['genrethriller'], avg['genrewar'], avg['genrewestern'])

    def calc_avg_for_all(self):
        return lab04.calc_all_avg(self.get_all_ratings(), genres)

    def get_all_ratings(self):
        ratings_list = wtiproj06_cassandra_client.get_ratings(self.session, self.keyspace, self.rating_table)
        return pd.DataFrame.from_records(ratings_list.current_rows)

    def add_user_avg(self, userid):
        avg = self.calc_avg_for_user(userid)
        wtiproj06_cassandra_client.add_avg(self.session, self.keyspace, self.user_avg_table, avg['userid'],
                                           avg['genreaction'],
                                           avg['genreadventure'], avg['genreanimation'],
                                           avg['genrechildren'],
                                           avg['genrecomedy'],
                                           avg['genrecrime'], avg['genredocumentary'], avg['genredrama'],
                                           avg['genrefantasy'],
                                           avg['genrefilmnoir'], avg['genrehorror'], avg['genreimax'],
                                           avg['genremusical'],
                                           avg['genremystery'], avg['genreromance'], avg['genrescifi'],
                                           avg['genreshort'],
                                           avg['genrethriller'], avg['genrewar'], avg['genrewestern'])

    def get_all_avg(self):
        avg = wtiproj06_cassandra_client.get_ratings(self.session, self.keyspace, self.all_avg_table)
        avg.current_rows[0].pop('uuid', None)
        return avg.current_rows[0]

    def calc_avg_for_user(self, args):
        return lab04.calc_avg_for_user(self.get_all_ratings(), genres, args)

    def calc_user_profile(self, user_id):
        avg_genres = self.calc_avg_for_all()
        avg_for_user = self.get_avg_for_user(user_id)
        user_profile, ar = lab04.user_dif(avg_for_user, avg_genres)
        return user_profile

    def get_avg_for_user(self, args):
        avg = wtiproj06_cassandra_client.get_user_rating(self.session, self.keyspace, self.user_avg_table, args)
        return avg.current_rows[0]
