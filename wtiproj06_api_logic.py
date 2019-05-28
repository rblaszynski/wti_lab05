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
                                               rate['rating'], rate['genreAction'],
                                               rate['genreAdventure'], rate['genreAnimation'],
                                               rate['genreChildren'],
                                               rate['genreComedy'],
                                               rate['genreCrime'], rate['genreDocumentary'], rate['genreDrama'],
                                               rate['genreFantasy'],
                                               rate['genreFilm-Noir'], rate['genreHorror'], rate['genreIMAX'],
                                               rate['genreMusical'],
                                               rate['genreMystery'], rate['genreRomance'], rate['genreSci-Fi'],
                                               rate['genreShort'],
                                               rate['genreThriller'], rate['genreWar'], rate['genreWestern'])

    def delete_ratings(self):
        wtiproj06_cassandra_client.delete_ratings(self.session, self.keyspace, self.rating_table)

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
        if len(avg.current_rows) != 0:
            avg.current_rows[0].pop('uuid', None)
            return avg.current_rows[0]
        else:
            return []

    def calc_avg_for_user(self, args):
        return lab04.calc_avg_for_user(self.get_all_ratings(), genres, args)

    def calc_user_profile(self, user_id):
        avg_genres = self.calc_avg_for_all()
        avg_for_user = self.get_avg_for_user(user_id)
        user_profile, ar = lab04.user_dif(avg_for_user, avg_genres)
        return user_profile

    def get_avg_for_user(self, args):
        avg = wtiproj06_cassandra_client.get_user_rating(self.session, self.keyspace, self.user_avg_table, args)
        if len(avg.current_rows) != 0:
            return avg.current_rows[0]
        else:
            return []

    def get_user_ratings(self, args):
        ratings = wtiproj06_cassandra_client.get_user_rating(self.session, self.keyspace, self.rating_table, args)
        if len(ratings.current_rows) != 0:
            ratings.current_rows[0].pop('uuid', None)
            return ratings.current_rows[0]
        else:
            return []
