from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def create_keyspace(session, keyspace):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)


def create_rating_table(session, keyspace, table):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
        uuid uuid,
        userID int,
        movieID int,
        rating float,
        genreAction float,
        genreAdventure float,
        genreAnimation float,
        genreChildren float,
        genreComedy float,
        genreCrime float,
        genreDocumentary float,
        genreDrama float,
        genreFantasy float,
        genreFilmNoir float,
        genreHorror float,
        genreIMAX float,
        genreMusical float,
        genreMystery float,
        genreRomance float,
        genreSciFi float,
        genreShort float,
        genreThriller float,
        genreWar float,
        genreWestern float,
        PRIMARY KEY(uuid))
        """)


def create_user_avg_table(session, keyspace, table):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
        userID int,
        genreAction float,
        genreAdventure float,
        genreAnimation float,
        genreChildren float,
        genreComedy float,
        genreCrime float,
        genreDocumentary float,
        genreDrama float,
        genreFantasy float,
        genreFilmNoir float,
        genreHorror float,
        genreIMAX float,
        genreMusical float,
        genreMystery float,
        genreRomance float,
        genreSciFi float,
        genreShort float,
        genreThriller float,
        genreWar float,
        genreWestern float,
        PRIMARY KEY(userID))
        """)


def create_all_avg_table(session, keyspace, table):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
            uuid uuid,
            genreAction float,
            genreAdventure float,
            genreAnimation float,
            genreChildren float,
            genreComedy float,
            genreCrime float,
            genreDocumentary float,
            genreDrama float,
            genreFantasy float,
            genreFilmNoir float,
            genreHorror float,
            genreIMAX float,
            genreMusical float,
            genreMystery float,
            genreRomance float,
            genreSciFi float,
            genreShort float,
            genreThriller float,
            genreWar float,
            genreWestern float,
            PRIMARY KEY(uuid))
            """)


def push_rating(session, keyspace, table,
                user_id,
                movie_id,
                rating,
                genre_action,
                genre_adventure,
                genre_animation,
                genre_children,
                genre_comedy,
                genre_crime,
                genre_documentary,
                genre_drama,
                genre_fantasy,
                genre_film_noir,
                genre_horror,
                genre_imax,
                genre_musical,
                genre_mystery,
                genre_romance,
                genre_scifi,
                genre_short,
                genre_thriller,
                genre_war,
                genre_western):
    session.execute("INSERT INTO " + keyspace + "." + table +
                    "(uuid, userID, movieID, rating, genreAction, genreAdventure, genreAnimation, genreChildren, genreComedy, genreCrime, genreDocumentary,"
                    "genreDrama, genreFantasy, genreFilmNoir, genreHorror, genreIMAX, genreMusical, genreMystery, genreRomance,"
                    "genreSciFi, genreShort, genreThriller, genreWar, genreWestern) VALUES (uuid(), "
                    "%(userID)s, %(movieID)s, %(rating)s, "
                    "%(genreAction)s, %(genreAdventure)s, "
                    "%(genreAnimation)s, %(genreChildren)s, "
                    "%(genreComedy)s, %(genreCrime)s, "
                    "%(genreDocumentary)s, %(genreDrama)s, "
                    "%(genreFantasy)s, %(genreFilmNoir)s, "
                    "%(genreHorror)s, %(genreIMAX)s, "
                    "%(genreMusical)s, %(genreMystery)s, "
                    "%(genreRomance)s, %(genreSciFi)s, "
                    "%(genreShort)s, %(genreThriller)s, "
                    "%(genreWar)s, %(genreWestern)s)"
                    , {
                        'userID': user_id,
                        'movieID': movie_id,
                        'rating': rating,
                        'genreAction': genre_action,
                        'genreAdventure': genre_adventure,
                        'genreAnimation': genre_animation,
                        'genreChildren': genre_children,
                        'genreComedy': genre_comedy,
                        'genreCrime': genre_crime,
                        'genreDocumentary': genre_documentary,
                        'genreDrama': genre_drama,
                        'genreFantasy': genre_fantasy,
                        'genreFilmNoir': genre_film_noir,
                        'genreHorror': genre_horror,
                        'genreIMAX': genre_imax,
                        'genreMusical': genre_musical,
                        'genreMystery': genre_mystery,
                        'genreRomance': genre_romance,
                        'genreSciFi': genre_scifi,
                        'genreShort': genre_short,
                        'genreThriller': genre_thriller,
                        'genreWar': genre_war,
                        'genreWestern': genre_western,
                    })


def add_avg(session, keyspace, table,
            user_id,
            genre_action,
            genre_adventure,
            genre_animation,
            genre_children,
            genre_comedy,
            genre_crime,
            genre_documentary,
            genre_drama,
            genre_fantasy,
            genre_film_noir,
            genre_horror,
            genre_imax,
            genre_musical,
            genre_mystery,
            genre_romance,
            genre_scifi,
            genre_short,
            genre_thriller,
            genre_war,
            genre_western):
    session.execute("INSERT INTO " + keyspace + "." + table +
                    "(userID, genreAction, genreAdventure, genreAnimation, genreChildren, genreComedy, genreCrime, genreDocumentary,"
                    "genreDrama, genreFantasy, genreFilmNoir, genreHorror, genreIMAX, genreMusical, genreMystery, genreRomance,"
                    "genreSciFi, genreShort, genreThriller, genreWar, genreWestern) VALUES ("
                    "%(userID)s,"
                    "%(genreAction)s, %(genreAdventure)s, "
                    "%(genreAnimation)s, %(genreChildren)s, "
                    "%(genreComedy)s, %(genreCrime)s, "
                    "%(genreDocumentary)s, %(genreDrama)s, "
                    "%(genreFantasy)s, %(genreFilmNoir)s, "
                    "%(genreHorror)s, %(genreIMAX)s, "
                    "%(genreMusical)s, %(genreMystery)s, "
                    "%(genreRomance)s, %(genreSciFi)s, "
                    "%(genreShort)s, %(genreThriller)s, "
                    "%(genreWar)s, %(genreWestern)s)"
                    , {
                        'userID': user_id,
                        'genreAction': genre_action,
                        'genreAdventure': genre_adventure,
                        'genreAnimation': genre_animation,
                        'genreChildren': genre_children,
                        'genreComedy': genre_comedy,
                        'genreCrime': genre_crime,
                        'genreDocumentary': genre_documentary,
                        'genreDrama': genre_drama,
                        'genreFantasy': genre_fantasy,
                        'genreFilmNoir': genre_film_noir,
                        'genreHorror': genre_horror,
                        'genreIMAX': genre_imax,
                        'genreMusical': genre_musical,
                        'genreMystery': genre_mystery,
                        'genreRomance': genre_romance,
                        'genreSciFi': genre_scifi,
                        'genreShort': genre_short,
                        'genreThriller': genre_thriller,
                        'genreWar': genre_war,
                        'genreWestern': genre_western,
                    })


def add_all_avg(session, keyspace, table,
                genre_action,
                genre_adventure,
                genre_animation,
                genre_children,
                genre_comedy,
                genre_crime,
                genre_documentary,
                genre_drama,
                genre_fantasy,
                genre_film_noir,
                genre_horror,
                genre_imax,
                genre_musical,
                genre_mystery,
                genre_romance,
                genre_scifi,
                genre_short,
                genre_thriller,
                genre_war,
                genre_western):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")
    session.execute("INSERT INTO " + keyspace + "." + table +
                    "(uuid, genreaction, genreadventure, genreanimation, genrechildren, genrecomedy, genrecrime, genredocumentary,"
                    "genredrama, genrefantasy, genrefilmnoir, genrehorror, genreimax, genremusical, genremystery, genreromance,"
                    "genrescifi, genreshort, genrethriller, genrewar, genrewestern) VALUES (uuid(),"
                    "%(genreaction)s, %(genreadventure)s, "
                    "%(genreanimation)s, %(genrechildren)s, "
                    "%(genrecomedy)s, %(genrecrime)s, "
                    "%(genredocumentary)s, %(genredrama)s, "
                    "%(genrefantasy)s, %(genrefilmnoir)s, "
                    "%(genrehorror)s, %(genreimax)s, "
                    "%(genremusical)s, %(genremystery)s, "
                    "%(genreromance)s, %(genrescifi)s, "
                    "%(genreshort)s, %(genrethriller)s, "
                    "%(genrewar)s, %(genrewestern)s)"
                    , {
                        'genreaction': genre_action,
                        'genreadventure': genre_adventure,
                        'genreanimation': genre_animation,
                        'genrechildren': genre_children,
                        'genrecomedy': genre_comedy,
                        'genrecrime': genre_crime,
                        'genredocumentary': genre_documentary,
                        'genredrama': genre_drama,
                        'genrefantasy': genre_fantasy,
                        'genrefilmnoir': genre_film_noir,
                        'genrehorror': genre_horror,
                        'genreimax': genre_imax,
                        'genremusical': genre_musical,
                        'genremystery': genre_mystery,
                        'genreromance': genre_romance,
                        'genrescifi': genre_scifi,
                        'genreshort': genre_short,
                        'genrethriller': genre_thriller,
                        'genrewar': genre_war,
                        'genrewestern': genre_western
                    })


def get_ratings(session, keyspace, table):
    rows = session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    return rows


def get_user_rating(session, keyspace, table, user_id):
    return session.execute("SELECT * FROM " + keyspace + "." + table + " WHERE userid =" + user_id + " ALLOW FILTERING;")


def delete_ratings(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")
