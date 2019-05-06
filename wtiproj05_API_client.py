import requests
import wtiproj03_ETL
import lab04

data_frame, _ = wtiproj03_ETL.get_df()
json_list = lab04.df_to_json(data_frame)

client = "http://localhost:8888"


def test_post(rating):
    print("POST /api/rating")
    print(requests.post(client + '/api/rating', json=rating).text)


def test_get_ratings(user_id):
    u_id = str(user_id)
    print("GET /api/rating/" + u_id)
    print(requests.get(client + '/api/rating/' + u_id).text)


def test_delete():
    print("DELETE /api/ratings")
    print(requests.delete(client + '/api/ratings').text)


def test_all_avg():
    print("GET /api/avg-genre-ratings/all-users")
    print(requests.get(client + '/api/avg-genre-ratings/all-users').text)


def test_user_avg(user_id):
    u_id = str(user_id)
    print("GET /api/avg-genre-ratings/" + u_id)
    print(requests.get(client + '/api/avg-genre-ratings/' + u_id).text)


def test_user_profile(user_id):
    u_id = str(user_id)
    print("GET /api/user-profile/" + u_id)
    print(requests.get(client + '/api/user-profile/' + u_id).text)


def add_ratings():
    for rating in json_list:
        test_post(rating)


if __name__ == '__main__':
    print('TEST API')
