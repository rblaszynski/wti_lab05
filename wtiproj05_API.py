from flask import Flask, jsonify, request
import wtiproj05_api_logic as api_logic

app = Flask(__name__)


@app.route('/')
def index():
    return "Hello, World!"


@app.route('/api/rating/<int:user_id>', methods=['GET'])
def get_rating_by_user_id(user_id):
    return api_logic.get_user_ratings(user_id)


@app.route('/api/rating', methods=['POST'])
def add_rating():
    result = request.get_json()
    api_logic.add_rating(result)
    return jsonify(result), 200, {"Content-Type": "application/json"}


@app.route('/api/ratings', methods=['DELETE'])
def del_ratings():
    api_logic.delete_ratings()
    return jsonify({'deleted'}), 200, {"Content-Type": "application/json"}


@app.route('/api/avg-genre-ratings/all-users', methods=["GET"])
def avg_all_users():
    avg = api_logic.get_avg_for_all()
    return jsonify(avg), 200, {"Content-Type": "application/json"}


@app.route('/api/avg-genre-ratings/<int:user_id>', methods=["GET"])
def avg_user(user_id):
    avg_for_user = api_logic.get_avg_for_user(user_id)
    return jsonify(avg_for_user), 200, {"Content-Type": "application/json"}


if __name__ == '__main__':
    app.run(debug=True, port=8888)
