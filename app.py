from flask import Flask, request, jsonify
import pymysql.cursors
from flask_cors import CORS

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app)


# Database connection
def db_connection():
    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='',
                           db='ipl',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn


# ----------------   Assignment Questions  -------------------- #

# Top Team of all the season
@app.route('/top-team-allseasons')
def top_team_allseasons():
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT DISTINCT(season) FROM matches ORDER BY season")
            teams = cursor.fetchall()
            team = [x['season'] for x in teams]
            win_teams = []
            for season in team:
                x = cursor.execute("SELECT winner as team, count(winner) as total_win "
                                   "FROM (SELECT winner from `matches` WHERE season=(%s) ORDER BY winner DESC) "
                                   "AS team GROUP BY winner ORDER BY count(winner) DESC LIMIT 4", season)
                teams = cursor.fetchall()
                winners = {
                    'season': season,
                    'teams': teams
                }
                win_teams.append(winners)
                
            return jsonify(win_teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# Api to get the list of seasons played
@app.route('/seasons', methods=["GET"])
def total_seasons():
    try:
        if request.method == 'GET':
            conn = db_connection()
            cursor = conn.cursor()
            if conn:
                x = cursor.execute("SELECT DISTINCT(season) FROM matches ORDER BY season")
                seasons = cursor.fetchall()
                match = [x['season'] for x in seasons]
                return jsonify(match)
            else:
                return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)


# Top Team of the season
@app.route('/top-team/<season>')
def top_team(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT winner as team, count(winner) as total_win "
                               "FROM (SELECT winner from `matches` WHERE season=(%s) ORDER BY winner DESC) "
                               "AS team GROUP BY winner ORDER BY count(winner) DESC LIMIT 4", season)
            teams = cursor.fetchall()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)


# Team win the max no. of tosses
@app.route('/toss-winner/<season>')
def max_toss_winner(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT toss_winner as team, count(toss_winner) as total_toss_win "
                               "FROM (SELECT toss_winner from `matches` WHERE season=(%s) ORDER BY toss_winner DESC) "
                               "AS team GROUP BY toss_winner ORDER BY count(toss_winner) DESC", season)
            teams = cursor.fetchone()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# Player of the match award
@app.route('/player_of_match/<season>')
def player_of_match(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT player_of_match as player, count(player_of_match) as total_awards "
                               "FROM (SELECT player_of_match from `matches` where season=(%s) "
                               "ORDER BY player_of_match DESC) "
                               "AS team GROUP BY player_of_match ORDER BY count(player_of_match) DESC", season)
            teams = cursor.fetchone()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# Team won maximum no. of matches
@app.route('/won-max-matches/<season>')
def won_max_matches(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT winner as team, count(winner) as total_win "
                               "FROM (SELECT winner from `matches` WHERE season=(%s) ORDER BY winner DESC) "
                               "AS team GROUP BY winner ORDER BY count(winner) DESC LIMIT 4", season)
            max_win = cursor.fetchone()
            return jsonify(max_win)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)


# Location has the max no. of win by top teams
@app.route('/wining-location/<season>')
def wining_location(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT COUNT(winner) as win, winner as team, city as location "
                               "FROM `matches` where season=(%s) group by winner order by win DESC limit 4", season)
            location = cursor.fetchone()
            return jsonify(location)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# % of team to decide to bat when they win the toss
@app.route('/electing-bat-bowl/<season>')
def electing_first(total_seasons):
    pass


# Location hosted most number of matches and win and loss % for the season
@app.route('/hosted-max-matches/<season>')
def hosted_max_matches(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT city, count(city) as matches "
                               "FROM (SELECT city from `matches` WHERE season=(%s) ORDER BY city DESC) "
                               "AS team GROUP BY city ORDER BY count(city) DESC", season)
            teams = cursor.fetchone()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# Team won by highest run margin
@app.route('/highest-run-margin/<season>')
def highest_run_margine(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT winner as team, win_by_runs "
                               "FROM (SELECT DISTINCT winner,win_by_runs from `matches` WHERE season=(%s) "
                               "ORDER BY win_by_runs DESC) "
                               "AS team GROUP BY winner ORDER BY win_by_runs DESC", season)
            teams = cursor.fetchone()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)

# ----------    Bonus Questions   --------------#


# Team won by highest number of wicket
@app.route('/win-by-wickets/<season>')
def win_by_wickets(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT winner as team, win_by_wickets "
                               "FROM (SELECT DISTINCT winner,win_by_wickets from `matches` WHERE season=(%s)"
                               " ORDER BY win_by_wickets DESC) "
                               "AS team GROUP BY winner ORDER BY win_by_wickets DESC", season)
            teams = cursor.fetchone()
            return jsonify(teams)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)


# No of toss and match win by the team 
@app.route('/toss-win-match/<season>')
def match_and_match_win(season):
    try:
        conn = db_connection()
        cursor = conn.cursor()
        if conn:
            x = cursor.execute("SELECT toss_winner as team, count(toss_winner) as select_bat "
                               "FROM (SELECT toss_winner from `matches` WHERE season=(%s) AND toss_decision='bat' "
                               "ORDER BY toss_winner DESC) "
                               "AS team GROUP BY toss_winner ORDER BY count(toss_winner) DESC", season)
            total = cursor.fetchall()

            return jsonify(total)
        else:
            return jsonify({'message': 'Database connection error!!'})
    except Exception as e:
        return str(e)


# Highest run scored by the batman in a season
@app.route('/higest-run/<season>')
def highest_run_scored(season):
    pass

# Most no. of catches by a fielder in a match for the season
@app.route('/max-catches/<season>')
def max_catches(season):
    pass


if __name__ == '__main__':
    app.run(debug=True)