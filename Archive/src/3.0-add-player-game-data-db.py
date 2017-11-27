import os
import sys
import sqlite3
import pandas as pd
from fuzzywuzzy import process

def add_csv_to_db(conn):
    """Add additional csvs to db.
    conn - sqlite3 connection."""
    csv_path = "../data/csv/"
    for f in os.listdir(csv_path):
        if f.startswith('players'):
            try:
                print f
                df = pd.read_csv(os.path.join(csv_path, f))
                df.Player = df.Player.str.split("\\").str[0]
                df.replace({"Tm":{"BRK": "BKN", "CHO": "CHA", "PHO": "PHX"}}, inplace=True)
                df.to_sql(f[:13], conn, index=False)
            except:
                pass
        else:
            try:
                print f
                df = pd.read_csv(os.path.join(csv_path, f))
                df.to_sql(f[:13], conn, index=False)
            except:
                pass

def players_dict(c):
    """Returns dictionary of {year:{team:{players}}}.
    c - sqlite3 cursor."""
    players_year = {}
    for f in os.listdir("../data/csv/"):
        if f.startswith('players'):
            players = c.execute("""SELECT Tm, Player FROM '{}'""".format(f[:13])).fetchall()
            year = {}
            for player in players:
                if player[0] not in year:
                    year[player[0]] = []
                year[player[0]].append(player[1])
            players_year[f[8:13]] = year
    return players_year

def find_players_team(calls, reports):
    """Populate players' teams in calls table.
    calls - df of calls table.
    reports - df of reports table."""
    df = pd.merge(calls, reports, how='left', on='report')
    n = len(df)
    idx = 0
    for row in df.iterrows():
        idx += 1
        year = row[1].season[2:7]
        away = players[year][row[1].away]
        home = players[year][row[1].home]

        if row[1].committing not in ['', ',']:
            _, c_away = process.extractOne(row[1].committing, away)
            _, c_home = process.extractOne(row[1].committing, home)
        else:
            c_away = 0
            c_home = 0

        if row[1].disadvantaged not in ['', ',']:
            _, d_away = process.extractOne(row[1].disadvantaged, away)
            _, d_home = process.extractOne(row[1].disadvantaged, home)
        else:
            d_away = 0
            d_home = 0

        if max(c_away, c_home) < 50:
            row[1].committing_team = "NaN"
        elif c_away > c_home:
            row[1].committing_team = row[1].away
        else:
            row[1].committing_team = row[1].home

        if max(d_away, d_home) < 50:
            row[1].disadvantaged_team = "NaN"
        elif d_away > d_home:
            row[1].disadvantaged_team = row[1].away
        else:
            row[1].disadvantaged_team = row[1].home

        sys.stdout.write("\r({0}/{1}) {2:.2f}% Complete".format(idx, n, (float(idx) / n) * 100))
        sys.stdout.flush()
    return df[calls.columns]

def find_winning_team(reports, gms):
    """Populate teams' scores and winner in reports table.
    reports - df of reports table.
    gms - df of gms table."""
    tms = {'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS',
           'Brooklyn Nets': 'BKN', 'Charlotte Hornets': 'CHA',
           'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
           'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN',
           'Detroit Pistons': 'DET', 'Golden State Warriors': 'GSW',
           'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
           'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL',
           'Memphis Grizzlies': 'MEM', 'Miami Heat': 'MIA',
           'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
           'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK',
           'Oklahoma City Thunder': 'OKC', 'Orlando Magic': 'ORL',
           'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
           'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC',
           'San Antonio Spurs': 'SAS', 'Toronto Raptors': 'TOR',
           'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'}
    gms.columns = ["Date", "Start", "Visitor", "V_PTS", "Home", "H_PTS",
                   "Boxscore", "OT", "Notes"]
    gms.Date = pd.to_datetime(gms["Date"], format="%a %b %d %Y")
    gms.V_PTS = pd.to_numeric(gms.V_PTS)
    gms.H_PTS = pd.to_numeric(gms.H_PTS)
    gms.replace({'Visitor': tms, 'Home': tms}, inplace=True)

    df = pd.merge(reports, gms, how="left", left_on=['away', 'home', 'date'], right_on=['Visitor', 'Home', 'Date'])
    df.away_score = df.V_PTS
    df.home_score = df.H_PTS

    def winner(row):
        if row.away_score > row.home_score:
            return row.away
        else:
            return row.home

    df.winner = df.apply(winner, axis=1)
    return df[reports.columns]

if __name__ == "__main__":
    conn = sqlite3.connect("../data/db/NBA-L2M.db")
    c = conn.cursor()
    add_csv_to_db(conn)
    players = players_dict(c)

    reports = pd.read_sql("SELECT * FROM reports", conn, parse_dates=['date']).replace({'away':{'PHO': 'PHX'}})
    calls = pd.read_sql("SELECT * FROM calls", conn)
    gms = pd.read_sql("SELECT * FROM 'scheduled-gms' WHERE Date != 'Date'", conn) # Accidently wrote a second header in the .csv

    calls_ = find_players_team(calls, reports)
    reports_ = find_winning_team(reports, gms)

    calls_.to_sql('calls', conn, if_exists='replace', index=False)
    reports_.to_sql('reports', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()