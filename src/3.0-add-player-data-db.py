import os
import sys
import sqlite3
import pandas as pd
from fuzzywuzzy import process

def add_csv_to_db(conn):
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

def find_players_team(df):
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

if __name__ == "__main__":
    conn = sqlite3.connect("../data/db/NBA-L2M.db")
    c = conn.cursor()
    add_csv_to_db(conn)
    players = players_dict(c)

    reports = pd.read_sql("SELECT * FROM reports", conn).replace({'away':{'PHO': 'PHX'}})
    calls = pd.read_sql("SELECT * FROM calls", conn)
    df = pd.merge(calls, reports, how='left', on='report')
    find_players_team(df)
    df[calls.columns].to_sql('calls', conn, if_exists='replace', index=False)
    df[reports.columns].to_sql('reports', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()
