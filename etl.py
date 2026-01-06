import os
import sqlite3
import pandas as pd

if __name__ == "__main__":
    # Load data from the SQLite database
    with sqlite3.connect("data/l2m.db") as conn:
        calls = pd.read_sql("SELECT * FROM calls", conn)
        urls = pd.read_sql("SELECT * FROM urls", conn)
        urls["pdf"] = urls.l2m_url.apply(lambda x: os.path.basename(x))
        refs = pd.read_sql("SELECT * FROM refs", conn)

    # Aggregate call review decisions by game and summarize the total incorrect and correct calls per referee
    game = (
        urls.merge(calls, on="pdf")
        .groupby(["gameid", "review_decision"])
        .size()
        .unstack(level=1)
        .fillna(0)
        .iloc[:, 1:]  # Skip calls that have no review decision
    )

    game.columns = ["CC1", "CC2", "CNC1", "CNC2", "IC1", "IC2", "INC1", "INC2"]
    game["correct"] = game.CC1 + game.CC2 + game.CNC1 + game.CNC2
    game["incorrect"] = game.IC1 + game.IC2 + game.INC1 + game.INC2

    # Merge with referee information and summarize total correct and incorrect calls per referee
    ref_calls = (
        refs.merge(game, left_on="gameid", right_index=True)
        .groupby(["first_name", "last_name"])
        .agg(
            correct=("correct", "sum"),
            incorrect=("incorrect", "sum"),
            games=("gameid", "nunique"),
        )
    )
    ref_calls["correct_pg"] = ref_calls.correct / ref_calls.games
    ref_calls["incorrect_pg"] = ref_calls.incorrect / ref_calls.games
    ref_calls["accuracy"] = ref_calls.correct / (
        ref_calls.correct + ref_calls.incorrect
    )
    ref_calls.sort_values("incorrect_pg", ascending=False, inplace=True)
    ref_calls.to_csv("output/referee_call_summary.csv")
