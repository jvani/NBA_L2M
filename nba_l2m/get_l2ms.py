from __future__ import print_function
import os
import re
import sys
import time
import urllib
import nba_py
import sqlite3
import pdfquery
import pandas as pd
from nba_py import game
from datetime import datetime
from bs4 import BeautifulSoup
from fuzzywuzzy import process


def _url_metadata(l2m_url):
    """Parse data included in l2m url.
    Args:
        l2m_url (str) - url to l2m .pdf
    Returns:
        (list) - [l2m_url, away team abbr, home team abbr, datetime]
    """

    # -- Split url basename on '@' or '-'.
    bname = re.split("-|@", os.path.basename(l2m_url).rstrip(".pdf"))
    # -- Pull away team abbr and home team abbr from split basename.
    away, home = bname[1], bname[2]
    # -- Pull and format date from split basename.
    if len(bname[5]) == 4:
        date = datetime(2000 + int(bname[5][-2:]), int(bname[3]), int(bname[4]))
    elif len(bname[5]) == 3:
        date = datetime(2000 + int(bname[5][:-1]), int(bname[3]), int(bname[4]))
    elif len(bname[5]) == 2:
        date = datetime(2000 + int(bname[5]), int(bname[3]), int(bname[4]))
    else:
        date = datetime(9999, int(bname[3]), int(bname[4]))
    # -- PHO v. PHX exception
    if away == "PHO":
        away = "PHX"
    if home == "PHO":
        home = "PHX"

    # -- Team abbr dictionary.
    TEAMS = nba_py.constants.TEAMS
    team_dict = {k: int(TEAMS[k]["id"]) for k in TEAMS.keys()}

    # -- Use nba_py to get results for a given day.
    api_call = nba_py.Scoreboard(date.month, date.day, date.year).json
    # -- Filter results to the correct game.
    game_res = api_call["resultSets"][0]["rowSet"]
    game = filter(lambda x: team_dict[away] == x[7], game_res)[0]
    score_res = api_call["resultSets"][1]["rowSet"]
    # -- Grab gameid, season, and scores.
    gameid, season = game[2], game[8]
    away_score = filter(lambda x: team_dict[away] == x[3], score_res)[0][21]
    home_score = filter(lambda x: team_dict[home] == x[3], score_res)[0][21]

    # print("{}, {}, {}, {}, {}, {}, {}".format(
    #     gameid, season, date, away, away_score, home, home_score))
    return [gameid, season, date, away, away_score, home, home_score, l2m_url]


def get_l2m_links(url, db=os.path.join("..", "data", "l2m.db")):
    """Get l2m links from archive and add to db if there are new links.
    Args:
        url (str) - l2m archive page.
        db (str) - path to sqlite db.
    Returns:
        df (dataframe) - l2m links.
    """

    tstart = time.time()
    print("L2M: Scraping pdf links.                                           ")
    print("L2M: {}".format(url), end="\n")
    sys.stdout.flush()

    # -- Scrape relevant urls.
    soup = BeautifulSoup(urllib.urlopen(url).read(), "html.parser")
    links = filter(lambda link: link.has_attr("href"), soup.find_all("a"))
    urls = filter(lambda url: url.endswith("pdf"), [
        link["href"] for link in links])
    l2m_urls = filter(re.compile(r"L2M").search, urls)

    # -- Load existing urls.
    with sqlite3.connect(db) as conn:
        try:
            exist = pd.read_sql("SELECT * FROM urls", conn).l2m_url.values
        except:
            exist = []

    # -- Select urls that are not in the db.
    new_urls = list(set(l2m_urls) - set(exist))

    # -- For each url parse data and retrieve data and print status.
    ll = len(new_urls)
    metadata = []
    for idx, new_url in enumerate(new_urls):
        try:
            data = _url_metadata(new_url)
            metadata.append(data)
            print("L2M: Parsing {} ({}/{})                                   " \
                .format(os.path.basename(data[-1]), idx + 1, ll))
            sys.stdout.flush()
            time.sleep(1)
        except:
            print("L2M: Error parsing {} ({}/{})                             " \
                .format(os.path.basename(new_url), idx + 1, ll))
            sys.stdout.flush()

    # -- Load data to dataframe.
    cols = ["gameid", "season", "date", "away", "away_score", "home",
            "home_score", "l2m_url"]
    df = pd.DataFrame(metadata, columns=cols).set_index("gameid")

    print("L2M: Saving to {}.".format(db))
    sys.stdout.flush()
    # -- Write dataframe to db (overwrite existing table).
    with sqlite3.connect(db) as conn:
        df.to_sql("urls", conn, if_exists="append")

    print("L2M: Complete ({:.2f}s elapsed)                                   " \
        .format(time.time() - tstart))

    return df


def _scrape_l2m(pdf_path, player_dict, db=os.path.join("..", "data", "l2m.db")):
    """Scrape data from pdf and write to db.
    Args:
        pdf_path (str) - path to pdf to scrape.
        player_dict (dict) - {player: team} dict.
        db (str) - path to database.
    """
    # -- Load an L2M.
    pdf_name = os.path.basename(pdf_path)
    pdf = pdfquery.PDFQuery(pdf_path)
    pdf.load()

    # -- Saved searched.
    searches = {
        "yy": "LTPage[pageid='{}'] LTTextLineHorizontal:contains('Video')",
        "peri": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('20, {}, 60, {}')",
        "time": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('60, {}, 100, {}')",
        "call": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('100, {}, 220, {}')",
        "comm": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('220, {}, 360, {}')",
        "disa": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('360, {}, 500, {}')",
        "deci": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('500, {}, 560, {}')"
    }

    # -- Saved data.
    data = []
    # -- Flip through each page in the pdf.
    for pp in range(pdf.doc.catalog["Pages"].resolve()["Count"]):
        pg = pp + 1
        # -- Find all lines.
        y0s = [float(y.attrib["y0"]) for y in pdf.pq(searches["yy"].format(pg))]
        # -- For all lines pull relevant data.
        for y0 in y0s:
            try:
                dy = 10
                peri = pdf.pq(searches["peri"].format(pg, y0 - dy, y0 + dy)).text()
                time = pdf.pq(searches["time"].format(pg, y0 - dy, y0 + dy)).text()
                call = pdf.pq(searches["call"].format(pg, y0 - dy, y0 + dy)).text()
                comm = pdf.pq(searches["comm"].format(pg, y0 - dy, y0 + dy)).text()
                disa = pdf.pq(searches["disa"].format(pg, y0 - dy, y0 + dy)).text()
                deci = pdf.pq(searches["deci"].format(pg, y0 - dy, y0 + dy)).text()

                if peri.startswith("Q"): # -- If it's a valid line.
                    c_tm = _find_players_team(comm, player_dict)
                    d_tm = _find_players_team(disa, player_dict)
                    tms = set(player_dict.values())
                    # -- Replace "ERR" if opposite team successfully populated.
                    if c_tm == "ERR" and d_tm in tms:
                        c_tm = filter(lambda x: x != d_tm, tms)[0]
                    if d_tm == "ERR" and c_tm in tms:
                        d_tm = filter(lambda x: x != c_tm, tms)[0]

                    data.append([pdf_name, peri, time, call, comm, c_tm, disa, d_tm, deci])
            except:
                # print("L2M: {}".format([pdf_name, peri, time, call, comm, disa, deci]))
                print("L2M: Error at {} in {}                                " \
                    .format(y0, pdf_name))

    # -- Load data into dataframe.
    cols = ["pdf", "period", "time", "call_type", "committing_player",
            "committing_team", "disadvantaged_player", "disadvantaged_team",
            "review_decision"]
    df = pd.DataFrame(data, columns=cols)

    # -- Append to database.
    with sqlite3.connect(db) as conn:
        df.to_sql("calls", conn, if_exists="append")


def _find_players_team(player, player_dict):
    """Use fuzzy string matching to find a player's team.
    Args:
        player (str) - Player's name.
        player_dict (dict) - Dict of {players: team}.
    Returns:
        tm (str) - Player's team.
    """

    if len(player) == 0: # -- If there isn't a player name return empty string.
        tm = ""
    else:
        # -- Find fuzzy match.
        fplayer = process.extractOne(player, player_dict.keys())

        if fplayer[1] > 80: # -- If there's a good match use player_dict.
            tm = player_dict[fplayer[0]]
        else: # -- Else fill with "ERR".
            tm = "ERR"

    return tm


def download_pdfs(pdf_folder=os.path.join("..", "pdfs"),
        db=os.path.join("..", "data", "l2m.db")):
    """Download new l2ms.
    Args:
        pdf_folder (str) - output folder to save pdfs.
        db (str) - path to sqlite db.
    """

    tstart = time.time()
    print("L2M: Loading urls table from {}.                        ".format(db))
    sys.stdout.flush()

    # -- Load urls table to dataframe.
    with sqlite3.connect(db) as conn:
        df = pd.read_sql("SELECT * FROM urls", conn)

    # -- Get list of scraped pdfs.
    try:
        with sqlite3.connect(db) as conn:
            pdfs = pd.read_sql("SELECT * FROM calls", conn).pdf.unique()
    except:
        pdfs = []

    # -- Create folder for pdfs.
    if not os.path.isdir(pdf_folder):
        os.mkdir(pdf_folder)

    # -- Create bool mask for l2ms that have not been downloaded.
    bool_mask = map(lambda x: os.path.basename(x) not in pdfs, df.l2m_url)

    # -- Download all l2ms that have not been downloaded.
    dfsize = len(df[bool_mask])
    for idx, (_, row) in enumerate(df[bool_mask].iterrows()):
        try:
            # -- Define path to save pdf.
            pdf_path = os.path.join(pdf_folder, os.path.basename(row.l2m_url))
            bname = os.path.basename(row.l2m_url)
            if not os.path.isfile(pdf_path):
                # -- Download pdf.
                urllib.urlretrieve(row.l2m_url, pdf_path)
                print("L2M: Downloading {} ({}/{})                           " \
                    .format(bname, int(idx) + 1, dfsize), end="\r")
                sys.stdout.flush()

        except:
            print("L2M: ERROR Downloading {} ({}/{})                         " \
                .format(os.path.basename(row.l2m_url), int(idx) + 1, dfsize))
            pass

        try:
            # -- Return {player: team} dictionary.
            player_dict = game.Boxscore(row.gameid) \
                .player_stats()[["PLAYER_NAME", "TEAM_ABBREVIATION"]] \
                .set_index("PLAYER_NAME")["TEAM_ABBREVIATION"].to_dict()
            print("L2M: Getting player dictionary {} ({}/{})                 " \
                .format(bname, int(idx) + 1, dfsize), end="\r")
            sys.stdout.flush()

        except:
            print("L2M: ERROR Getting player dictionary {} ({}/{})           " \
                .format(os.path.basename(row.l2m_url), int(idx) + 1, dfsize))
            print("L2M: GameID: {}                                           " \
                .format(row.gameid))
            pass

        try:
            # -- Scrape pdf.
            _scrape_l2m(pdf_path, player_dict)
            print("L2M: Downloaded & Scraped {} ({}/{})                      " \
                .format(bname, int(idx) + 1, dfsize))
            sys.stdout.flush()

        except:
            print("L2M: ERROR Scraping {} ({}/{})                                " \
                .format(os.path.basename(row.l2m_url), int(idx) + 1, dfsize))
            pass

    print("L2M: Complete ({:.2f}s elapsed)                                   " \
        .format(time.time() - tstart))
    sys.stdout.flush()


def update_refs(db=os.path.join("..", "data", "l2m.db")):
    """Get referees for each l2m.
    Args:
        db (str) - path to l2m.db.
    """

    tstart = time.time()
    print("L2M: Updating referee table.                                       ")
    sys.stdout.flush()

    # -- Load existing data.
    try:
        with sqlite3.connect(db) as conn:
            refs = pd.read_sql("SELECT * FROM refs", conn)
    except:
        refs = []

    with sqlite3.connect(db) as conn:
        urls = pd.read_sql("SELECT * FROM urls", conn)

    # -- Get list of gameids that do not have corresponding ref data.
    if len(refs) > 0:
        gameids = list(set(urls.gameid.values) - set(refs.gameid.values))
    else:
        gameids = list(set(urls.gameid.values))

    data = []
    ngames = len(urls)
    # -- For each gameid get referees.
    for idx, gameid in enumerate(gameids):
        refs = game.BoxscoreSummary(gameid).json["resultSets"][2]["rowSet"]
        refs = [[gameid] + ref for ref in refs]
        data = data + refs
        print("L2M: Finding refs for game {} ({}/{})                         " \
            .format(gameid, idx, ngames))
        sys.stdout.flush()

    # -- Put data in dataframe.
    cols = ["gameid", "refid", "first_name", "last_name", "jersey_num"]
    df = pd.DataFrame(data, columns=cols)

    # -- Write dataframe to db.
    with sqlite3.connect(db) as conn:
        df.to_sql("refs", conn, if_exists="append")

    print("L2M: Complete ({:.2f}s elapsed)                                   " \
        .format(time.time() - tstart))
    sys.stdout.flush()


if __name__ == "__main__":
    df = get_l2m_links("http://official.nba.com/nba-last-two-minute-reports-archive/")
    df = get_l2m_links("http://official.nba.com/2017-18-nba-officiating-last-two-minute-reports/")
    download_pdfs()
    update_refs()
