from __future__ import print_function
import os
import re
import sys
import time
import urllib
import sqlite3
import pdfquery
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup


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

    return [l2m_url, away, home, date]


def _scrape_l2m(pdf_path, db=os.path.join("data", "l2m.db")):
    """Scrape data from pdf and write to db.
    Args:
        pdf_path (str) - path to pdf to scrape.
    """

    pdf_name = os.path.basename(pdf_path)
    pdf = pdfquery.PDFQuery(pdf_path)
    pdf.load()

    searches = {
        "yy": "LTPage[pageid='{}'] LTTextLineHorizontal:contains('Video')",
        "peri": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('20, {}, 60, {}')",
        "time": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('60, {}, 100, {}')",
        "call": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('80, {}, 210, {}')",
        "comm": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('210, {}, 350, {}')",
        "disa": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('350, {}, 490, {}')",
        "deci": "LTPage[pageid='{}'] LTTextLineHorizontal:in_bbox('490, {}, 550, {}')"
    }

    data = []
    for pp in range(pdf.doc.catalog["Pages"].resolve()["Count"]):
        pg = pp + 1
        y0s = [float(y.attrib["y0"]) for y in pdf.pq(searches["yy"].format(pg))]
        for y0 in y0s:
            peri = pdf.pq(searches["peri"].format(pg, y0 - 1, y0 + 14)).text()
            time = pdf.pq(searches["time"].format(pg, y0 - 1, y0 + 14)).text()
            call = pdf.pq(searches["call"].format(pg, y0 - 1, y0 + 14)).text()
            comm = pdf.pq(searches["comm"].format(pg, y0 - 1, y0 + 14)).text()
            disa = pdf.pq(searches["disa"].format(pg, y0 - 1, y0 + 14)).text()
            deci = pdf.pq(searches["deci"].format(pg, y0 - 1, y0 + 14)).text()
            if peri.startswith("Q"):
                data.append([pdf_name, peri, time, call, comm, disa, deci])

    cols = ["pdf", "period", "time", "call_type", "committing_player",
            "disadvantaged_player", "review_decision"]
    df = pd.DataFrame(data, columns=cols)

    with sqlite3.connect(db) as conn:
        df.to_sql("calls", conn, if_exists="append")


def get_l2m_links(url, db=os.path.join("data", "l2m.db")):
    """Get l2m links from archive and add to db if there are new links.
    Args:
        url (str) - l2m archive page.
        db (str) - path to sqlite db.
    Returns:
        df (dataframe) - l2m links.
    """

    tstart = time.time()
    print("L2M: Scraping pdf links.                                           ")
    sys.stdout.flush()
    # -- Scrape relevant urls, pull metadata, and load to dataframe.
    soup = BeautifulSoup(urllib.urlopen(url).read(), "html.parser")
    links = filter(lambda link: link.has_attr("href"), soup.find_all("a"))
    urls = filter(lambda url: url.endswith("pdf"), [
        link["href"] for link in links])
    l2m_urls = filter(re.compile(r"L2M").search, urls)
    metadata = map(lambda l2m_url: _url_metadata(l2m_url), l2m_urls)
    df = pd.DataFrame(metadata, columns=["l2m_url", "away", "home", "date"]) \
        .set_index(["away", "home", "date"])

    print("L2M: Saving to {}.".format(db))
    sys.stdout.flush()
    # -- Write dataframe to db (overwrite existing table).
    with sqlite3.connect(db) as conn:
        existing = pd.read_sql("SELECT * FROM urls", conn) \
            .set_index(["away", "home", "date"])
        df = df[~df.isin(existing)]
        df.to_sql("urls", conn, if_exists="append")

    print("L2M: Complete ({:.2f}s elapsed)                                   " \
        .format(time.time() - tstart))

    return df


def download_pdfs(pdf_folder=os.path.join(".", "pdfs"),
        db=os.path.join(".", "data", "l2m.db")):
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
        df = pd.read_sql("SELECT * FROM urls", conn) \
            .set_index(["away", "home", "date"])

    # -- List pdfs that have already been downloaded.
    pdfs = filter(lambda fname: fname.endswith(".pdf"), os.listdir(pdf_folder))
    # -- Create bool mask for l2ms that have not been downloaded.
    bool_mask = map(lambda x: os.path.basename(x) not in pdfs, df.l2m_url)
    ll = len(df[bool_mask])
    # -- Download all l2ms that have not been downloaded.
    for n_row, (idx, row) in enumerate(df[bool_mask].iterrows()):
        try:
            bname = os.path.basename(row.l2m_url)
            pdf_path = os.path.join(pdf_folder, bname)
            urllib.urlretrieve(row.l2m_url, pdf_path)
            _scrape_l2m(pdf_path)
            print("L2M: Downloaded & Scraped {} ({}/{})                      " \
                .format(bname, n_row + 1, ll))
            sys.stdout.flush()
        except:
            pass

    print("L2M: Complete ({:.2f}s elapsed)                                   " \
        .format(time.time() - tstart))


if __name__ == "__main__":
    url = "http://official.nba.com/nba-last-two-minute-reports-archive/"
    df = get_l2m_links(url)
    download_pdfs()
