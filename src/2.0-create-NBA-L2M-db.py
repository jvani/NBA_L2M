import os
import re
import sys
import math
import PyPDF2
import sqlite3
import pdfquery
import warnings
from datetime import datetime, timedelta

def create_db(db_path):
    """Creates NBA-L2M.db and initial tables"""
    conn = sqlite3.connect(os.path.join(db_path, "NBA-L2M.db"))
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS reports
    (report PRIMARY KEY, away, home, date,
    season, away_score, home_score, winner);""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS calls
    (report, period, time, call, committing, disadvantaged,
    decision, committing_team, disadvantaged_team);""")

    return conn, c

def split_pdf_pages(pdf_path, temp_path):
    """Split all pdfs into individual pages.
    pdf_path - folder path of downloaded pdf
    temp_path - folder path to output split pdfs"""

    # Clear temp folder.
    try:
        os.system("rm " + os.path.join(temp_path, "*"))
    except:
        pass

    pdf = PyPDF2.PdfFileReader(pdf_path, strict=False)
    for pp in range(pdf.numPages):
        page = PyPDF2.PdfFileWriter()
        page.addPage(pdf.getPage(pp))
        output_path = os.path.join(temp_path, "{}-".format(pp) + os.path.basename(pdf_path))
        with open(output_path, "wb") as output_pdf:
            page.write(output_pdf)

def l2m_season(game_date):
    """Determine NBA season/playoffs by date str.
    l2m_date - date string from l2m filename."""
    if game_date > datetime(2017, 4, 13):
        return "2016-17 NBA Playoffs"
    elif game_date > datetime(2016, 9, 1):
        return "2016-17 NBA Regular Season"
    elif game_date > datetime(2016, 4, 13):
        return "2015-16 NBA Playoffs"
    elif game_date > datetime(2015, 9, 1):
        return "2015-16 NBA Regular Season"
    elif game_date > datetime(2015, 4, 15):
        return "2014-15 NBA Playoffs"
    elif game_date > datetime(2014, 9, 1):
        return "2014-15 NBA Regular Season"
    else:
        return "NaN"

def scrape_l2ms(L2M_path, temp_path):
    """Scrape L2Ms data.
    L2M_path - path to downloaded L2Ms"""
    pdf_filenames = os.listdir(L2M_path)
    reports = []
    calls = []

    for idx, pdf_filename in enumerate(pdf_filenames): # For each report, scrape & split pages.
        try:
            pdf_path = os.path.join(L2M_path, pdf_filename)
            game_date = datetime.strptime(pdf_filename[12:-4], "%m-%d-%y") - timedelta(days=1)
            reports.append([pdf_filename[:-4], pdf_filename[4:7], pdf_filename[8:11], game_date, l2m_season(game_date), "NaN", "NaN", "NaN"])

            split_pdf_pages(pdf_path, temp_path)
            pdf_pages = os.listdir(temp_path)
            for pdf_page in pdf_pages: # For each page scrape data.
                page_path = os.path.join(temp_path, pdf_page)
                pdf = pdfquery.PDFQuery(page_path)
                pdf.load()

                # Find rows using 'Video' as anchor.
                rows = pdf.pq("""LTTextLineHorizontal:contains("Video ")""")
                for row in rows: # Scrape call data for each row.
                    for i in row:
                        y0 = float(filter(lambda x: x[0] == "y0", i.items())[0][1])
                        peri = pdf.pq("LTTextLineHorizontal:in_bbox('20, {}, 60, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        time = pdf.pq("LTTextLineHorizontal:in_bbox('60, {}, 100, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        call = pdf.pq("LTTextLineHorizontal:in_bbox('100, {}, 210, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        comm = pdf.pq("LTTextLineHorizontal:in_bbox('210, {}, 350, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        disa = pdf.pq("LTTextLineHorizontal:in_bbox('350, {}, 490, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        deci = pdf.pq("LTTextLineHorizontal:in_bbox('490, {}, 550, {}')".format(
                                math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                        # if peri.startswith("Q"):
                        calls.append([pdf_filename[:-4], peri, time, call, comm, disa, deci, "NaN", "NaN"])
            except:
                pass

        n = len(pdf_filenames)
        sys.stdout.write("\r({2}/{3}) {1:.2f}% Complete, {0}".format(pdf_filename, (float(idx + 1) / n) * 100, idx + 1, n))
        sys.stdout.flush()

    return calls, reports

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    db_path = "../data/db/"
    L2M_path = "../data/pdfs/"
    temp_path = "../data/temp/"

    conn, c = create_db(db_path)
    calls, reports = scrape_l2ms(L2M_path, temp_path)

    sys.stdout.write("Writing data to db...")
    sys.stdout.flush()

    c.executemany("""
    INSERT OR IGNORE INTO reports
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)""", reports)

    c.executemany("""
    INSERT OR IGNORE INTO calls
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""", calls)

    conn.commit()
    conn.close()

    sys.stdout.write("\rComplete!")
    sys.stdout.flush()
