import os
import re
import math
import urllib
import PyPDF2
import sqlite3
import pdfquery
from bs4 import BeautifulSoup

def create_db(db_path):
    """Creates NBA-L2M.db and initial tables"""
    conn = sqlite3.connect(os.path.join(db_path, "NBA-L2M.db"))
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS reports
    (report PRIMARY KEY, away, away_score, home, home_score,
    away_W, date, link, season);""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS calls
    (report, period, time, call, committing, disadvantaged,
    committing_team, disadvantaged_team, decision);""")

    conn.commit()
    conn.close()

def l2m_pdf_name(link):
    """Returns standardized pdf_name from link.
    link - pdf url from NBA L2M archive"""
    pdf_name = os.path.basename(link)
    pdf_name = pdf_name.replace('@', '-')
    pdf_name = pdf_name.replace('2017', '17')
    pdf_name = pdf_name.replace('2016', '16')
    pdf_name = pdf_name.replace('-1.pdf', '.pdf')
    pdf_name = pdf_name.replace('-2.pdf', '.pdf')
    pdf_name = pdf_name.replace('-4.pdf', '.pdf')
    pdf_name = pdf_name.replace('b.pdf', '.pdf')
    return pdf_name

def get_l2m_links(url):
    """Returns a list of urls for all L2Ms.
    url - path to NBA L2M archive"""
    soup = BeautifulSoup(urllib.urlopen(url).read(), "html.parser")
    l2ms =[]
    for tag in soup.find_all("p"):
        link = tag.find("a")
        if link and link["href"].endswith(".pdf"):
            for l2m in tag.find_all("a"):
                l2ms.append([l2m["href"], l2m_pdf_name(l2m["href"])])

    # Filtering poorly named files (42/1008), mostly from 2015.
    regex = re.compile(r"^L2M-...-...-.*-...pdf$")
    l2ms = filter(lambda x: regex.search(x[1]), l2ms)
    return l2ms

def split_pdf_pages(pdf_path):
    """Split all pdfs by page.
    pdf_path - full path of downloaded pdf"""
    opened_pdf = PyPDF2.PdfFileReader(pdf_path, strict=False)
    for i in range(opened_pdf.numPages):
        output = PyPDF2.PdfFileWriter()
        output.addPage(opened_pdf.getPage(i))
        with open(os.path.join(os.path.dirname(pdf_path), "{}-".format(i) + os.path.basename(pdf_path)), "wb") as output_pdf:
            output.write(output_pdf)
    os.remove(pdf_path)

def scrape_l2m(pdf_path):
    """Scrape L2M data from each page.
    pdf_path - full path of downloaded pdf"""
    pdfs = os.listdir(pdf_path)
    calls = []
    for f in pdfs:
        pdf = pdfquery.PDFQuery(os.path.join(pdf_path, f))
        pdf.load()
        rows = pdf.pq("""LTTextLineHorizontal:contains("Video ")""")
        for row in rows[2:]:
            for i in row:
                y0 = float(filter(lambda x: x[0] == "y0", i.items())[0][1])
                peri = pdf.pq("LTTextLineHorizontal:in_bbox('0, {}, 90, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                time = pdf.pq("LTTextLineHorizontal:in_bbox('60, {}, 130, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                call = pdf.pq("LTTextLineHorizontal:in_bbox('100, {}, 260, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                comm = pdf.pq("LTTextLineHorizontal:in_bbox('200, {}, 400, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                disa = pdf.pq("LTTextLineHorizontal:in_bbox('360, {}, 500, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                deci = pdf.pq("LTTextLineHorizontal:in_bbox('500, {}, 560, {}')".format(
                        math.floor(y0 - 11), math.ceil(y0 + 14))).text()
                calls.append([pdfs[0][-24:-4], peri, time, call, comm, disa, "Nan", "Nan", deci])
    WL = re.findall('\d+', pdf.pq("""LTTextLineHorizontal:contains(", 20{}")""".format(f[-6:-4])).text())
    report = [pdfs[0][-24:-4], pdfs[0][-20:-17], WL[0], pdfs[0][-16:-13],
              WL[1], WL[0] > WL[1], pdfs[0][-12:-4]]
    return calls, report

if __name__ == "__main__":
    ## EXPECTED RUNTIME OF 70 MINUTES.
    pdf_path = "../data/temp/"
    db_path = "../data/db/"

    # Create db and tables (if necessary) and open connection to db.
    create_db(db_path)
    conn = sqlite3.connect(os.path.join(db_path, "NBA-L2M.db"))
    c = conn.cursor()

    # Get all L2M pdf links from nba.com archive.
    links = get_l2m_links("http://official.nba.com/nba-last-two-minute-reports-archive/")
    n = len(links)
    times = []

    for idx, (link, pdf_name) in enumerate(links):
        if idx != 0:
            os.system("rm " + pdf_path + "*") # Clear temp folder.

        pdf = os.path.join(pdf_path, pdf_name) # Define local pdf path.

        urllib.urlretrieve(link, pdf) # Download L2M.
        split_pdf_pages(pdf) # Split pdf into seperate pages.
        calls, report = scrape_l2m(pdf_path) # Scrape all pages.
        report.extend([link, 'Nan']) # Add link & 'season' filler to data.

        c.execute("""
        INSERT OR IGNORE INTO reports
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""", report)

        c.executemany("""
        INSERT OR IGNORE INTO calls
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""", calls)

        print "\r{3} {0:.2f}% Complete, ({1}/{2})".format(
        (float(idx + 1) / n) * 100, idx, n, pdf),

    conn.commit()
    conn.close()
