import os
import re
import sys
import urllib
from bs4 import BeautifulSoup

def init_data():
    if not os.path.exists("../data/pdfs/"):
        os.makedirs("../data/pdfs/")
    if not os.path.exists("../data/temp/"):
        os.makedirs("../data/temp/")
    if not os.path.exists("../data/db/"):
        os.makedirs("../data/db/")

def l2m_pdf_name(link):
    """Returns standardized pdf_name from link.
    link - pdf url from NBA L2M archive"""
    pdf_name = os.path.basename(link)

    # Replace all TM1@TM2 with TM1-TM2.
    pdf_name = pdf_name.replace('@', '-')

    # Get rid of bogus endings.
    for end in ['-1.pdf', '-2.pdf', '-4.pdf', 'b.pdf', '-V2.pdf']:
        pdf_name = pdf_name.replace(end, '.pdf')

    # Standardize dates.
    pdf_name = pdf_name.replace('2017', '17').replace('2016', '16')
    for d in range(1, 10):
        pdf_name = pdf_name.replace('-{0}-{0}-'.format(d), '-0{0}-0{0}-'.format(d))
        pdf_name = pdf_name.replace('-{}-'.format(d), '-0{}-'.format(d))
    return pdf_name

def get_l2m_links(url):
    """Returns a list of urls for all L2Ms.
    url - path to NBA L2M archive"""
    soup = BeautifulSoup(urllib.urlopen(url).read(), "html.parser")
    l2ms =[]
    # Find all L2M reports on Archive page.
    for tag in soup.find_all("p"):
        link = tag.find("a")
        if link and link["href"].endswith(".pdf"):
            for l2m in tag.find_all("a"):
                # Save L2M url and standardized name.
                l2ms.append([l2m["href"], l2m_pdf_name(l2m["href"])])

    # Filtering poorly named files (42/1008), mostly from 2015.
    regex = re.compile(r"^L2M-...-...-.*-...pdf$")
    l2ms = filter(lambda x: regex.search(x[1]), l2ms)
    return l2ms

if __name__ == "__main__":
    init_data()
    # Get all L2M pdf links from nba.com archive.
    links = get_l2m_links("http://official.nba.com/nba-last-two-minute-reports-archive/")
    n = len(links)
    for idx, (link, pdf_name) in enumerate(links):
        # 3 links are not working as of 08/06/2017.
        try:
            pdf = os.path.join("../data/pdfs/", pdf_name) # Define local pdf path.
            urllib.urlretrieve(link, pdf) # Download L2M.
            sys.stdout.write("\r({2}/{3}) {1:.2f}% Complete, {0}".format(pdf_name, (float(idx + 1) / n) * 100, idx + 1, n))
            sys.stdout.flush()
        except:
            pass
