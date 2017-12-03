<h1 align="center">
  NBA_L2M
</h1>

# NBA Officiating Last Two Minute Reports (L2M) Database
`data/l2m.db` is current as of 2017-11-26.

## Overview
The NBA assesses all 'officiated events' in the last two minutes (and OT) of games that were within 5 points with 2 minutes left in the game. All calls and 'notable non-calls' are reviewed following the same standards as the NBA's instant replay. This project collects and scrapes data from all [published L2Ms](http://official.nba.com/nba-last-two-minute-reports-archive/) (.pdf) and stores the results in a queryable database.

## Contents
Included in `data/l2m.db` are two tables. `urls`, which includes the links to each L2M with the following columns: `away | home | date | l2m_url`. `calls`, which includes the scraped calls from each L2M with the following columns: `pdf | period | time | call_type | committing_player | disadvantaged_player | review_decision`

## Project Organization

    ├── README.md
    ├── data
    │   └── l2m.db         <- sqlite database.           
    ├── nba_l2m            <- Python code to create db.
    └── Archive            <- Prev. version (rewrite in progress).
