# NBA Officiating Last Two Minute Reports (L2M) Database
`data/db/NBA-L2M.db` is current as of 08-08-2017.

## Overview
The NBA assesses all 'officiated events' in the last two minutes (and OT) of games that were within 5 points with 2 minutes left in the game. All calls and 'notable non-calls' are reviewed following the same standards as the NBA's instant replay. This project collects and scrapes data from all [published L2Ms](http://official.nba.com/nba-last-two-minute-reports-archive/) (.pdf) and stores the results in a queryable database. Supplemental data has been added including: final scores, winner, disadvantaged team, etc.

## Contents
Included in `data/db/NBA-L2M.db` are two primary tables.

#### `reports`
<img src="references/readme-pics/reports.png?raw=true" width="700" />

#### `calls`
<img src="references/readme-pics/calls.png?raw=true" width="900" />

## Reproducing the Database

```
cd src/
python 1.0-scrape-NBA-L2M.py # Will initialize data folder structure and download all L2M pdfs.
python 2.0-create-NBA-L2M-db.py # Initialize db, scrapes L2M pdfs, and adds data to db.
python 3.0-add-player-game-data-db.py # Populates additional data from bbref to db.
```

__Note:__ At present, database must be recreated to update.

## Project Organization

    ├── README.md
    ├── data
    │   ├── db             <- Resulting NBA-L2M.db
    │   ├── csv            <- Downloaded csvs from bbref.
    │   ├── pdfs           <- Downloaded L2Ms from NBA.
    │   └── temp
    ├── notebooks          <- Analysis Jupyter notebooks.
    ├── src                <- Python code to create db.
    └── references         <- Data dictionaries and data sources.
