<h1 align="center">
  NBA_L2M
</h1>

## Overview
The NBA assesses all 'officiated events' in the last two minutes (and OT) of games that were within 5 points with 2 minutes left in the game. All calls and 'notable non-calls' are reviewed following the same standards as the NBA's instant replay. This project collects and scrapes data from all published L2Ms and stores the results in a queryable database.

## Contents
Included in `data/l2m.db` are three tables:
### `urls`
<p align="center">
    <img width="600" src="https://github.com/jvani/NBA_L2M/blob/master/data/urls.png?raw=true"></img>
</p>

### `calls`
<p align="center">
    <img width="700" src="https://github.com/jvani/NBA_L2M/blob/master/data/calls.png?raw=true"></img>
</p>

### `refs`
<p align="center">
    <img width="300" src="https://github.com/jvani/NBA_L2M/blob/master/data/refs.png?raw=true"></img>
</p>

## Project Organization

    ├── README.md
    ├── data
    │   └── l2m.db         <- sqlite database.           
    └── nba_l2m            <- Python code to create db.

