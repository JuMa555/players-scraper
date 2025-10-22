## Players Scraper

This project scrapes and analyzes football player data from Wikipedia using Python. 
The scraped data is stored in an SQLite database and used for statistical analysis and comparisons.
Additionally, there is a function with query for calculating the average age, the average number of appearances and the total number of players by club. Also, there is a function with query for comparing with chosen player, it calculates how many players who play in the same position and who are younger than chosen player have a higher number of current club appearances than that player.


## Project Structure

```

ci\_task/

│

├── data/

│   ├── playersData.csv       # Input CSV with player data thad had to be imported into SQL database

│   ├── playersURLs.csv       # Input CSV with Wikipedia URLs that had to be scraped

│

├── db\_utils.py              # SQL queries for creating connection, creating table players, inserting player from .csv file, updating/inserting player from scraped data, adding and enriching columns AgeCategory and GoalsPerClubGame, analyzing club stats, comparing players by position and standardazing club names

├── playersScraper.py         # Main script for scraping: scraping single player, scraping all players and parsing player data

├── players.db                # SQLite database

├── requirements.txt

└── README.md

```



---



## Installation


1. Clone the repository:

```bash

git clone https://github.com/JuMa555/players-scraper.git

cd ci\_task

```



2. Install dependencies:


```bash

pip install -r requirements.txt

```



3. Run the scraper and analysis script:



```bash

python playersScraper.py

```
