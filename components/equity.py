import scraper, requests
#Spy is real time s&p
from bs4 import BeautifulSoup# Soup object for HTML response
import requests #HTTP
import pandas as pd

SNP_LIVE_URL= 'https://finance.yahoo.com/quote/%5EGSPC/'
GAINERS_URL="https://finance.yahoo.com/markets/stocks/gainers/"

def get_SnP():
    SnPValue=scraper.getData(SNP_LIVE_URL, {"data-testid": "qsp-price"})
    if not SnPValue:
        print("No S&P") 
    return SnPValue
    

def get_gainers():    
    gainers=scraper.getData(GAINERS_URL, 'js-signals_1')
    if not gainers:
        print("no pain no gain")
    return gainers
    r = requests.get("https://finviz.com")
    soup = BeautifulSoup(r.content, "html.parser")

    for table in soup.find_all("table", {"class": "t-home-table"}):
        print()

        for tr in table.find_all("tr"):
            td = tr.find_all("td")
            row = [i.text for i in td]

            if len(row) > 5 and row[5] in ['Top Gainers', 'Top Losers']:
                print(row)
    # Now 'headers' contains the column names and 'rows' contains the data


get_gainers()


