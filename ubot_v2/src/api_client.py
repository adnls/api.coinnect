import requests
from tools import Change

class CoinMarketCap:
   
    def tick():

        try :
            
            url = """https://api.coinmarketcap.com/v1/ticker/?limit=1000"""
            rep = requests.get(url)
            tick = rep.json()

            return tick
        
        except requests.exceptions.ConnectionError:

            return - 1
            
        
    def glob():

        try :
            
            url = "https://api.coinmarketcap.com/v1/global/"
            rep = requests.get(url)
            glob = rep.json()

            return glob
        
        except requests.exceptions.ConnectionError:
            
            return -1
        


        
        
