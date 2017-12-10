from api_client import CoinMarketCap
from tools import Change, Serialize
import bdd
import pandas as pd
import json
import math
import datetime

class Inserts:

    def __init__(self):
        
        
        self.tickers = object()
        self.nbr = object()

    def tick(self, uBot):
        
        time = datetime.datetime.now()
        
        globalDict = CoinMarketCap.glob()
        rep = CoinMarketCap.tick()
        
        if globalDict != -1 and rep != -1 :

            print("connection coinmarketcap ok")          
                
            uBot.postQuery("""INSERT INTO glob (totalmc, mcvolume24h, btcpct, activecurr, activeass, activemark)
                                   VALUES (%s, %s, %s, %s, %s, %s)"""
                                %(globalDict["total_market_cap_usd"], globalDict["total_24h_volume_usd"],
                                  globalDict["bitcoin_percentage_of_market_cap"], globalDict["active_currencies"],
                                  globalDict["active_assets"], globalDict["active_markets"]))

            values = []
            
            for i in range(len(rep)):
                
                pos = 0
                values.append("(")
                
                del rep[i]["last_updated"]
                del rep[i]["max_supply"]

                for key in rep[i].keys():
                    
                    if rep[i][key] == None:
                        rep[i][key] = "NULL"

                    if key in ["id", "name", "symbol"]:
                        rep[i][key] = rep[i][key].replace("-", "_")
                        rep[i][key] = rep[i][key].replace("'", "_")
                        
                    if pos not in range(3) or rep[i][key] == "NULL":
                        
                        values.append(rep[i][key])
                    else :
                        values.append("""'%s'"""%rep[i][key])

                    pos += 1

                    if pos != len(rep[i]):

                        values.append(",")
                    else :
                        
                        values.append(")")

                if i != len(rep)-1:
                    
                    values.append(",")
                
            values_string = "".join(values)

            sql = """INSERT INTO tick
                     (id, nom, nom_abreg, rang, cours_usd, cours_btc,
                       volume24h_usd, mc_usd, av_supp, total_supp, change_1h,
                       change_24h, change_7d)
                     VALUES %s"""%values_string

            uBot.postQuery(sql)
            
        else:
                        
            print("connection coinmarketcap fail")          
                           
            uBot.bypass(1000)
                        
        uBot.truncTable("tick", 500000)
        uBot.truncTable("glob", 500)

        uBot.savePostQuery()

        time2 = datetime.datetime.now() - time
        print("""time inserts tick : %s secondes"""%(round(float("""%s.%s"""%(time2.seconds, time2.microseconds)), 2)))


        
                                     
    def get_tick(self, uBot, nbr):

        time = datetime.datetime.now()
                    
        sql =    """SELECT *
                                FROM tick
                                ORDER BY
                                YEAR(dateheure) DESC,
                                MONTH(dateheure) DESC,
                                DAY(dateheure) DESC,
                                HOUR(dateheure) DESC,
                                DAY(dateheure) DESC,
                                MINUTE(dateheure) DESC
                                LIMIT %s000"""%nbr
        
        data = uBot.getQuery(sql)
        data = pd.DataFrame(data)

        names = data["id"].head(1000).unique()
        
        self.tickers = data
        self.nbr = nbr
        
        return names
    
    def curr_worker(self, currency):
        
        values = []
        data = self.tickers
        nbr = self.nbr
                
        values.append("(")
                
        rowsPd = data.loc[data["id"] == currency]

        rowsPd = rowsPd.sort_values(by="dateheure")
        rowsPd["min_usd"] = rowsPd["cours_usd"].rolling(window=nbr).min()
        rowsPd["max_usd"] = rowsPd["cours_usd"].rolling(window=nbr).max()
        rowsPd["open_usd"] = rowsPd["cours_usd"].shift(nbr-1)
        rowsPd["close_usd"] = rowsPd["cours_usd"]
                    
        rowsPd["min_btc"] = rowsPd["cours_btc"].rolling(window=nbr).min()
        rowsPd["max_btc"] = rowsPd["cours_btc"].rolling(window=nbr).max()
        rowsPd["open_btc"] = rowsPd["cours_btc"].shift(nbr-1)
        rowsPd["close_btc"] = rowsPd["cours_btc"]

        del rowsPd["cours_usd"]
        del rowsPd["cours_btc"]
        del rowsPd["rang"]
                
        rowsPd = rowsPd.astype(str)
        rowsPd = rowsPd.tail(1)
        rowTick = rowsPd.to_dict(orient="records")
                
        pos = 0
        for key in rowTick[0].keys():
                    
            if rowTick[0][key] == "None" or rowTick[0][key] == "nan":
                        
                rowTick[0][key] = "NULL"

            if rowTick[0][key] == "NULL" or key not in ["nom", "id", "nom_abreg", "dateheure"]:
                values.append((rowTick[0][key]))
            else:
                values.append("""'%s'"""%rowTick[0][key])

            pos += 1
                    
            if pos != len(rowTick[0]):
                values.append(",")

            else:
                values.append(")")
        return "".join(values)
        
                    
    def other_worker():       
                
        if pos != len(names):    
            values.append(",")

        values = "".join(values)

        uBot.postQuery("""INSERT INTO %s
                (av_supp, change_1h, change_24h, change_7d, dateheure,
                id, mc_usd, nom, nom_abreg, total_supp, volume24h_usd,
                min_usd, max_usd, open_usd, close_usd,
                min_btc, max_btc, open_btc, close_btc)
                               VALUES %s"""%(table, values))

        uBot.truncTable("""%s"""%table, 500000)
        
        uBot.savePostQuery()
        
        time2 = datetime.datetime.now() - time
        print("""time inserts %s : %s secondes"""%(table, round(float("""%s.%s"""%(time2.seconds, time2.microseconds)), 2)))
                
    def otherTables(self, tablesIn, tablesOut, nbr):

        time = datetime.datetime.now()
        
        nbrMinus1 = nbr-1
        
        sql =    """SELECT *
                    FROM table_test
                    ORDER BY
                    YEAR(dateheure) DESC,
                    MONTH(dateheure) DESC,
                    DAY(dateheure) DESC,
                    HOUR(dateheure) DESC,
                    DAY(dateheure) DESC,
                    MINUTE(dateheure) DESC
                    LIMIT %s000"""%(nbr+1)

        self.uBot.useBdd("root", "123azerty")
        
        data = self.uBot.getQuery(sql)
        data = pd.DataFrame(data)

        names = data["id"].unique()

        values = []
        pos = 0
        
        for name in names:

            values.append("(")
            
            rowsPd = pd.DataFrame(data.loc[data["id"]==name])
            rowsPd = rowsPd.sort_values(by="dateheure")

            rowsPd["min_usd_new"] = rowsPd["min_usd"].rolling(window=nbr).min()
            rowsPd["max_usd_new"] = rowsPd["max_usd"].rolling(window=nbr).max()
            rowsPd["open_usd_new"] = rowsPd["open_usd"].shift(nbrMinus1)
            rowsPd["close_usd_new"] = rowsPd["close_usd"]
                
            rowsPd["min_btc_new"] = rowsPd["min_btc"].rolling(window=nbr).min()
            rowsPd["max_btc_new"] = rowsPd["max_btc"].rolling(window=nbr).max()
            rowsPd["open_btc_new"] = rowsPd["open_btc"].shift(nbrMinus1)
            rowsPd["close_btc_new"] = rowsPd["close_btc"]

            rowsPd = rowsPd.astype(str)
            rowsPd = rowsPd.tail(1)  
            rowTick = rowsPd.to_dict(orient="records")

            pos2 = 0
            for key in rowTick[0].keys():
                
                if rowTick[0][key] == "None" or rowTick[0][key] == "nan":
                    
                    rowTick[0][key] = "NULL"

                if rowTick[0][key] == "NULL" or key not in ["nom", "id", "nom_abreg", "dateheure"]:
                    values.append((rowTick[0][key]))
                else:
                    values.append("""'%s'"""%rowTick[0][key])

                pos2 += 1
                
                if pos2 != len(rowTick[0]):
                    values.append(",")

                else:
                    values.append(")")
                
            pos +=1
            
            if pos != len(names):    
                values.append(",")

        values = "".join(values)
           
        self.uBot.postQuery("""INSERT INTO %s SELECT * FROM %s ORDER BY dateheure DESC LIMIT 1"""%(tablesOut[1], tablesIn[1])) 

        self.uBot.truncTable("""%s"""%tablesOut[0], 500000)
        self.uBot.truncTable("""%s"""%tablesOut[1], 500)
            
        self.uBot.savePostQuery()

        time2 = datetime.datetime.now() - time
        print("""time inserts 3 : %s secondes"""%(round(float("""%s.%s"""%(time2.seconds, time2.microseconds)), 2)))
        
      
        
                    

