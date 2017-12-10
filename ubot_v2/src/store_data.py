from api_client import CoinMarketCap
from tools import Change, Serialize
import bdd
import pandas as pd
import json

class Update:

    def __init__(self):
        
        self.uBot = bdd.MyBdd()

    def motherTables(self):
     
        globalDict = CoinMarketCap.glob()
        tickerList = CoinMarketCap.tick()

        if globalDict != -1 and tickerList != -1 :

            print("//////////////////////////////////////////////////////////")
            print("NOTE : connection à 'https://api.coinmarketcap.com/v1' ok\nNOTE : données obtenues")          
                
            self.uBot.useBdd("root", "123azerty")

            self.uBot.postQuery("""INSERT INTO glob (totalmc, mcvolume24h, btcpct, activecurr, activeass, activemark)
                                   VALUES (%s, %s, %s, %s, %s, %s)"""
                                %(globalDict["total_market_cap_usd"], globalDict["total_24h_volume_usd"],
                                  globalDict["bitcoin_percentage_of_market_cap"], globalDict["active_currencies"],
                                  globalDict["active_assets"], globalDict["active_markets"]))

            i=0
            while i <= len(tickerList)-1:
                self.uBot.postQuery("""INSERT into tick (id, nom, nom_abreg, rang, cours_usd, cours_btc,
                                                         volume24h_usd, mc_usd, av_supp, total_supp, change_1h,
                                                         change_24h, change_7d)
                                       VALUES ('%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                                    %(tickerList[i]["id"],tickerList[i]["name"],tickerList[i]["symbol"],tickerList[i]["rank"],
                                      tickerList[i]["price_usd"],tickerList[i]["price_btc"],tickerList[i]["24h_volume_usd"],
                                      tickerList[i]["market_cap_usd"],tickerList[i]["available_supply"],tickerList[i]["total_supply"],
                                      tickerList[i]["percent_change_1h"],tickerList[i]["percent_change_24h"],tickerList[i]["percent_change_7d"]))
                i+=1 

        else:
            
            print("//////////////////////////////////////////////////////////")
            print("WARNING : connection à 'https://api.coinmarketcap.com/v1' echec\nWARNING : bypass demandé")          
               
            self.uBot.useBdd("root", "123azerty")
            self.uBot.bypass(500000)
            print("NOTE : bypass effectué")
            

        self.uBot.truncTable("tick", 500000)
        self.uBot.truncTable("glob", 500)

        self.uBot.savePostQuery()


    def tables_1min(self):
                       
        self.uBot.useBdd("root", "123azerty")

        names = self.uBot.getQuery("""SELECT DISTINCT nom_abreg
                                      FROM tick""")

        i=0            
        while i<= len(names)-1:
                
            rows = self.uBot.getQuery("""SELECT * FROM tick
                                         WHERE nom_abreg = '%s'
                                         ORDER BY dateheure
                                         DESC LIMIT 2"""
                                        %names[i]["nom_abreg"])                
                
            rowsPd = pd.DataFrame(rows)
            rowsPd = rowsPd.sort_values(by="dateheure")
                
            rowsPd["min_usd"] = rowsPd["cours_usd"].rolling(window=2).min()
            rowsPd["max_usd"] = rowsPd["cours_usd"].rolling(window=2).max()
            rowsPd["open_usd"] = rowsPd["cours_usd"].shift(1)
            rowsPd["close_usd"] = rowsPd["cours_usd"]
                
            rowsPd["min_btc"] = rowsPd["cours_btc"].rolling(window=2).min()
            rowsPd["max_btc"] = rowsPd["cours_btc"].rolling(window=2).max()
            rowsPd["open_btc"] = rowsPd["cours_btc"].shift(1)
            rowsPd["close_btc"] = rowsPd["cours_btc"]
                
            rowsPd = rowsPd.tail(1)
                
            rowTick = rowsPd.to_dict(orient="records")
            rowTick = Change.NoneInListItems_1row(rowTick, "NULL")
                
            columnsToChange = ["open_usd", "min_usd", "max_usd","open_btc", "min_btc", "max_btc"]
                
            j = 0
            while j<= len(columnsToChange)-1:
                rowTick = Change.nanInListSingleItem_1row(rowTick, columnsToChange[j], "NULL")
                j+=1
                    
            self.uBot.postQuery("""INSERT INTO tick_1min (dateheure, nom_abreg, open_usd, close_usd, min_usd,
                                                          max_usd, open_btc, close_btc, min_btc, max_btc, id,
                                                          nom, volume24h_usd, mc_usd, av_supp, total_supp, change_1h,
                                                          change_24h, change_7d)
                                   VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s)"""
                                %(rowTick[0]["dateheure"], rowTick[0]["nom_abreg"], rowTick[0]["open_usd"],
                                  rowTick[0]["close_usd"], rowTick[0]["min_usd"], rowTick[0]["max_usd"],
                                  rowTick[0]["open_btc"], rowTick[0]["close_btc"], rowTick[0]["min_btc"],
                                  rowTick[0]["max_btc"], rowTick[0]["id"], rowTick[0]["nom"],
                                  rowTick[0]["volume24h_usd"], rowTick[0]["mc_usd"], rowTick[0]["av_supp"],
                                  rowTick[0]["total_supp"], rowTick[0]["change_1h"], rowTick[0]["change_24h"],
                                  rowTick[0]["change_7d"]))
                      
            i+=1
           
        self.uBot.truncTable("tick_1min", 500000)
           
        self.uBot.savePostQuery()


    def otherTables(self, tablesIn, tablesOut, nbr):
        
        nbrMinus1 = nbr-1
      
                        
        self.uBot.useBdd("root", "123azerty")
        names = self.uBot.getQuery("""SELECT DISTINCT nom_abreg FROM %s"""%tablesIn[0])

        i=0            
        while i<= len(names)-1:
                
            rows = self.uBot.getQuery("""SELECT * FROM %s WHERE nom_abreg = '%s' ORDER BY dateheure DESC LIMIT %s"""%(tablesIn[0], names[i]["nom_abreg"], nbr))                
                
            rowsPd = pd.DataFrame(rows)
            rowsPd = rowsPd.sort_values(by="dateheure")

            rowsPd["min_usd_new"] = rowsPd["min_usd"].rolling(window=nbr).min()
            rowsPd["max_usd_new"] = rowsPd["max_usd"].rolling(window=nbr).max()
            rowsPd["open_usd_new"] = rowsPd["open_usd"].shift(nbrMinus1)
            rowsPd["close_usd_new"] = rowsPd["close_usd"]
                
            rowsPd["min_btc_new"] = rowsPd["min_btc"].rolling(window=nbr).min()
            rowsPd["max_btc_new"] = rowsPd["max_btc"].rolling(window=nbr).max()
            rowsPd["open_btc_new"] = rowsPd["open_btc"].shift(nbrMinus1)
            rowsPd["close_btc_new"] = rowsPd["close_btc"]

            rowsPd = rowsPd.tail(1)
                
            rowTick = rowsPd.to_dict(orient="records")
                                
            columnsToChange = ["open_usd_new", "min_usd_new", "max_usd_new","open_btc_new", "min_btc_new", "max_btc_new", "close_usd_new", "close_btc_new", "volume24h_usd", "mc_usd", "av_supp", "total_supp", "change_1h", "change_24h", "change_7d"]
                
            j = 0
            while j<= len(columnsToChange)-1:
                rowTick = Change.NoneInListSingleItem_1row(rowTick, columnsToChange[j], float("nan"))
                rowTick = Change.nanInListSingleItem_1row(rowTick, columnsToChange[j], "NULL")
                j+=1

            self.uBot.postQuery("""INSERT INTO %s (dateheure, nom_abreg, open_usd, close_usd, min_usd, max_usd, open_btc, close_btc, min_btc, max_btc, id, nom, volume24h_usd, mc_usd, av_supp, total_supp, change_1h, change_24h, change_7d) VALUES ('%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s)"""%(tablesOut[0], rowTick[0]["dateheure"], rowTick[0]["nom_abreg"], rowTick[0]["open_usd_new"], rowTick[0]["close_usd_new"], rowTick[0]["min_usd_new"], rowTick[0]["max_usd_new"], rowTick[0]["open_btc_new"], rowTick[0]["close_btc_new"], rowTick[0]["min_btc_new"], rowTick[0]["max_btc_new"], rowTick[0]["id"], rowTick[0]["nom"], rowTick[0]["volume24h_usd"], rowTick[0]["mc_usd"], rowTick[0]["av_supp"], rowTick[0]["total_supp"], rowTick[0]["change_1h"], rowTick[0]["change_24h"], rowTick[0]["change_7d"]))
                                
            i+=1
           
        self.uBot.postQuery("""INSERT INTO %s SELECT * FROM %s ORDER BY dateheure DESC LIMIT 1"""%(tablesOut[1], tablesIn[1])) 

        self.uBot.truncTable("""%s"""%tablesOut[0], 500000)
        self.uBot.truncTable("""%s"""%tablesOut[1], 500)
            
        self.uBot.savePostQuery()


        
      
        
                    

