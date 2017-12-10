from api_client import CoinMarketCap
from tools import Change, Serialize
import bdd
import pandas as pd
import json
import math
import datetime
import queue
from threading import Thread, RLock

class Update:

    def __init__(self):

        self.lock = RLock()
        self.uBot = bdd.MyBdd()
        self.q = queue.Queue()
                             
    def tables_1min(self):

        sql =    """SELECT *
                    FROM tick
                    ORDER BY
                    YEAR(dateheure) DESC,
                    MONTH(dateheure) DESC,
                    DAY(dateheure) DESC,
                    HOUR(dateheure) DESC,
                    DAY(dateheure) DESC,
                    MINUTE(dateheure) DESC,
                    id
                    LIMIT 2000"""
        
        self.uBot.useBdd("root", "123azerty")

        date = datetime.datetime.now()
        
        data = self.uBot.getQuery(sql)
        data = pd.DataFrame(data)
        
        names = data["nom_abreg"].unique()

        for item in names :
            self.q.put(item)

        all_values = []
        
        def worker():
            
            while 1:
                name = self.q.get()
                
                values = []
                
                values.append("(")
                
                rowsPd = data.loc[data["nom_abreg"] == name]

                rowsPd = rowsPd.sort_values(by="dateheure")
                    
                rowsPd["min_usd"] = rowsPd["cours_usd"].rolling(window=2).min()
                rowsPd["max_usd"] = rowsPd["cours_usd"].rolling(window=2).max()
                rowsPd["open_usd"] = rowsPd["cours_usd"].shift(1)
                rowsPd["close_usd"] = rowsPd["cours_usd"]
                    
                rowsPd["min_btc"] = rowsPd["cours_btc"].rolling(window=2).min()
                rowsPd["max_btc"] = rowsPd["cours_btc"].rolling(window=2).max()
                rowsPd["open_btc"] = rowsPd["cours_btc"].shift(1)
                rowsPd["close_btc"] = rowsPd["cours_btc"]

                del rowsPd["cours_usd"]
                del rowsPd["cours_btc"]
                del rowsPd["rang"]
                
                rowsPd = rowsPd.astype(str)
                rowsPd = rowsPd.tail(1)
                rowTick = rowsPd.to_dict(orient="records")

                pos = 0
                for key in rowTick[0].keys():
                    
                    if rowTick[0][key] == "None":
                        
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

                values_string = "".join(values)

                with self.lock:
                    
                    all_values.append(values_string)
                    
                self.q.task_done()
        
        for i in range(2):
                
            t = Thread(target=worker)
            # daemon lets the program end once the tasks are done
            t.daemon = True
            t.start()

                    
        self.q.join()
            
        all_values = "".join(all_values)
        all_values = all_values.replace( """)(""", """),(""")     
    
        date2 = datetime.datetime.now() - date
        print("""time : %s secondes"""%(round(float("""%s.%s"""%(date2.seconds, date2.microseconds)), 2)))
           
