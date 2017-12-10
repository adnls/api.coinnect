from api_client import CoinMarketCap
from tools_2 import Format_string
import bdd
import pandas as pd
import json
import math
import datetime
import decimal
from threading import RLock
import requests

lock = RLock()

def to_str(value):
    if isinstance(value, decimal.Decimal) or isinstance(value, datetime.datetime):
        return str(value)

class New_update:

    def __init__(self, con1, con2, con3, con4, con5,
                 con6, con7, con8, con9, con10,
                 con11, con12, con13, con14, con15,
                 con16, con17):
        
        self.con1 = con1
        self.con2 = con2
        self.con3 = con3
        self.con4 = con4
        self.con5 = con5
        self.con6 = con6
        self.con7 = con7
        self.con8 = con8
        self.con9 = con9
        self.con10 = con10
        self.con11 = con11
        self.con12 = con12
        self.con13 = con13
        self.con14 = con14
        self.con15 = con15
        self.con16 = con16
        self.con17 = con17

    def store(self):
        
        globalDict = CoinMarketCap.glob()
        rep = CoinMarketCap.tick()
        
        if globalDict != -1 and rep != -1 :

            print("connection coinmarketcap ok")
            
            values = Format_string.for_tick_insert(rep)

            sql = """INSERT INTO tick_test
                     VALUES %s"""%(values)

            sql2 = """INSERT INTO glob_test
                      (dateheure, totalmc, mcvolume24h, btcpct, activecurr, activeass, activemark)
                      VALUES
                      (DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00'), {}, {}, {}, {}, {}, {})""".format(globalDict["total_market_cap_usd"],
                      globalDict["total_24h_volume_usd"],
                      globalDict["bitcoin_percentage_of_market_cap"], globalDict["active_currencies"],
                      globalDict["active_assets"], globalDict["active_markets"])

            self.con1.postQuery(sql)
            self.con1.postQuery(sql2)
            
        else:
                        
            print("connection coinmarketcap fail -> bypass")                        
            self.con1.bypass()
            
        #cascade on delete pour glob_test fk dateheure
        self.con1.truncTable("tick_test", 10081)
        self.con1.savePostQuery()
        return
        
    
    def send_tick_n_glob(self, period):

        nbr = int
        user = str
        uBot = object()
        if period == "1min" : nbr = 1 ; uBot = self.con2; user = "adnls"
        elif period == "5min" : nbr = 5; uBot = self.con3; user = "adnls2"
        elif period == "15min" : nbr = 15; uBot = self.con4; user = "adnls3"
        elif period == "30min" : nbr = 30; uBot = self.con5; user = "adnls4"
        elif period == "1h" : nbr = 60; uBot = self.con6; user = "adnls5"
        elif period == "2h" : nbr = 120; uBot = self.con7; user = "adnls6"
        elif period == "4h" : nbr = 240; uBot = self.con8; user = "adnls7"
        elif period == "1d" : nbr = 1440; uBot = self.con9; user = "adnls8"
        elif period == "1w" : nbr = 10080; uBot = self.con10; user = "adnls9"
        
        sql_temp="""SET @ref = (SELECT MAX(dateheure) FROM tick_test);

                    CREATE TEMPORARY TABLE high_low 
                    AS SELECT 
                    id, 
                    MAX(cours_usd) AS high_usd,
                    MIN(cours_usd) AS low_usd,
                    MAX(cours_btc) AS high_btc,
                    MIN(cours_btc) AS low_btc
                    FROM tick_test 
                    WHERE dateheure >= (@ref - INTERVAL %s MINUTE)
                    GROUP BY id;

                    CREATE TEMPORARY TABLE open
                    AS SELECT
                    id, 
                    cours_usd AS open_usd,
                    cours_btc AS open_btc
                    FROM tick_test
                    WHERE dateheure = (@ref - INTERVAL %s MINUTE);

                    CREATE TEMPORARY TABLE close
                    AS SELECT *
                    FROM tick_test
                    WHERE dateheure = @ref"""%(nbr, nbr)

        sql_tick="""SELECT
                    
                    a.id,
                    a.nom AS name,
                    a.nom_abreg AS symbol,
                    a.rang AS rank,
                    
                    c.open_usd,
                    b.high_usd,
                    b.low_usd,
                    a.cours_usd AS close_usd,
                    
                    c.open_btc,
                    b.high_btc,
                    b.low_btc,
                    a.cours_btc AS close_btc,
                    
                    a.volume24h_usd AS volume_24h_usd,
                    a.mc_usd AS market_cap_usd,
                    a.av_supp AS available_supply,
                    a.total_supp AS total_supply,
                    a.max_supp AS max_supply,

                    a.dateheure AS last_updated
                    
                    FROM close AS a
                    LEFT JOIN high_low AS b
                    ON a.id = b.id
                    LEFT JOIN open AS c
                    ON b.id = c.id"""
        
        sql_glob="""SELECT
                    totalmc AS total_market_cap_usd,
                    mcvolume24h AS total_24h_volume_usd,
                    btcpct AS bitcoin_percentage_of_market_cap,
                    activecurr AS active_currencies,
                    activeass AS active_assets,
                    activemark AS active_markets,
                    dateheure AS last_updated
                    FROM glob_test
                    WHERE dateheure = @ref"""
         
        uBot.useBdd("""%s"""%(user), "123azerty")
        
        uBot.postQuery(sql_temp)
        tick = uBot.getQuery(sql_tick)
        glob = uBot.getQuery(sql_glob)
        
        uBot.closeCommunication()
        
        kit = json.dumps({"period":period, "tick":tick, "glob" : glob[0]}, default=to_str)
        rep = requests.post("http://localhost:5000/new_tick", data = kit)
        
        return
            
    def send_rank(self, payload):
        
        user = str
        uBot = object()
        if len(payload) == 2:
            nbr = int
            period = str
            if payload[0] == "1h" and payload[1] == "usd":
                nbr = 1; uBot = self.con11; user = "adnls10"
            elif payload[0] == "1h" and payload[1] == "btc":
                nbr = 1; uBot = self.con12; user = "adnls11"
            elif payload[0] == "1d" and payload[1] == "usd":
                nbr = 24; uBot = self.con13; user = "adnls12"
            elif payload[0] == "1d" and payload[1] == "btc":
                nbr = 24; uBot = self.con14; user = "adnls13"
            elif payload[0] == "1w" and payload[1] == "usd":
                nbr = 168; uBot = self.con15; user = "adnls14"
            elif payload[0] == "1w" and payload[1] == "btc":
                nbr = 168; uBot = self.con16; user = "adnls15"

            refcurr = payload[1]
            period = payload[0]
            
            sql_temp = """SET @ref = (SELECT MAX(dateheure) FROM tick_test);

                        CREATE TEMPORARY TABLE change_start AS SELECT
                        id, cours_%s AS close FROM tick_test WHERE dateheure = @ref - INTERVAL %s HOUR;

                        CREATE TEMPORARY TABLE change_end AS SELECT
                        id, cours_%s AS close FROM tick_test WHERE dateheure = @ref"""%(refcurr, nbr, refcurr)
            
            sql_rank =   """SELECT
                            a.id,
                            ((b.close - a.close)*100)/a.close
                            AS value
                            FROM change_start AS a
                            LEFT JOIN change_end AS b 
                            ON a.id = b.id"""

            uBot.useBdd("""%s"""%(user), "123azerty")
            uBot.postQuery(sql_temp)
            rank = uBot.getQuery(sql_rank) 
            uBot.closeCommunication()
            
            kit = json.dumps({"header":payload, "rank":rank}, default = to_str)
            
        else :
                  
            uBot = self.con17; user = "adnls16"
            
            sql_set = "SET @ref = (SELECT MAX(dateheure) FROM tick_test)"
            sql_rank_mc = "SELECT id, mc_usd AS value FROM tick_test WHERE dateheure = (SELECT MAX(dateheure) FROM tick_test)"
            sql_rank_vol = "SELECT id, volume24h_usd AS value FROM tick_test WHERE dateheure = @ref"

            uBot.useBdd("""%s"""%(user), "123azerty")
            uBot.postQuery(sql_set)
            rank_mc = uBot.getQuery(sql_rank_mc)
            rank_vol = uBot.getQuery(sql_rank_vol)
            uBot.closeCommunication()
            
            kit = json.dumps({"header":payload, "rank_mc":rank_mc, "rank_vol" : rank_vol}, default = to_str)
            
        rep = requests.post("http://localhost:5000/new_rank", data = kit)
        
        return
        
