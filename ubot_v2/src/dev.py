from store_data_4 import New_update
import datetime
import decimal
import bdd
import time
from multiprocessing import Pool
from threading import Thread
import threading
from container import Container
import pandas as pd

con1 = bdd.MyBdd()
con1.useBdd("root", "123azerty")

con2 = bdd.MyBdd()
con3 = bdd.MyBdd()
con4 = bdd.MyBdd()
con5 = bdd.MyBdd()
con6 = bdd.MyBdd()
con7 = bdd.MyBdd()
con8 = bdd.MyBdd()
con9 = bdd.MyBdd()
con10 = bdd.MyBdd()
con11 = bdd.MyBdd()
con12 = bdd.MyBdd()
con13 = bdd.MyBdd()
con14 = bdd.MyBdd()
con15 = bdd.MyBdd()
con16 = bdd.MyBdd()
con17 = bdd.MyBdd()

new_update = New_update(con1, con2, con3, con4, con5,
                        con6, con7, con8, con9, con10,
                        con11, con12, con13, con14, con15,
                        con16, con17)

if __name__ == '__main__':
    
    periods = ["1min", "5min", "15min", "30min", "1h", "2h", "4h", "1d", "1w"]
    ranks = [["other"],
             ["1h", "usd"], ["1h", "btc"],
             ["1d", "usd"], ["1d", "btc"],
             ["1w", "usd"], ["1w", "btc"]]
    ts = []
    while True:
        
        signal = datetime.datetime.now()
        if signal.second == 0:

            start = time.time()
            new_update.store()

            for rank in ranks:
                t = Thread(target=new_update.send_rank, args=(rank,), name="send_data")
                t.deamon = True
                ts.append(t)
                ts[-1].start()
            
            for period in periods:
                t = Thread(target=new_update.send_tick_n_glob, args=(period,), name="send_data")
                t.deamon = True
                ts.append(t)
                ts[-1].start()        
                
            for i in range(len(ts)):    
                ts[i].join()
                
            del ts[:]
            
            print("""time taken : %s seconds"""%(time.time()-start))
            
            #for loop in range(1):
     #   tick = insert.from_tick_2(1, ctnr)
        
      #  if len(ctnr.the_file["1min"]["tick"]) == 0:
       #     for i in range(len(tick)):
                
        #        name = tick[i]["id"]
         #       ctnr.the_file["1min"]["tick"].update({name : [tick[i]]})
                
            #print(pd.DataFrame(ctnr.the_file["1min"]["tick"]["bitcoin"]))
            
        #else:
         #   names = ctnr.the_file["1min"]["tick"].keys() 
          #  for i in range(len(tick)):
                
           #     name = tick[i]["id"]
            #    if name in names:
             #       ctnr.the_file["1min"]["tick"][name].append(tick[i])
              #  else:
               #     ctnr.the_file["1min"]["tick"].update({name : [tick[i]]})
                    
                #if len(ctnr.the_file["1min"]["tick"][name]) > 500:
                #    del ctnr.the_file["1min"]["tick"][name][0]
            #print(pd.DataFrame(ctnr.the_file["1min"]["tick"]["bitcoin"]))
        
         #       
    #i = 1
    #while True:
    #   signal = datetime.datetime.now()
    #    if signal.second == 0 :
     #       start = time.time()
     #       Inserts.tick(con)
     #       print("""%s. time taken : %s seconds"""%(i ,(time.time()-start)))
     #       i+=1
            
    #start = time.time()
    
   # with Pool(2) as p:

    #    currencies = insert.get_tick(connector1, 15)
    #    res = p.map(insert.curr_worker, currencies)
    #    res = "".join(res)
     #   res = res.replace(")(", "),(")
         
    #    currencies = insert.get_tick(connector1, 60)
    #    res = p.map(insert.curr_worker, currencies)
        #res = "".join(res)
        #res = res.replace(")(", "),(")
        #print("""2. time taken : %s seconds"""%(time.time()-start))
    
#for table in tables:
    
   # if table == "tick_1min":
    #    nbr = 2
     #   con = con1
    #else:
     #   nbr = 5
      #  con = con2
        
    #t = Thread(target=worker, args=(table, nbr, con))
    #t.deamon = True
    #t.start()
    
#t.join()

#insert.otherTables(["tick_1min", "glob"], ["tick_5min", "glob_5min"], 5)
