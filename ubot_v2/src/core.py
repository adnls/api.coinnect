import datetime
import store_data
import requests
import json
from threading import RLock, Thread
from workers import Worker

lock = RLock()
        
class Threader():
    
    def threader(container):
        
        update = store_data.Update()
        ctn = container
        countDays = 0

        while True:

            date = datetime.datetime.now()

            if date.second == 0:
                
                print("\n")
                print("/////////////////////////////////////")
                print(date.strftime("%Y-%m-%d %H:%M:%S"))
                
                update.motherTables()
                print("/////////////////////////////////////")
                print("NOTE : tables mères actualisées")
                
                update.tables_1min()
                print("/////////////////////////////////////")
                print("NOTE : tables à 1min actualisées")

                t1 = Thread(target=Worker.exploit, args=("1min",))
                t1.daemon = True
                t1.start()
             
                if date.minute in (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55):

                    tables = update.otherTables(["tick_1min", "glob"], ["tick_5min", "glob_5min"], 5)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 5min actualisées")
         
                elif date.minute in (1, 16, 31, 46):

                    tables = update.otherTables(["tick_5min", "glob_5min"], ["tick_15min", "glob_15min"], 3)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 15min actualisées")

                elif date.minute in(2, 32):

                    tables = update.otherTables(["tick_15min", "glob_15min"], ["tick_30min", "glob_30min"], 2)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 30min actualisées")
      
                elif date.minute == 3:

                    tables = update.otherTables(["tick_30min", "glob_30min"], ["tick_1h", "glob_1h"], 2)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 1h actualisées")
     
                elif (date.hour in(0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22) and date.minute == 4):

                    tables = update.otherTables(["tick_1h", "glob_1h"], ["tick_2h", "glob_2h"], 2)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 2h actualisées")
     
                elif date.hour in(0, 4, 8, 12, 16, 20) and date.minute == 6:

                    tables = update.otherTables(["tick_2h", "glob_2h"], ["tick_4h", "glob_4h"], 2)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 4h actualisées")
    
                elif (date.hour == 0 and date.minute == 7):

                    tables = update.otherTables(["tick_4h", "glob_4h"], ["tick_1d", "glob_1d"], 6)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 1d actualisées")

                    countDays+=1
                                
                elif ((countDays == 1 or countDays%7 == 0) and (date.minute == 8) and (date.hour == 0)):

                    tables = update.otherTables(["tick_1d", "glob_1d"], ["tick_1w", "glob_1w"], 7)
                    print("/////////////////////////////////////")
                    print("NOTE : tables à 1w actualisées")
                       
                date2 = datetime.datetime.now() - date
                print("/////////////////////////////////////////////")
                print("""NOTE : durée des opérations %s secondes"""%(round(float("""%s.%s"""%(date2.seconds, date2.microseconds)), 2)))
                print("/////////////////////////////////////////////")
                         
