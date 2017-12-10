import requests
import json
from threading import RLock

lock = RLock()

class Worker:
    
    def test():
        
        for i in range(1, 10):
            
            print("mon test thread ", i)

    def exploit(period):
        
        data = json.dumps({"test":"test"})
                       
        try:                   
            rep = requests.post("http://localhost:5000/pool_2", data = data)                   
            print("""NOTE : %s %s\n
                     NOTE : table tick_1min transmise au scanner"""
                  %(rep, rep.json()))
        except requests.exceptions.ConnectionError:
            for i in range(1, 10):
                print(period)
      
       # with lock:                    
            #ctn.set_tick(data)
            #ctn.set_tick_1min(data)
            #ctn.set_glob_1min(data)
