import json

message = [{"wait_please" : "container_is_empty"}]
message = json.dumps(message)

class Container():
        
    def __init__(self):
        
        
        self.the_file = {"1min" :  {"tick" : {},
                                    "glob" : {}},
                         
                          "5min" :  {"tick" : {},
                                     "glob" : {}},
                         
                          "15min" : {"tick" : {},
                                     "glob" : {}},
                          
                          "30min" : {"tick" : {},
                                     "glob" : {}},
                          
                          "1h" :    {"tick" : {},
                                     "glob" : {}},
                          
                          "2h" :    {"tick" : {},
                                     "glob" : {}},
                          
                          "4h" :    {"tick" : {},
                                     "glob" : {}},
                          
                          "1d" :    {"tick" : {},
                                     "glob" : {}},
                          
                          "1w" :    {"tick" : {},
                                     "glob" : {}}
                          }
                      
    def set_the_file(self, period, tick, glob):

        self.the_file[period]["tick"].append(tick)
        self.the_file[period]["glob"].append(glob[0])
 
    def get_the_file(self, period, part):
        
        return self.the_file[period][part]
        
