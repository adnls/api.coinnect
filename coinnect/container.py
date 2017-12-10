import json
import pandas as pd

message = [{"wait_please" : "container_is_empty"}]
message = json.dumps(message)

class Container():
        
    def __init__(self):
        
        
        self.the_file = {"1min" :  {"tick" : {},
                                    "glob" : []},
                         
                          "5min" :  {"tick" : {},
                                     "glob" : []},
                         
                          "15min" : {"tick" : {},
                                     "glob" : []},
                          
                          "30min" : {"tick" : {},
                                     "glob" : []},
                          
                          "1h" :    {"tick" : {},
                                     "glob" : []},
                          
                          "2h" :    {"tick" : {},
                                     "glob" : []},
                          
                          "4h" :    {"tick" : {},
                                     "glob" : []},
                          
                          "1d" :    {"tick" : {},
                                     "glob" : []},
                          
                          "1w" :    {"tick" : {},
                                     "glob" : []}
                          }

        self.rank_file = {"market_cap_usd" : None,
                          "volume_24h_usd" : None,
                          "change_1h_usd" : None,
                          "change_1d_usd" : None,
                          "change_1w_usd" : None,
                          "change_1h_btc" : None,
                          "change_1d_btc" : None,
                          "change_1w_btc" : None}
        
    def set_the_file(self, period, tick, glob):
        
        if len(self.the_file[period]["tick"]) == 0:
            for i in range(len(tick)):
                
                name = tick[i]["id"]
                self.the_file[period]["tick"].update({name : [tick[i]]})
                
            #print(pd.DataFrame(self.the_file[period]["tick"]["bitcoin"]))
            
        else:
            names = self.the_file[period]["tick"].keys() 
            for i in range(len(tick)):
                
                name = tick[i]["id"]
                if name in names:
                    self.the_file[period]["tick"][name].append(tick[i])
                else:
                    self.the_file[period]["tick"].update({name : [tick[i]]})
                
                if len(self.the_file[period]["tick"][name]) > 300:
                    del self.the_file[period]["tick"][name][0]
                    
        self.the_file[period]["glob"].append(glob)
                    
        if len(self.the_file[period]["glob"]) > 300:
            del self.the_file[period]["glob"][0]
 
    def get_the_file(self, period, currency):
        
        return self.the_file[period]["tick"][currency][-100:]

    def get_the_file_2(self, period):
        
        return self.the_file[period]["glob"][-100:]

    def get_the_file_3(self, period):
        
        return self.the_file[period]["tick"].keys()

    def get_the_file_4(self, period, limit):

        result = pd.DataFrame(self.rank_file["market_cap_usd"])
        result["value"] = result["value"].fillna(value='0')
        result["value"] = result["value"].astype(float)
        result["rank"] = result["value"].rank(method="dense", ascending = False).astype(int)
        result = result.sort_values("rank", ascending=1)
        result = result[:limit]
        ids = result["id"].unique()
        data = {}
        for curr in ids :
            data.update({curr:self.the_file[period]["tick"][curr][-100:]})

        return data

    def set_rank_file(self, data):
        
        if data["header"][0] == "other":
            self.rank_file["market_cap_usd"] = data["rank_mc"]
            self.rank_file["volume_24h_usd"] = data["rank_vol"]
        else :
            self.rank_file["""change_%s_%s"""%(data["header"][0], data["header"][1])] = data["rank"]
            
    def get_rank_file(self, item):
        return self.rank_file[item]
        
        
