from threading import Thread
from flask import Flask, request
import json
import pandas as pd
import cherrypy
import queue
from container import Container
import decimal

ctnr = Container()
coinnect = Flask("coinnect")       

@coinnect.route('/new_tick', methods=['POST'])
def handle_new_tick():      
    data = request.get_data()   
    data = json.loads(data)
    ctnr.set_the_file(data["period"], data["tick"], data["glob"])
    return "new_tick_handled", 200

@coinnect.route('/new_rank', methods=['POST'])
def handle_new_rank():      
    data = request.get_data()   
    data = json.loads(data)
    ctnr.set_rank_file(data)
    return "new_rank_handled", 200

@coinnect.route('/coinnect/ticker/', methods=['GET'])
def serve_ticker():
    payload = request.args.to_dict()
    print(payload)
    if len(payload) == 2: 
        for key, value in payload.items():
            print(key)
            print(value)
            if (key == "currency") and (value in ctnr.the_file["1min"]["tick"].keys()
                or value in ctnr.the_file["5min"]["tick"].keys()
                or value in ctnr.the_file["15min"]["tick"].keys()
                or value in ctnr.the_file["30min"]["tick"].keys()
                or value in ctnr.the_file["1h"]["tick"].keys()
                or value in ctnr.the_file["2h"]["tick"].keys()
                or value in ctnr.the_file["4h"]["tick"].keys()
                or value in ctnr.the_file["1d"]["tick"].keys()
                or value in ctnr.the_file["1w"]["tick"].keys()):
                    
                currency = value
                
            elif key == "period" and value in ctnr.the_file.keys():
                
                period = value
            elif key == "limit":
                try:
                    limit = int(value)
                    if limit <= 0 or limit > 100:
                        return "max limit is 100", 403
                except ValueError:
                    return "can't build payload", 404
                
            else:
                return "can't build payload", 404

        if "currency" in payload.keys() and "period" in payload.keys():
            result = ctnr.get_the_file(period, currency)
            
        elif "limit" in payload.keys() and "period" in payload.keys():
            result = ctnr.get_the_file_4(period, limit)
        else :
            return "can't build payload", 404

        return json.dumps(result), 200
        
    
    return "can't build payload", 404

@coinnect.route('/coinnect/global/', methods=['GET'])
def serve_global():
    payload = request.args.to_dict()
    print(payload)
    if len(payload) == 1 and "period" in payload.keys():
        if payload["period"] in ["1min", "5min", "15min", "30min", "1h", "2h", "4h", "1d", "1w"]:
            data = ctnr.get_the_file_2(payload["period"])
            if len(data)>0:
                return json.dumps(data), 200
            else :
                return "db empty", 403
        else :
            return "can't build payload", 404
    else:
        return "can't build payload", 404

@coinnect.route('/coinnect/index/currencies/', methods=['GET'])
def serve_index():
    payload = request.args.to_dict()
    print(payload)
    if len(payload) == 1 and "period" in payload.keys():
        if payload["period"] in ["1min", "5min", "15min", "30min", "1h", "2h", "4h", "1d", "1w"]:
            data = ctnr.get_the_file_3(payload["period"])
            if len(data)>0:
                return json.dumps(list(data)), 200
            else :
                return "db empty", 403
        else :
            return "can't build payload", 404
    else:
        return "can't build payload", 404

@coinnect.route('/coinnect/rank/', methods=['GET'])
def serve_rank():
    payload = request.args.to_dict()
    print(payload)
    
    for key, value in payload.items():
        
        if key == "by" and value in ctnr.rank_file.keys():
            by = value
            print(by)
            
        elif key == "order" and value in ["asc", "desc"]:
            if value == "desc": order = 1
            else: order = 0
            print(order)
            
        elif key == "limit":
            try:
                limit = int(value)
                print(limit)
                
            except ValueError:
                return "can't build payload", 404
        else:
            return "can't build payload", 404

    if len(payload) == 3 and "by" in payload.keys() and "order" in payload.keys() and "limit" in payload.keys():
        
        pass
    elif len(payload) == 2 and "by" in payload.keys():
        
        if "order" in payload.keys():
            limit = 100
    
        elif "limit" in payload.keys():
            order = 1
    
        else :
            return "can't build payload", 404
        
    elif len(payload) == 1 and "by" in payload.keys():
        order = 1; limit = 100
    
    else:
        return "can't build payload", 404

    result = ctnr.get_rank_file(by)
    
    if result is not None:
        if len(result) > 0:

            result = pd.DataFrame(result)
            result["value"] = result["value"].fillna(value='0')
            result["value"] = result["value"].astype(float)
            result["rank"] = result["value"].rank(method="dense", ascending = False).astype(int)
            result = result.sort_values("rank", ascending=order)
            result["value"] = result["value"].round(2)
            result["value"] = result["value"].map("{:.2f}".format)
            result = result[["rank", "id", "value"]].astype(str).head(limit).to_dict(orient="records")
            return json.dumps(result), 200
        else:
            return "db empty", 403
    else :
        return "db empty", 403
        
    
if __name__ == '__main__':

    def launch_server(port, app):
        #config serveur     (
        cherrypy.tree.graft(app, "/")

        cherrypy.server.unsubscribe()
        server = cherrypy._cpserver.Server()
                    
        server.socket_host = "0.0.0.0"
        server.socket_port = port
        server.thread_pool = 30

        server.subscribe()

        #lancement du serveur
        cherrypy.engine.start()
        cherrypy.engine.block()

    launch_server(5000, coinnect)
        
        
