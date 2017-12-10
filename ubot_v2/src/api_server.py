from threading import Thread
from flask import Flask, request
import container
from core import Threader

# init container ->
ctn = container.Container()

# def flask app ->
app = Flask(__name__)

#//////////////////////////////// path 1 ->
@app.route('/glob_1min', methods=['GET'])
def send_glob_1min():

    data = ctn.get_glob_1min()                                    
    return data

#//////////////////////////////// path 2 ->
@app.route('/glob_5min', methods=['GET'])
def send_glob_5min():

    data = ctn.get_glob_5min()                                    
    return data 

#//////////////////////////////// path 3 ->
@app.route('/glob_15min', methods=['GET'])
def send_glob_15min():

    data = ctn.get_glob_15min()                                    
    return data 

#//////////////////////////////// path 4 ->
@app.route('/glob_30min', methods=['GET'])
def send_glob_30min():

    data = ctn.get_glob_30min()                                    
    return data 

#//////////////////////////////// path 5 ->
@app.route('/glob_1h', methods=['GET'])
def send_glob_1h():

    data = ctn.get_glob_1h()                                    
    return data 

#//////////////////////////////// path 6 ->
@app.route('/glob_2h', methods=['GET'])
def send_glob_2h():

    data = ctn.get_glob_2h()                                    
    return data 

#//////////////////////////////// path 7 ->
@app.route('/glob_4h', methods=['GET'])
def send_glob_4h():

    data = ctn.get_glob_4h()                                    
    return data 

#//////////////////////////////// path 8 ->
@app.route('/glob_1d', methods=['GET'])
def send_glob_1d():

    data = ctn.get_glob_1d()                                    
    return data 

#//////////////////////////////// path 9 ->
@app.route('/glob_1w', methods=['GET'])
def send_glob_1w():

    data = ctn.get_glob_1w()                                    
    return data 

#//////////////////////////////// path 10 ->
@app.route('/tick', methods=['GET'])
def send_tick():

    data = ctn.get_tick()                                    
    return data 

#//////////////////////////////// path 11 ->
@app.route('/tick_1min', methods=['GET'])
def send_tick_1min():

    data = ctn.get_tick_1min()                                    
    return data 
        
#//////////////////////////////// path 12 ->
@app.route('/tick_5min', methods=['GET'])
def send_tick_5min():

    data = ctn.get_tick_5min()                                    
    return data 

#//////////////////////////////// path 13 ->
@app.route('/tick_15min', methods=['GET'])
def send_tick_15min():

    data = ctn.get_tick_15min()                                    
    return data 

#//////////////////////////////// path 14 ->
@app.route('/tick_30min', methods=['GET'])
def send_tick_30min():

    data = ctn.get_tick_30min()                                    
    return data 

#//////////////////////////////// path 15 ->
@app.route('/tick_1h', methods=['GET'])
def send_tick_1h():

    data = ctn.get_tick_1h()                                    
    return data 

#//////////////////////////////// path 16 ->
@app.route('/tick_2h', methods=['GET'])
def send_tick_2h():

    data = ctn.get_tick_2h()                                    
    return data 

#//////////////////////////////// path 17 ->
@app.route('/tick_4h', methods=['GET'])
def send_tick_4h():

    data = ctn.get_tick_4h()                                    
    return data

#//////////////////////////////// path 18 ->
@app.route('/tick_1d', methods=['GET'])
def send_tick_1d():

    data = ctn.get_tick_1d()                                    
    return data

#//////////////////////////////// path 19 ->
@app.route('/tick_1w', methods=['GET'])
def send_tick_1w():

    data = ctn.get_tick_1w()                                    
    return data

#////////////////////////////////////
#////////////////////////////////////
if __name__ == "__main__":

    # def thread 1 ->
    class Api_client(Thread):
  
        def __init__(self):

            Thread.__init__(self)

        def run(self):

            #my threader
            Threader.threader(ctn)

    #instanciation des threads ->
    thread_1 = Api_client()


    thread_1.start()
    app.run()
