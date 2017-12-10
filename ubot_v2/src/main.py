from api_server import app, ctn
import cherrypy
from core import Threader
from threading import Thread

# def thread 1 ->
class Api_client(Thread):
  
    def __init__(self):

        Thread.__init__(self)

    def run(self):

        Threader.threader(ctn)


#création du thread ->
thread_1 = Api_client()

#lancement du thread ->
thread_1.start()

#config serveur     
cherrypy.tree.graft(app, "/")

cherrypy.server.unsubscribe()
server = cherrypy._cpserver.Server()
            
server.socket_host = "0.0.0.0"
server.socket_port = 8080
server.thread_pool = 30

server.subscribe()

#lancement du serveur
cherrypy.engine.start()
cherrypy.engine.block()
