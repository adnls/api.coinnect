import pymysql

class MyDb:

    def __init__(self):

        self.bdd="ubot"
        self.host="localhost"
        self.user=""
        self.password=""
        self.con = object()
        self.cur = object()
        
    def connect(self, user, password):
        
        self.user = user
        self.password = password

        self.con = pymysql.connect(host=self.host,
                                  user=self.user,
                                  password=self.password,
                                  db=self.bdd,
                                  cursorclass=pymysql.cursors.DictCursor)

        self.cur = self.con.cursor()
        
    def insert(self, sql):
        
        self.cur.execute(sql)
        
    def select(self, sql):

        rows = object()
        
        self.cur.execute(sql)
        rows = self.cur.fetchall()      

        return rows
        
    def save(self):

        self.con.commit()


    def quit(self):

        self.con.close()

    def truncate(self, table, top):

        self.cur.execute("""SELECT count(*) FROM %s""" % (table))
        count = self.cur.fetchall()
        

        if count[0]['count(*)'] > top:

            limit = count[0]['count(*)']-top           
            self.cur.execute("""DELETE FROM %s LIMIT %s"""%(table, limit))
            
    def bypass(self, nbr):

            sql_str = """INSERT INTO tick 
                (id, nom, nom_abreg, rang, cours_usd, cours_btc, 

                volume24h_usd, mc_usd, av_supp, total_supp, 

                change_1h, change_24h, change_7d) 
                SELECT id, nom, nom_abreg, rang, cours_usd, 

                cours_btc, volume24h_usd, mc_usd, av_supp, 

                total_supp, change_1h, change_24h, change_7d 
                FROM tick                
                ORDER BY dateheure DESC                
                LIMIT %s"""%nbr
            
            self.cur.execute(sql_str)

            sql_str = """INSERT INTO glob 
                        (totalmc, mcvolume24h, btcpct, activecurr, 

                        activeass, activemark) 
                        SELECT totalmc, mcvolume24h, btcpct, activecurr, 

                        activeass, activemark
                        FROM glob 
                        ORDER BY dateheure DESC 
                        LIMIT 1"""
                        
            self.cur.execute(sql_str)
                        
            
