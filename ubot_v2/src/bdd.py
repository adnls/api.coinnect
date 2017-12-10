import pymysql

class MyBdd:

    def __init__(self):

        self.bdd="ubot"
        self.host="localhost"
        self.user=""
        self.password=""
        self.con = object()
        self.cur = object()
        
    def useBdd(self, user, password):
        
        self.user = user
        self.password = password

        self.con = pymysql.connect(host=self.host,
                                  user=self.user,
                                  password=self.password,
                                  db=self.bdd,
                                  cursorclass=pymysql.cursors.DictCursor)

        self.cur = self.con.cursor()
        
    def postQuery(self, sql):
        
        self.cur.execute(sql)
        
    def getQuery(self, sql):

        rows = object()
        
        self.cur.execute(sql)
        rows = self.cur.fetchall()      

        return rows
        
    def savePostQuery(self):

        self.con.commit()


    def closeCommunication(self):

        self.con.close()

    def truncTable(self, table, top):

        self.cur.execute("""SELECT count(DISTINCT dateheure) FROM %s"""%(table))
        count = self.cur.fetchall()
        count = count[0]['count(DISTINCT dateheure)']
        
        if count > top:
            self.cur.execute("""SET @ref = (SELECT MIN(dateheure) FROM %s);
                                DELETE FROM %s WHERE dateheure = @ref"""%(table, table))
            
    def bypass(self):

            sql =        """SET @ref = (SELECT MAX(dateheure) FROM tick_test);

                            INSERT INTO tick_test
                            SELECT dateheure + INTERVAL 1 MINUTE, id, nom, nom_abreg, rang, cours_usd, 
                            cours_btc, volume24h_usd, mc_usd, av_supp, 
                            total_supp, max_supp, change_1h, change_24h, change_7d 
                            FROM tick_test
                            WHERE dateheure = @ref"""
            print("in the bypass")
            self.cur.execute(sql)

            sql =    """SET @ref = (SELECT MAX(dateheure) FROM glob_test);

                            INSERT INTO glob_test                         
                            SELECT dateheure + INTERVAL 1 MINUTE,
                            totalmc, mcvolume24h, btcpct, activecurr, 
                            activeass, activemark
                            FROM glob_test
                            WHERE dateheure = @ref"""
                        
            self.cur.execute(sql)
                        
            
