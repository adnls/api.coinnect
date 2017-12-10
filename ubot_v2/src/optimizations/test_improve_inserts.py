import requests
import db
import datetime

mydb = db.MyDb()
mydb.connect("adnls", "123azerty")

rep = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=1000")
rep = rep.json()

values = []

for i in range(len(rep)):
    
    pos = 0
    values.append("(")
    
    del rep[i]["last_updated"]
    del rep[i]["max_supply"]

    for key in rep[i].keys():
        
        if rep[i][key] == None:
            rep[i][key] = "NULL"

        if key in ["id", "name", "symbol"]:
            rep[i][key] = rep[i][key].replace("-", "_")
            rep[i][key] = rep[i][key].replace("'", "_")
            
        if pos not in range(3) or rep[i][key] == "NULL":
            
            values.append(rep[i][key])
        else :
            values.append("""'%s'"""%rep[i][key])

        pos += 1

        if pos != len(rep[i]):

            values.append(",")
        else :
            
            values.append(")")

    if i != len(rep)-1:
        
        values.append(",")
    
values = "".join(values)

sql = """INSERT INTO tick
         (id, nom, nom_abreg, rang, cours_usd, cours_btc,
           volume24h_usd, mc_usd, av_supp, total_supp, change_1h,
           change_24h, change_7d)
         VALUES %s"""%values

mydb.insert(sql)
mydb.save()

mydb.quit()

