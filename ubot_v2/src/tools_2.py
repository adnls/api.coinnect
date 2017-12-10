class Format_string:
    def for_tick_insert(rep):

        values = []
            
        for i in range(len(rep)):
            
            pos = 0
            values.append("""(DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00'),""")
            
            del rep[i]["last_updated"]
            
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
            
        return "".join(values)
