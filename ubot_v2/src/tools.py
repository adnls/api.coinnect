import math
import datetime
import decimal
import json

class Change:
    
    def nanInListItems(a, value):

        i=0
        while i<= len(a)-1:
            for key in a[i].keys():
               if math.isnan(a[i][key]) is True:
                  a[i][key] = value                  
            i+=1
        return a

    def nanInListSingleItem_1row(a, b, value):

        
        if math.isnan(a[0][b]) is True:
            a[0][b] = value                  
           
        return a
    
    def NoneInListItems(a, value):

        i=0
        while i<= len(a)-1:
            for key in a[i].keys():
               if a[i][key] is None:
                  a[i][key] = value                  
            i+=1
        return a

    def NoneInListItems_1row(a, value):

        
        for key in a[0].keys():
            if a[0][key] is None:
                a[0][key] = value                  
            
        return a


    def NoneInListSingleItem_1row(a, b, value):

        
        
        if a[0][b] is None:
            a[0][b] = value                  
            
        return a


    def charInListItems(a, old, new):

        i=0
        while i<= len(a)-1:
            for key in a[i].keys():                
                a[i][key] = a[i][key].replace(old, new) 
            i+=1
        return a

    def charInListSingleItem(a, b, old, new):

        i=0
        while i<= len(a)-1:                
            a[i][b] = a[i][b].replace(old, new) 
            i+=1
        return a

    def charInListKeys(a, old, new):

        i=0
        while i<=len(a)-1:
            a[i]={key.replace(old, new): a[i][key] for key in a[i].keys()}
            i+=1
        return a;

class Serialize:

    def json(data):

        i = 0
        while i<= len(data)-1:
            for key in data[i].keys():
                if isinstance(data[i][key], decimal.Decimal) or isinstance(data[i][key], datetime.datetime):
                    data[i][key] = str(data[i][key])
            i+= 1
            
        data = json.dumps(data)
        
        return data
        
        
