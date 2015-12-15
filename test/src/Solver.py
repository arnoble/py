import math
import pyodbc

class Solver:
    def demo(self):
        while True:
            a = int(input("a"))
            b = int(input("b"))
            c = int(input("c"))
            d = b**2 - 4*a*c
            if d>=0:
                disc = math.sqrt(d)
                root1 = (-b + disc) / (2 * a)
                root2 = (-b - disc) / (2 * a)
                print(root1, root2)
            else:
                print("error")
# Solver().demo()


class DBconn:
    def connect(self):
        return pyodbc.connect('DSN=newSp;PWD=ragtinmor')

# DERP index
userId = 3
cnxn   = DBconn().connect()
cursor = cnxn.cursor()
#cursor.execute("select * from user")

cursor.execute("""
        select Date,ProductId,Position
        from trade
        where InvestorId = ?
        order by ProductId,Date
        """,[userId])

rows = cursor.fetchall()
if not rows:
    print("NO DATA")
else:
    for row in rows:
        print('name:', row[1])      # access by column index
        print('name:', row.Date)   # or access by name

