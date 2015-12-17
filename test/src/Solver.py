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


#####
# DERP index
#
# Clock is driven by daily arrival or productPrices
# ... if a new day arrives, compute new portValue as at last close and calc the index
# ... then accumulate any coupons for the current positions
# ... then do any new trades on/before thisDate
#
#####
#
#init
#
userId = 137
cnxn   = DBconn().connect()
cursor = cnxn.cursor()

#
# get trades
#
tradeIndex = 0
cursor.execute("""
        select Date,t.ProductId,100*Position/IssuePrice Position
        from trade t join product using (productid)
        where InvestorId = ?
        order by Date,ProductId
        """,[userId])
trades = cursor.fetchall()
if not trades:
    print("NO trade DATA")
    exit()
firstDate = trades[0].Date
numTrades = len(trades)
#
# get productPrices
#
productPriceIndex = 0
cursor.execute("""
        select pp.*
        from productprices pp join trade using (productid)
        where InvestorId = ? and pp.Date >= ?
        order by Date,ProductId
        """,[userId,firstDate])
productPrices = cursor.fetchall()
if not productPrices:
    print("NO price DATA")
    exit()

#
# get coupons
#
couponIndex = 0
cursor.execute("""
        select pc.*
        from productcoupons pc join trade using (productid)
        where InvestorId = ? and pc.Date > ?
        order by Date,ProductId
        """,[userId,firstDate])
coupons = cursor.fetchall()
if not coupons:
    print("NO coupon DATA")
numCoupons = len(coupons)




#
# compute index
#
currentBids       = {}
currentAsks       = {}
currentPositions  = {}
sumCoupons        = 0.0
indexValue        = 1.0
portValue         = 0.0
previousDate      = firstDate
previousPortValue = 0.0
#
#  Clock is driven by daily arrival or productPrices
#
for productPrice in productPrices:
    # init
    thisDate = productPrice.Date
    #
    # ... if a new day arrives, compute new portValue as at last close and calc the index
    #
    if thisDate != previousDate:
        # add any new trades prior to thisDate
        while tradeIndex < numTrades and trades[tradeIndex].Date < thisDate:
            tradePid      = trades[tradeIndex].ProductId
            tradePosition = trades[tradeIndex].Position
            # ...init in case trade date is before firstPriceDate
            if tradePid not in currentBids:
                currentBids[tradePid] = 0.0
            if tradePid not in currentAsks:
                currentAsks[tradePid] = 0.0
            # ... init new positions
            if tradePosition > 0:
                previousPortValue += tradePosition * currentAsks[tradePid]
            else:
                previousPortValue += tradePosition * currentBids[tradePid]
            if tradePid in currentPositions:
                currentPositions[tradePid]  += tradePosition
            else:
                currentPositions[tradePid]   = tradePosition
            tradeIndex = tradeIndex + 1


        # calc portValue
        portValue = sumCoupons
        for pid,pos in currentPositions.items():
            portValue += currentBids[pid] * pos
        # calc index
        indexValue  *= portValue/previousPortValue
        print("Index:",previousDate,indexValue)
        #
        # save new index value
        #
        # cursor.execute("insert into products(id, name) values (?, ?)", 'pyodbc', 'awesome library')
        # cnxn.commit()
        #
        # init for next time
        previousDate       = thisDate
        previousPortValue  = portValue



    # accumulate any new coupons up to this date for existing positions
    # DOME: ccy conversion
    while couponIndex< numCoupons and [couponIndex].Date <= thisDate:
        sumCoupons += currentPositions[thisPid] * coupons[couponIndex].Amount
        couponIndex = couponIndex + 1


    # update for this price
    thisPid              = productPrice.ProductId
    currentBids[thisPid] = productPrice.Bid
    currentAsks[thisPid] = productPrice.Ask


    #
    # for matured products, get maturityPayoff and DateMatured
    #

