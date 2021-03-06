import sys
import math
import pyodbc
import mysql.connector
import datetime
# if run from command line
# if __name__ == "__main__":
#    import sys

#
# date from data    previousDate >= datetime.date(2015,11,10)
#
#
#
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
    def connect(self,dbServer):
        ## return pyodbc.connect('DSN=newSp;PWD=ragtinmor')
        #return pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};Login Prompt=False;User ID=root;Password=ragtinmor;Data Source=localhost;Database=sp')
        # pyodbc problem: negative doubles are not returned at all! so switched to MySQL odbc.connector
        #  ... http://dev.mysql.com/doc/connector-python/en/
        if dbServer == 'spCloud':
            return mysql.connector.connect(user='anoble', password='Ragtin_Mor14_Lucian', host='198.44.48.250', database='sp')
        else:
            return mysql.connector.connect(user='root', password='ragtinmor', host='localhost', database='sp')
#####
# OVERVIEW
#
#  0. DATA
#       Position column of 'trade' table records trades; in any self-consistent units you like, since they will be normalised to wts() portfolio weights
#
#  1. INIT
#       vector trades(),        date-ordered, selects all rows in 'trade' table for this 'InvestorId'
#       vector productPrices(), date-ordered, selects all rows in 'productprices' table where ProductId's in 'trade' table for this 'InvestorId'
#       vector coupons(),       date-ordered, selects all rows in 'productcoupons' table where ProductId's in 'trade' table for this 'InvestorId'
#
#       vector cumTrades(productId) stores cumulative Positions from trades() for each productId
#
#  2. LOOP through productPrices()
#       3. IF NEW date 't' from productPrices(), clock "ticks" and we recalc everything at t-1
#           3.1 update to t-1
#               cumTrades(productId) from trades to t-1
#               theseCoupons(productId) from coupons to t-1
#               cashflow = sumOverProductId(previousUnits*theseCoupons)
#               indexValue = sumOverProductId(previousUnits*previousMids)
#               currentWeights(productid) recalculated from cumTrades, so as to sum to 1.0
#               weightChanges  = currentWeights - previousWeights
#
#           3.2 IF absSum(WeightChanges) != 0.0   rebalance at t-1
#               currentUnits = indexValue * currentWeights / previousMids  # trade at MIDs, and record transaction costs below in cashflows
#               changeUnits  = currentUnits - previousUnits
#               cashflow -= positive(changeUnits) * (previousAsk - previousMid)
#               cashflow -= negative(changeUnits) * (previousMid - previousBid)
#
#           3.3 handle cashflows
#               slippage      = 1.0 - cashflow/indexValue
#               indexvalue    = indexvalue * slippage
#               previousUnits = currentUnits * slippage
#       4. update currentBid(productId),currentAsk(productId)
#
#####



#
# functions
#

#
#init
#
# CautiousIncome@jbrearley.com spCloud
# DERP@cubeinvesting.com    newSp
#
for a in sys.argv:
    print("Argv:",a)
if len(sys.argv)<3:
    print("Usage: loginEmail dbServer debug")
    exit()
userEmail = sys.argv[1]
dbServer  = sys.argv[2]
debug     = False
if len(sys.argv)>3:
    debug = True
cnxn   = DBconn().connect(dbServer)
cursor = cnxn.cursor(named_tuple=True)  # or user dictionary=True
q = "select UserId  from user where email = '"+sys.argv[1]+"'"
cursor.execute(q)
userId = cursor.fetchone()
if not userId:
    print("NO user with that email")
    exit()
userId = userId.UserId


#
# get info on matured products...in the end we just add MaturityPayoffs to the main sql below
# get productIds in trade table
#
#q = "select distinct ProductId from trade where investorid=" + format(userId) + " order by ProductId"
#cursor.execute(q)
#allProductIds = cursor.fetchall()
#if not allProductIds:
#    print("NO trade productIds")
#    exit()
#
# get matured products and payoffs
#
#allPidsStr = {str(row[0]) for row in allProductIds}
#q = "select ProductId,DateMatured,MaturityPayoff from product where productid in (" + ','.join(allPidsStr) + ") order by ProductId"
#cursor.execute(q)
#allMaturities = cursor.fetchall()

#
# get maturity dates
#
q = "select ProductId,DateMatured Date,MaturityPayoff from product where MaturityPayoff!=0 and productid in (select distinct ProductId from trade where investorid=" + format(userId) + ")"
cursor.execute(q)
allMaturities = cursor.fetchall()
productMaturityDate    = {}
for x in allMaturities:
    productMaturityDate[x.ProductId] = x.Date

#
# get trades in date,Position order so that SELLS come before BUYS
#
# vector trades(),        date-ordered, selects all rows in 'trade' table for this 'InvestorId'
tradeIndex = 0
q = "select * from trade where investorid=" + format(userId) + " order by Date,Position,ProductId"
cursor.execute(q)
trades = cursor.fetchall()
if not trades:
    print("NO trade DATA")
    exit()
#for (trade_date,pos) in cursor:
#  print("{}, was traded on {:%d %b %Y}".format(
#    pos, trade_date))

firstDate = trades[0].Date
numTrades = len(trades)


#
# get productPrices
#
# DOME: if 'trade' has a ProductIsaPrice=1 row we should BEWARE as there could also be a row with the same (real product) ProductId but ProductIsaPrice=0
q = "select * from trade t where InvestorId = "+format(userId)+" and t.ProductIsaPrice=0 and ProductId in (130)"
cursor.execute(q)
problemTrades = cursor.fetchall()
if len(problemTrades)>0:
    print("Illegal ProductId")
    exit()
#       ... leave for now as we only use UnderlyingId=130 for cash
#       vector productPrices(), date-ordered, selects all rows in 'productprices' table where ProductId's in 'trade' table for this 'InvestorId'
productPriceIndex = 0
q = "select pp.* from productprices pp join trade t using (productid) where InvestorId = "+format(userId)+" and pp.Date >= '"+format(firstDate)+"' and t.ProductIsaPrice=0 "
q += "union select p.UnderlyingId ProductId,p.Date,p.Price Bid,p.Price Ask from prices p join trade t on (t.productid=p.underlyingid) where InvestorId = "+format(userId)+" and p.Date >= '"+format(firstDate)+"' and t.ProductIsaPrice=1 "
q += "union select ProductId,DateMatured Date,MaturityPayoff Bid,MaturityPayoff Ask from product where MaturityPayoff!=0 and DateMatured!='0000-00-00'and productid in (select distinct ProductId from trade where investorid=" + format(userId) + ")"
q += " order by Date,ProductId"
cursor.execute(q)
productPrices = cursor.fetchall()
if not productPrices:
    print("NO price DATA")
    exit()
numPprices = len(productPrices)



#
# get coupons
#
#       vector coupons(),       date-ordered, selects all rows in 'productcoupons' table where ProductId's in 'trade' table for this 'InvestorId'
couponIndex = 0
q = "select distinct pc.ProductId,pc.Date,pc.Amount*IssuePrice Amount,pc.ccy from productcoupons pc join trade using (productid) join product using (productid) where InvestorId = "\
    +format(userId)+" and pc.Date >= '"+format(firstDate)+"' order by Date,ProductId"
cursor.execute(q)
coupons = cursor.fetchall()
if not coupons:
    print("NO coupon DATA")
numCoupons = len(coupons)


#
# update strings
#
q = "select IndexStrategyId from user where email='"+userEmail+"'"
cursor.execute(q)
results = cursor.fetchall()
if not results:
    print("NO strategy for",userEmail)
    exit()

strategyId = results[0][0]

cursor.execute("delete from indexstrategylevel   where indexstrategyid="+format(strategyId))
cursor.execute("delete from indexstrategyweights where indexstrategyid="+format(strategyId))
cnxn.commit()



#
# compute index
#
cumTrades            = {} # (productId) stores cumulative Positions from trades() for each productId
currentBids          = {}
currentAsks          = {}
previousBids         = {}
previousAsks         = {}
currentPositions     = {}
previousPositions    = {}
currentWeights       = {}
previousWeights      = {}
weightChanges        = {}
previousUnits        = {}
currentUnits         = {}
sumCoupons           = {}
startIndexValue      = 1000.0
indexValue           = startIndexValue
previousDate         = firstDate
isFirstDate          = True
#
#  2. LOOP through productPrices()
#
ppCounter=0
for productPrice in productPrices:
    ppCounter = ppCounter + 1
    thisDate  = productPrice.Date
    #       3. IF NEW date 't' from productPrices(),or if this is the lasat date, clock "ticks" and we recalc everything at t-1
    if thisDate != previousDate or ppCounter == numPprices:
        cashflow = 0.0
        #           3.1 update to t-1
        # ... accumulate any coupons ON_OR_BEFORE previousDate for previousPositions
        while couponIndex < numCoupons and coupons[couponIndex].Date <= previousDate:
            thisPid     = coupons[couponIndex].ProductId
            if thisPid in previousUnits:
                thisCoupon  = coupons[couponIndex].Amount
                if thisPid not in sumCoupons:
                    sumCoupons[thisPid] = thisCoupon
                else:
                    sumCoupons[thisPid] += thisCoupon
            couponIndex = couponIndex + 1
        for pid,y in sumCoupons.items():
            cashflow += previousUnits[pid] * sumCoupons[pid]


        #    ... reflect in currentPositions any trades ON_OR_BEFORE precedingDate
        #    ... new trades should be entered in TRADE table with POSITION set to the PREVAILING portfolio weight of this trade as at trade date
        while tradeIndex < numTrades and trades[tradeIndex].Date <= previousDate:
            tradeDate     = trades[tradeIndex].Date
            tradePid      = trades[tradeIndex].ProductId
            tradePosition = trades[tradeIndex].Position
            #        ... trades assumed executed on precedingDate
            #        ... raise ERROR if no pricing
            if tradePid not in previousBids or  tradePid not in previousAsks:
                print("No prices for",tradePid,"on",tradeDate)
                exit()

            if tradePid in previousPositions:
                currentPosition = previousPositions[tradePid]
            else:
                currentPosition = 0.0
            newPosition = currentPosition + tradePosition
            # ... update positions
            currentPositions[tradePid]   = newPosition
            tradeIndex = tradeIndex + 1


        #               currentWeights(productid) recalculated from cumTrades, so as to sum to 1.0
        sumPositions = 0.0
        for pid,pos in currentPositions.items():
            if pos>1 or pos<0:
                print("Illegal Position ",tradePid,"on",tradeDate)
                exit()
            sumPositions += currentPositions[pid]
        for pid,pos in currentPositions.items():
            currentWeights[pid] = currentPositions[pid] / sumPositions

        #               weightChanges  = currentWeights - previousWeights
        absSumWeightChanges = 0.0
        for pid,pos in currentPositions.items():
            if pid not in previousWeights:
                previousWeight = 0.0
            else:
                previousWeight = previousWeights[pid]
            weightChanges[pid]   = currentWeights[pid] - previousWeight
            absSumWeightChanges += abs(weightChanges[pid])

        #               indexValue = sumOverProductId(previousUnits*previousMids)
        thisIndexValue = 0.0
        for pid,pos in previousUnits.items():
            previousMid      = (previousBids[pid] + previousAsks[pid])/2.0
            thisIndexValue  += previousUnits[pid] * previousMid
        if isFirstDate:
            indexValue = startIndexValue
        else:
            indexValue = thisIndexValue


        #  3.2 IF absSum(WeightChanges) != 0.0   rebalance at t-1
        if absSumWeightChanges != 0.0:
            for pid,pos in currentWeights.items():
                previousMid       = (previousBids[pid] + previousAsks[pid])/2.0
                currentUnits[pid] = indexValue * pos / previousMid  # trade at MIDs, and record transaction costs below in cashflows
                if pid not in previousUnits:
                    previousUnit = 0.0
                else:
                    previousUnit = previousUnits[pid]
                unitsChange       = currentUnits[pid] - previousUnit
                if unitsChange > 0.0 :
                    cashflow -= unitsChange * (previousAsks[pid] - previousMid)
                elif unitsChange < 0.0 :
                    cashflow -= unitsChange * (previousMid - previousBids[pid])

        #  3.3 handle cashflows
        slippage      = 1.0 + cashflow/indexValue
        indexValue    = indexValue * slippage
        for pid,pos in currentUnits.items():
            currentUnits[pid] = currentUnits[pid] * slippage

        #
        # save new index value
        #
        if isFirstDate:
            isFirstDate          = False
            thisIndexValue       = startIndexValue
        else:
            thisIndexValue       = indexValue

        print("Index:",previousDate,thisIndexValue)
        weightsString =  "("+format(strategyId)+",20,'"+format(previousDate)+"',"+format(len(currentUnits))+")"
        levelsString  =  "("+format(strategyId)+",'"+format(previousDate)+"',"+format(thisIndexValue)+")"
        cursor.execute("insert into indexstrategyweights (IndexStrategyId,Underlyingid,Date,Weight) values "+weightsString+";")
        cursor.execute("insert into indexstrategylevel   (IndexStrategyId,Date,Level)               values "+levelsString+";")
        cnxn.commit()

        # init for next time
        # ... track positions
        for pid,pos in currentPositions.items():
            previousPositions[pid] = currentPositions[pid]
            previousWeights[pid]   = currentWeights[pid]
            previousUnits[pid]     = currentUnits[pid]
        # ... zero coupons
        for pid,y in sumCoupons.items():
            sumCoupons[pid] = 0.0
        # ... track bids, asks
        #for pid,x in currentBids.items():
        #    previousBids[pid] = x
        #for pid,x in currentAsks.items():
        #    previousAsks[pid] = x
        # ... track date
        previousDate       = thisDate


    # ... otherwise just record bid/ask for this productId
    thisPid              = productPrice.ProductId
    if thisPid not in productMaturityDate or thisDate < productMaturityDate[thisPid]:
        if productPrice.Bid > 0.0:
            #currentBids[thisPid] = productPrice.Bid
            #if thisPid not in previousBids:
            #    previousBids[thisPid] = productPrice.Bid
            #elif abs((previousBids[thisPid] - productPrice.Bid)/previousBids[thisPid]) > 0.3 :
            #    print("Date:",previousDate,"productId:",thisPid,"thisBid:",productPrice.Bid,"previousBid:",previousBids[thisPid])
            #    exit()
            if thisPid in previousBids and abs((previousBids[thisPid] - productPrice.Bid)/previousBids[thisPid]) > 0.3 :
                print("Date:",previousDate,"productId:",thisPid,"thisBid:",productPrice.Bid,"previousBid:",previousBids[thisPid])
                if input("continue? y/n") == "n":
                  exit()
            previousBids[thisPid] = productPrice.Bid

        if productPrice.Ask > 0.0:
            #currentAsks[thisPid] = productPrice.Ask
            #if thisPid not in previousAsks:
            #    previousAsks[thisPid] = productPrice.Ask
            #elif abs((previousAsks[thisPid] - productPrice.Ask)/previousAsks[thisPid]) > 0.3 :
            #    print("Date:",previousDate,"productId:",thisPid,"thisAsk:",productPrice.Ask,"previousAsk:",previousAsks[thisPid])
            #    exit()
            if thisPid in previousAsks and abs((previousAsks[thisPid] - productPrice.Ask)/previousAsks[thisPid]) > 0.3 :
                print("Date:",previousDate,"productId:",thisPid,"thisAsk:",productPrice.Ask,"previousAsk:",previousAsks[thisPid])
                if input("continue? y/n") == "n":
                  exit()
            previousAsks[thisPid] = productPrice.Ask
    #
    # for matured products, get maturityPayoff and DateMatured
    #


# cursor.execute(updateLevelsString,multi=True)
# cursor.execute(updateWeightsString,multi=True)
cnxn.commit()
cursor.close()
cnxn.close()
