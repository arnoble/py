import sys
import math
import pyodbc
import mysql.connector
import datetime
# if run from command line
# if __name__ == "__main__":
#    import sys

#
# date from data    previousDate >= datetime.date(2016,10,03)
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
            return mysql.connector.connect(user='anoble', password='Ragtin_Mor14_Lucian', host='166.63.0.149', database='sp')
        else:
            return mysql.connector.connect(user='root', password='ragtinmor', host='localhost', database='sp')
#####
# OVERVIEW
#
#  0. DATA
#       'trade' table records trades: date,ProductId,ProductIsaPrice(1 for Money, 0 for Products),NumUnits (use any value for Money trades),Money
#
#  1. INIT
#       vector trades(),        date-ordered, selects all rows in 'trade' table for this 'InvestorId'
#       vector productPrices(), date-ordered, selects all rows in 'productprices' table where ProductId's in 'trade' table for this 'InvestorId'
#       vector coupons(),       date-ordered, selects all rows in 'productcoupons' table where ProductId's in 'trade' table for this 'InvestorId'
#
#
#  2. LOOP through productPrices()
#       3. IF NEW date 't' from productPrices(), clock "ticks" and we recalc everything at t-1
#           3.1 update to t-1
#               process coupons to t-1 -> create new CashAsset NumUnits
#               calc NAV, doing FX
#               IndexReturn = closingNAV/openingNAV
#               process trades(productId) from trades to t-1
#               -> CashAsset trades are subscriptions/redemptions create/redeem FundUnits at NAV
#               -> Product   trades credit/debit CashAsset/ProductUnits
#
#       4. calc/save current momeyWeights
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
q = "select UserId  from user where email = '"+userEmail+"'"
cursor.execute(q)
userId = cursor.fetchone()
if not userId:
    print("NO user with that email")
    exit()
userId = userId.UserId


#
# strategy static data
#
q = "select u.IndexStrategyId,s.ccy from user u join indexstrategy s using (indexstrategyid) where email='"+userEmail+"'"
cursor.execute(q)
results = cursor.fetchone()
if not results:
    print("NO strategy for",userEmail)
    exit()

strategyId  = results.IndexStrategyId
strategyCcy = results.ccy

cursor.execute("delete from indexstrategylevel   where indexstrategyid="+format(strategyId))
cursor.execute("delete from indexstrategyweights where indexstrategyid="+format(strategyId))
cnxn.commit()


#
# get lastdatadate
#
q = "select max(LastDataDate) LastDataDate from cashflows "
cursor.execute(q)
any = cursor.fetchone()
if not any:
    print("NO LastDataDate")
    exit()
lastDataDate = any.LastDataDate


#
# get any product maturity dates
#
q  = "select ProductId,DateMatured Date,MaturityPayoff from product where MaturityPayoff!=0 and productid in "
q += "(select distinct ProductId from trade where investorid=" + format(userId) + ")"
cursor.execute(q)
allMaturities = cursor.fetchall()
productMaturityDate    = {}
for x in allMaturities:
    productMaturityDate[x.ProductId] = x.Date

#
# get any product crossRates
#
q = "select ProductId,ccy,BaseCcy from product where productid in (select distinct ProductId from trade where investorid=" + format(userId) + ")"
cursor.execute(q)
any = cursor.fetchall()
productCcy             = {}
crossRates             = {}
productCrossRate       = {}
for x in any:
    productCcy[x.ProductId] = x.ccy
    if x.ccy != strategyCcy and x.BaseCcy != strategyCcy:
        crossRateName = x.ccy + strategyCcy
        productCrossRate[x.ProductId] = crossRateName
        if crossRateName not in crossRates:
            q = "select UnderlyingId from underlying where name='" + crossRateName +"'"
            cursor.execute(q)
            any = cursor.fetchone()
            if not any:
                print("NO underlyingId for ",crossRateName)
                exit()
            q = "select * from prices where Underlyingid=" + str(any.UnderlyingId) +" order by date"
            cursor.execute(q)
            any = cursor.fetchall()
            if not any:
                print("NO prices for ",crossRateName)
                exit()
            crossRates[crossRateName] = {}
            for y in any:
                crossRates[crossRateName][y.Date] = y.Price


#
# get trades
# create trades() in date,Position order so that SELLS come before BUYS
#
tradeIndex = 0
q = "select * from trade where investorid=" + format(userId) + " order by Date,Position,ProductId"
cursor.execute(q)
trades = cursor.fetchall()
if not trades:
    print("NO trade DATA")
    exit()
#for (trade_date,pos) in cursor:
#  print("{}, was traded on {:%d %b %Y}".format(pos, trade_date))
firstDate = trades[0].Date
numTrades = len(trades)


#
# detect any problem entries in sp.trade table
#  - to record cash in 'sp.trade' table we set ProductId=130 and ProductIsaPrice=1
#  - so we need to check no REAL product with 1d=130 has been added tp sp.trade table
q = "select * from trade t where InvestorId = "+format(userId)+" and t.ProductIsaPrice=0 and ProductId in (130)"
cursor.execute(q)
problemTrades = cursor.fetchall()
if len(problemTrades)>0:
    print("Illegal ProductId")
    exit()    #  ... leave for now as we only use UnderlyingId=130 for cash

#
# get productPrices
# create productPrices(), date-ordered, selects all rows in 'productprices' table where ProductId's in 'trade' table for this 'InvestorId'
#   ... adding values from prices table if ProductIsaPrice=1
productPriceIndex = 0
q = "select pp.* from productprices pp join trade t using (productid) where InvestorId = "+format(userId)+" and pp.Date >= '"+format(firstDate)+"' and pp.Date <= '"+format(lastDataDate)+"' and t.ProductIsaPrice=0 "
q += "union select p.UnderlyingId ProductId,p.Date,p.Price Bid,p.Price Ask from prices p join trade t on (t.productid=p.underlyingid) where InvestorId = "+format(userId)+" and p.Date >= '"+format(firstDate)+"' and t.ProductIsaPrice=1 "
q += "union select ProductId,DateMatured Date,MaturityPayoff Bid,MaturityPayoff Ask from product where MaturityPayoff!=0 and productid in (select distinct ProductId from trade where ProductIsaPrice=0 and investorid=" + format(userId) + ")"
q += " order by Date,ProductId"
cursor.execute(q)
productPrices = cursor.fetchall()
if not productPrices:
    print("NO price DATA")
    exit()
numPprices = len(productPrices)



#
# get coupons
# create coupons(),       date-ordered, selects all rows in 'productcoupons' table where ProductId's in 'trade' table for this 'InvestorId'
couponIndex = 0
q = "select distinct pc.ProductId,pc.Date,pc.Amount*IssuePrice"
q += " Amount,pc.ccy from productcoupons pc join trade using (productid) join product p using (productid) where InvestorId = "\
    +format(userId)+" and pc.Date >= '"+format(firstDate)+"' order by Date,ProductId"
cursor.execute(q)
coupons = cursor.fetchall()
if not coupons:
    print("NO coupon DATA")
numCoupons = len(coupons)





#
# init
#
productBids           = {}
productAsks           = {}
productWeights        = {}
weightChanges         = {}
sumCoupons            = {}
startIndexValue       = 1000.0
fundUnits             = 1.0
oldNav                = 1.0
cashPid               = 130
productUnits          = {}
productUnits[cashPid] = 0.0;
indexValue            = startIndexValue
previousDate          = firstDate
isFirstDate           = True

#
#  2. LOOP through productPrices()
#
ppCounter=0
for productPrice in productPrices:
    ppCounter = ppCounter + 1
    thisDate  = productPrice.Date
    # 3. IF NEW date 't' from productPrices(),or if this is the last date, clock "ticks" and we recalc everything at t-1
    if thisDate != previousDate or ppCounter == numPprices:
        cashflow = 0.0
        # 3.1 update to t-1
        # ... accumulate any coupons ON_OR_BEFORE previousDate for productPositions
        while couponIndex < numCoupons and coupons[couponIndex].Date <= previousDate:
            thisPid     = coupons[couponIndex].ProductId
            if thisPid in productUnits:
                thisCoupon  = coupons[couponIndex].Amount
                thisCcy     = coupons[couponIndex].ccy
                if thisPid in productCrossRate:
                    thisCoupon *= crossRates[productCrossRate[thisPid]][previousDate]
                if thisPid not in sumCoupons:
                    sumCoupons[thisPid] = thisCoupon
                else:
                    sumCoupons[thisPid] += thisCoupon
            couponIndex = couponIndex + 1
        for pid,y in sumCoupons.items():
            cashflow += productUnits[pid] * sumCoupons[pid]
        # ... and buy some cash units with the coupons
        productUnits[cashPid]  += cashflow / productAsks[cashPid]

        #  calc NAV at MIDs, doing FX
        thisAssetValue  = 0.0
        for pid,pos in productUnits.items():
            productMid = (productBids[pid] + productAsks[pid])/2.0
            if pid in productCrossRate:
                if previousDate not in crossRates[productCrossRate[pid]]:
                    print(previousDate,"not in crossRates for",pid)
                    exit(111)
                productMid *= crossRates[productCrossRate[pid]][previousDate]
            thisAssetValue  += productUnits[pid] * productMid
        if fundUnits <= 0 or thisAssetValue<=0.0:
            thisNav   = 1.0
            fundUnits = 1.0
        else:
            thisNav = thisAssetValue/fundUnits
        if isFirstDate:
            isFirstDate = False
            indexValue  = startIndexValue
        else:
            indexValue *= thisNav/oldNav
        oldNav            = thisNav

        #
        # save new index value
        #
        print("Index:",previousDate,indexValue,thisAssetValue)
        weightsString =  "("+format(strategyId)+",20,'"+format(previousDate)+"',"+format(0)+")"
        levelsString  =  "("+format(strategyId)+",'"+format(previousDate)+"',"+format(indexValue)+")"
        cursor.execute("insert into indexstrategyweights (IndexStrategyId,Underlyingid,Date,Weight) values "+weightsString+";")
        cursor.execute("insert into indexstrategylevel   (IndexStrategyId,Date,Level)               values "+levelsString+";")
        cnxn.commit()

        #  new trades ... reflect in currentPositions any trades ON_OR_BEFORE precedingDate
        traceAssetValue = thisAssetValue
        while tradeIndex < numTrades and trades[tradeIndex].Date <= previousDate:
            tradeDate     = trades[tradeIndex].Date
            tradePid      = trades[tradeIndex].ProductId
            tradeUnits    = trades[tradeIndex].NumUnits
            tradeMoney    = trades[tradeIndex].Money
            #        ... trades assumed executed on precedingDate
            #        ... raise ERROR if no pricing
            if tradePid not in productBids or  tradePid not in productAsks:
                print("No prices for",tradePid,"on",tradeDate,"assuming 100.00")
                productBids[tradePid] = 100.00
                productAsks[tradePid] = 100.00
                # exit()
            # ... update positions
            if tradePid not in productUnits:
                productUnits[tradePid] = 0.0
            if tradePid == cashPid:
                # cash creates/redeems fund units at oldNav
                fundUnits += tradeMoney/oldNav
                if tradeMoney>0.0:
                    # positive tradeMoney means BUY
                    tradeUnits = tradeMoney/ productAsks[cashPid]
                    tradePrice = productAsks[cashPid]
                else:
                    # negative tradeMoney means SELL
                    tradeUnits = tradeMoney/ productBids[cashPid]
                    tradePrice = productBids[cashPid]
            else:
                # positive/negative tradeMoney reduces/increases the cashPid
                if tradeUnits == 0:
                    print(previousDate,tradePid," cannot have zero #units traded")
                    exit(112)
                tradePrice              =  tradeMoney/tradeUnits
                productUnits[cashPid]  -= tradeMoney / productBids[cashPid]

            productUnits[tradePid] += tradeUnits

            # maybe check on assetValue
            if True:
                someAssetValue  = 0.0
                for pid,pos in productUnits.items():
                    productMid = (productBids[pid] + productAsks[pid])/2.0
                    if pid in productCrossRate:
                        if previousDate not in crossRates[productCrossRate[pid]]:
                            print(previousDate,"not in crossRates for",pid)
                            exit(111)
                        productMid *= crossRates[productCrossRate[pid]][previousDate]
                    someAssetValue  += productUnits[pid] * productMid
                thisMid      = (productBids[tradePid] + productAsks[tradePid])/2.0
                cashMid      = (productBids[cashPid] + productAsks[cashPid])/2.0
                thisBidOffer = tradeUnits*(tradePrice-thisMid)
                assetValueDiff = someAssetValue - traceAssetValue
                print(tradeDate,
                      "NAV:",          '{:10.2f}'.format(someAssetValue),
                      "diff:",         '{:10.2f}'.format(assetValueDiff),
                      "BO:",           '{:10.2f}'.format(thisBidOffer),
                      "trading",       '{:10.2f}'.format(tradeUnits),
                      "of",            '{:4d}'.format(tradePid),
                      "at",            '{:10.2f}'.format(tradePrice),
                      "MID:",          '{:10.2f}'.format(thisMid),
                      "value:",        '{:10.2f}'.format(tradeMoney),
                      "cash:",         '{:10.2f}'.format(productUnits[cashPid]*cashMid) )
                traceAssetValue = someAssetValue
            tradeIndex              = tradeIndex + 1

        # init for next time
        # ... zero coupons
        for pid,y in sumCoupons.items():
            sumCoupons[pid] = 0.0
        previousDate      = thisDate


    # not a recalc date - just record bid/ask for this productId
    thisPid  = productPrice.ProductId
    if thisPid not in productMaturityDate or thisDate <= productMaturityDate[thisPid]:
        if productPrice.Bid > 0.0:
            if thisPid in productBids and abs((productBids[thisPid] - productPrice.Bid)/productBids[thisPid]) > 0.3 :
                print("Strange price on Date:",previousDate,"productId:",thisPid,"thisBid:",productPrice.Bid,"productBid:",productBids[thisPid])
                exit(112)
            productBids[thisPid] = productPrice.Bid

        if productPrice.Ask > 0.0:
            if thisPid in productAsks and abs((productAsks[thisPid] - productPrice.Ask)/productAsks[thisPid]) > 0.3 :
                print("Strange price on Date:",previousDate,"productId:",thisPid,"thisAsk:",productPrice.Ask,"productAsk:",productAsks[thisPid])
                exit(113)
            productAsks[thisPid] = productPrice.Ask


# finally calc and save moneyWeights using MIDs
q = "update trade set Position=0 where InvestorId = "+format(userId)+" "
cursor.execute(q)
for pid,pos in productUnits.items():
    productMid      = (productBids[pid] + productAsks[pid])/2.0
    if pid in productCrossRate:
        productMid *= crossRates[productCrossRate[pid]][previousDate]
    thisWeight  = (productUnits[pid] * productMid) / thisAssetValue
    q = "update trade set Position=" + format(thisWeight) + " where ProductId = "+format(pid)+" and InvestorId="+format(userId)+" limit 1"
    cursor.execute(q)


# tidy up
cnxn.commit()
cursor.close()
cnxn.close()
