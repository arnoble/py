import sys
import math
import pyodbc
import mysql.connector
import datetime
# if run from command line
# if __name__ == "__main__":
#    import sys

#
# date from data    previousDate == datetime.date(2016,07,29)
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
    print("Usage: loginEmail dbServer <debug>")
    exit()
userEmail = sys.argv[1]
dbServer  = sys.argv[2]
debug     = False
if len(sys.argv)>3:
    debug = True
cnxn   = DBconn().connect(dbServer)
cursor = cnxn.cursor(named_tuple=True)  # or user dictionary=True

#
# strategy static data
#
q = "select u.IndexStrategyId,s.ccy,s.InvestorId from user u join indexstrategy s using (indexstrategyid) where email='"+userEmail+"'"
cursor.execute(q)
results = cursor.fetchone()
if not results:
    print("NO strategy for",userEmail)
    exit()

strategyId  = results.IndexStrategyId
strategyCcy = results.ccy
userId      = results.InvestorId
# coupon income file
couponFile = open("coupons"+str(strategyId)+".txt","w")
# p&l detail file
pAndLFile = open("pAndL"+str(strategyId)+".txt","w")
# p&l summary file
pAndLsummaryFile = open("pAndLsummary"+str(strategyId)+".txt","w")

cursor.execute("delete from indexstrategylevel   where indexstrategyid="+format(strategyId))
cursor.execute("delete from indexstrategyweights where indexstrategyid="+format(strategyId))
cnxn.commit()


#
# get lastdatadate
#
q = "select max(Date) LastDataDate from prices where UnderlyingId=1 "
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
q += "(select distinct ProductId from trade where investorid=" + format(userId) + " and ProductIsAPrice=0)"
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
q = "select * from trade where investorid=" + format(userId) + " order by Date,NumUnits desc,ProductId"
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
q  = "select pp.* from productprices pp join trade t using (productid) where InvestorId = "+format(userId)+" and pp.Date >= '"+format(firstDate)
q += "' and pp.Date <= '"+format(lastDataDate)+"' and t.ProductIsaPrice=0 "
# ChoiceA for real cash index use this:
#  q += "union select p.UnderlyingId ProductId,p.Date,p.Price Bid,p.Price Ask from prices p join trade t on (t.productid=p.underlyingid) "
# ChoiceB or if cash earns no interest, use this:
q += "union select p.UnderlyingId ProductId,p.Date,1.0 Bid,1.0 Ask from prices p join trade t on (t.productid=p.underlyingid) "
q += "where InvestorId = "+format(userId)+" and p.Date >= '"+format(firstDate)+"' and t.ProductIsaPrice=1 "
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
totalCouponCashflow   = 0.0
totalBidOffer         = 0.0
totalPandL            = 0.0
couponCashflow        = {}
fundUnits             = 1.0
oldNav                = 1.0
cashPid               = 130
productUnits          = {}
productUnits[cashPid] = 0.0;
productValues         = {}
productCosts          = {}
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
            anyValue        = productUnits[pid] * sumCoupons[pid]
            if anyValue != 0.0:
                if pid in couponCashflow:
                    couponCashflow[pid] += anyValue
                else:
                    couponCashflow[pid]  = anyValue
                totalCouponCashflow += anyValue
                cashflow            += anyValue
                couponFile.write( "\nOn:" + previousDate.strftime('%Y-%m-%d') + " Coupon of" + '{:10.2f}'.format(sumCoupons[pid]) + " on" + '{:10.2f}'.format(productUnits[pid]) + " units of ProductId " + str(pid) + " CouponCashflow:" + '{:10.2f}'.format(anyValue) + " CumulativeCouponCash:" + '{:10.2f}'.format(totalCouponCashflow))

        # ... and buy some cash units with the coupons
        if cashflow != 0.0 :
            productUnits[cashPid]  += cashflow / productAsks[cashPid]

        #  calc NAV at MIDs, doing FX
        thisAssetValue  = 0.0
        for pid,pos in productUnits.items():
            productMid = (productBids[pid] + productAsks[pid])/2.0
            if pid in productCrossRate:
                if previousDate not in crossRates[productCrossRate[pid]]:
                    lessThanDates = {x for x in crossRates[productCrossRate[pid]] if x< previousDate}
                    useThisDate   = max(lessThanDates)
                    print(previousDate,"not in crossRates for",pid,"using rate for ",useThisDate)
                    # exit(111)
                else:
                    useThisDate = previousDate
                productMid *= crossRates[productCrossRate[pid]][useThisDate]
            anyValue           = productUnits[pid] * productMid
            productValues[pid] = anyValue
            thisAssetValue    += anyValue
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
        cashMid        = (productBids[cashPid] + productAsks[cashPid])/2.0
        thisString = "On:" + previousDate.strftime('%Y-%m-%d') + " Index:" + '{:10.2f}'.format(indexValue) + " PortfolioValue:" + '{:10.2f}'.format(thisAssetValue) + " CouponCashflow:" + '{:10.2f}'.format(totalCouponCashflow) + " exCouponCashflow:" + '{:10.2f}'.format(thisAssetValue-totalCouponCashflow) + " BidOffer (positive=cost):" + '{:10.2f}'.format(totalBidOffer) + "Cash:" + '{:10.2f}'.format(productUnits[cashPid]*cashMid)
        print( thisString )
        pAndLFile.write( "\n" + thisString )
        weightsString =  "("+format(strategyId)+",20,'"+format(previousDate)+"',"+format(0)+")"
        levelsString  =  "("+format(strategyId)+",'"+format(previousDate)+"',"+format(indexValue)+")"
        cursor.execute("insert into indexstrategyweights (IndexStrategyId,Underlyingid,Date,Weight) values "+weightsString+";")
        cursor.execute("insert into indexstrategylevel   (IndexStrategyId,Date,Level)               values "+levelsString+";")
        cnxn.commit()

        # output detailed unrealised pandl
        totalUnrealisedPandl = 0.0
        cumulativeProductValue = 0.0
        for pid,pos in productUnits.items():
            if pid != cashPid and round(pos,1) != 0.0:
                thisMid        = (productBids[pid] + productAsks[pid])/2.0
                cumulativeProductValue += pos * thisMid
                pAndL = productValues[pid] - productCosts[pid]
                totalUnrealisedPandl += pAndL
                pAndLFile.write( "\nUnrealised:" + '{:10.2f}'.format(productUnits[pid]) + " units of ProductId " + str(pid) + " Cost:" + '{:10.2f}'.format(productCosts[pid]) + " Value:" + '{:10.2f}'.format(productValues[pid]) + " UnrealisedPandL:" + '{:10.2f}'.format(pAndL)  + " CumulativeUnrealisedPandL:" + '{:10.2f}'.format(totalUnrealisedPandl))
                if ppCounter == numPprices:
                    pAndLsummaryFile.write( "\nUnrealised:" + '{:10.2f}'.format(productUnits[pid]) + " units of ProductId " + str(pid) + " Cost:" + '{:10.2f}'.format(productCosts[pid]) + " Value:" + '{:10.2f}'.format(productValues[pid]) + " UnrealisedPandL:" + '{:10.2f}'.format(pAndL)  + " CumulativeUnrealisedPandL:" + '{:10.2f}'.format(totalUnrealisedPandl))
        # add final reconciliation
        valueChangeShouldBe = totalCouponCashflow + totalPandL + totalUnrealisedPandl
        if abs(1000000 + valueChangeShouldBe - thisAssetValue) > thisAssetValue*0.01 and thisAssetValue != 0.0:
            anyValue = 1
        pAndLFile.write( "\nIndex:" + '{:10.2f}'.format(indexValue) + " TotalValue:" + '{:10.2f}'.format(thisAssetValue)  + " ProductValue:" + '{:10.2f}'.format(cumulativeProductValue) + " Cash:" + '{:10.2f}'.format(productUnits[cashPid]*cashMid) + " AllPandL (unrealised+realised+coupons:" + '{:10.2f}'.format(valueChangeShouldBe) + " UnrealisedPandL:" + '{:10.2f}'.format(totalUnrealisedPandl) + " RealisedPandL:" + '{:10.2f}'.format(totalPandL)  + " Coupons:" + '{:10.2f}'.format(totalCouponCashflow)+ "       MEMO:BidOffer (positive=cost):" + '{:10.2f}'.format(totalBidOffer))
        if ppCounter == numPprices:
            pAndLsummaryFile.write( "\nIndex:" + '{:10.2f}'.format(indexValue) + " TotalValue:" + '{:10.2f}'.format(thisAssetValue)  + " ProductValue:" + '{:10.2f}'.format(cumulativeProductValue) + " Cash:" + '{:10.2f}'.format(productUnits[cashPid]*cashMid) + " AllPandL (unrealised+realised+coupons:" + '{:10.2f}'.format(valueChangeShouldBe) + " UnrealisedPandL:" + '{:10.2f}'.format(totalUnrealisedPandl) + " RealisedPandL:" + '{:10.2f}'.format(totalPandL)  + " Coupons:" + '{:10.2f}'.format(totalCouponCashflow)+ "       MEMO:BidOffer (positive=cost):" + '{:10.2f}'.format(totalBidOffer))







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
                productCosts[tradePid] = 0.0
            if tradePid == cashPid:
                # cash creates/redeems fund units at oldNav
                pAndLsummaryFile.write( "\nCASH TRANSACTION:" + tradeDate.strftime('%Y-%m-%d') + '{:10.2f}'.format(tradeMoney))
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
                # update cash
                productUnits[cashPid]  -= tradeMoney / productBids[cashPid]

                # realised pandl
                if tradeUnits > 0:
                    productCosts[tradePid] += tradeMoney
                else:
                    if tradePid not in productUnits or productUnits[tradePid] == 0.0:
                        print(previousDate,tradePid," cannot zero #units")
                        exit(113)
                    avgCost = productCosts[tradePid] / productUnits[tradePid]
                    baseCost = tradeUnits * avgCost
                    pAndL   = (-tradeMoney) + baseCost
                    totalPandL += pAndL
                    productCosts[tradePid]  += baseCost
                    pAndLFile.write( "\nOn:" + previousDate.strftime('%Y-%m-%d') + " sold" + '{:10.2f}'.format(tradeUnits) + " units of ProductId " + str(pid) + " at:" + '{:10.2f}'.format(tradePrice) + " Cashflow:" + '{:10.2f}'.format(tradeMoney) + " TradePrice:" + '{:10.2f}'.format(tradePrice) + " AverageCost:" + '{:10.2f}'.format(avgCost)  + " PandL:" + '{:10.2f}'.format(pAndL)  + " CumulativePandL:" + '{:10.2f}'.format(totalPandL))
                    pAndLsummaryFile.write( "\nOn:" + previousDate.strftime('%Y-%m-%d') + " sold" + '{:10.2f}'.format(tradeUnits) + " units of ProductId " + str(pid) + " at:" + '{:10.2f}'.format(tradePrice) + " Cashflow:" + '{:10.2f}'.format(tradeMoney) + " TradePrice:" + '{:10.2f}'.format(tradePrice) + " AverageCost:" + '{:10.2f}'.format(avgCost)  + " PandL:" + '{:10.2f}'.format(pAndL)  + " CumulativePandL:" + '{:10.2f}'.format(totalPandL))




            productUnits[tradePid] += tradeUnits

            # maybe check on assetValue
            if True:
                someAssetValue  = 0.0
                for pid,pos in productUnits.items():
                    productMid = (productBids[pid] + productAsks[pid])/2.0
                    if pid in productCrossRate:
                        if previousDate not in crossRates[productCrossRate[pid]]:
                            lessThanDates = {x for x in crossRates[productCrossRate[pid]] if x< previousDate}
                            useThisDate   = max(lessThanDates)
                            print(previousDate,"not in crossRates for",pid,"using rate for ",useThisDate)
                            # exit(111)
                        else:
                            useThisDate = previousDate
                        productMid *= crossRates[productCrossRate[pid]][useThisDate]
                    someAssetValue  += productUnits[pid] * productMid
                thisMid        = (productBids[tradePid] + productAsks[tradePid])/2.0
                cashMid        = (productBids[cashPid] + productAsks[cashPid])/2.0
                thisBidOffer   = tradeUnits*(tradePrice-thisMid)
                assetValueDiff = someAssetValue - traceAssetValue
                totalBidOffer += thisBidOffer

                thisString = "TRANSACTION:" + tradeDate.strftime('%Y-%m-%d') + " NAV:" + '{:10.2f}'.format(someAssetValue) + " diff:" + '{:10.2f}'.format(assetValueDiff) + " BO:" + '{:10.2f}'.format(thisBidOffer) + " units:" + '{:10.2f}'.format(tradeUnits) + " of product#" + '{:4d}'.format(tradePid) + " at" + '{:10.2f}'.format(tradePrice) + " MID:" + '{:10.2f}'.format(thisMid) + " value:" + '{:10.2f}'.format(tradeMoney) + " cash:" + '{:10.2f}'.format(productUnits[cashPid]*cashMid)
                print( thisString )
                pAndLFile.write( "\n\t" + thisString )

                traceAssetValue = someAssetValue
            tradeIndex              = tradeIndex + 1

        # init for next time
        # ... zero coupons
        for pid,y in sumCoupons.items():
            sumCoupons[pid] = 0.0
        previousDate      = thisDate


    # not a recalc date - just record bid/ask for this productId
    thisPid  = productPrice.ProductId
    if (thisPid not in productMaturityDate or thisDate <= productMaturityDate[thisPid]):
        if productPrice.Bid > 0.0:
            if thisPid in productBids and abs((productBids[thisPid] - productPrice.Bid)/productBids[thisPid]) > 0.3 :
                print("IGNORING Strange price on Date:",previousDate,"productId:",thisPid,"thisBid:",productPrice.Bid,"productBid:",productBids[thisPid])
            else:
                productBids[thisPid] = productPrice.Bid

        if productPrice.Ask > 0.0:
            if thisPid in productAsks and abs((productAsks[thisPid] - productPrice.Ask)/productAsks[thisPid]) > 0.3 :
                print("IGNORING Strange price on Date:",previousDate,"productId:",thisPid,"thisAsk:",productPrice.Ask,"productAsk:",productAsks[thisPid])
            else:
                productAsks[thisPid] = productPrice.Ask





# finally calc and save moneyWeights using MIDs
q = "update trade set Position=0 where InvestorId = "+format(userId)+" "
cursor.execute(q)
for pid,pos in productUnits.items():
    productMid      = (productBids[pid] + productAsks[pid])/2.0
    if pid in productCrossRate:
        productMid *= crossRates[productCrossRate[pid]][previousDate]
    thisWeight  = (productUnits[pid] * productMid) / thisAssetValue
    # ignore small weights as there are sometimes small remainders when a position is unwound - clerical error etc
    if thisWeight > 0.005:
        q = "update trade set Position=" + format(thisWeight) + " where ProductId = "+format(pid)+" and InvestorId="+format(userId)+" limit 1"
        cursor.execute(q)


# tidy up
cnxn.commit()
cursor.close()
cnxn.close()
couponFile.close()
pAndLFile.close()