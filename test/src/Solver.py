import sys
import math
import pyodbc
import mysql.connector
import datetime
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
    def connect(self):
        ## return pyodbc.connect('DSN=newSp;PWD=ragtinmor')
        #return pyodbc.connect('DRIVER={MySQL ODBC 3.51 Driver};Login Prompt=False;User ID=root;Password=ragtinmor;Data Source=localhost;Database=sp')
        return mysql.connector.connect(user='anoble', password='Ragtin_Mor14_Lucian',
                              host='166.63.0.149',
                              database='sp')
#####
# DERP index
#
#  Clock is driven by arrival (not necessarily daily) of a date-ordered price for 1 product
# ... if a new day arrives "close off the books" for precedingDate
#    ... indexReturn is mean(positionReturns) which implies daily rebalancing to equal weights
#        ... unrealistic, but close to the complicated alternative of BUYS/SELLS on trade dates
#            ... minimisation problem:  min(bidAskCosts = deltaTrades*bidAsks) subject to
#                ... (oldPositions + deltaTrades)*ASKS equally weighted = 1/N * sum((oldPositions + deltaTrades)*ASKS)
#                ... (oldPositions + deltaTrades)> 0
#
#    ... accumulate any coupons ON_OR_BEFORE precedingDate for previousPositions
#    ... do any trades BEFORE precedingDate (trade.Date could fall between productPrices.Dates)
#        ... trades assumed executed on precedingDate
#        ... raise ERROR if no pricing
#        ... assert positions either ZERO or ONE
#        ... productReturn = returnSinceLastUsingMIDS - midToBidIfSold - midToAskIfBought + coupons
#    ... compute portValue as at precedingDate and update the index
#
# ... otherwise just record bid/ask for this productPrice
#
#####

#
# functions
#

#
#init
#
cnxn   = DBconn().connect()
cursor = cnxn.cursor(named_tuple=True)  # or user dictionary=True
for a in sys.argv:
    print("Argv:",a)
if len(sys.argv)<2:
    print("Usage: loginEmail")
    exit()
q = "select UserId  from user where email = '"+sys.argv[1]+"'"
cursor.execute(q)
userId = cursor.fetchone()
if not userId:
    print("NO user with that email")
    exit()
userId = userId.UserId


# userId = 137  is DERP@cubeinvesting.com


#
# get trades in date,Position order so that SELLS come before BUYS
#
tradeIndex = 0
q = "select * from trade where investorid=" + format(userId) + " order by Date,Position,ProductId"
cursor.execute(q)
trades = cursor.fetchall()
if not trades:
    print("NO trade DATA")
    exit()

#for (trade_date,pos) in cursor:
#  print("{}, was hired on {:%d %b %Y}".format(
#    pos, trade_date))

firstDate = trades[0].Date
numTrades = len(trades)
#
# get productPrices
#
productPriceIndex = 0
q = "select pp.* from productprices pp join trade using (productid) where InvestorId = "+format(userId)+" and pp.Date >= '"+format(firstDate)+"' order by Date,ProductId"
cursor.execute(q)
productPrices = cursor.fetchall()
if not productPrices:
    print("NO price DATA")
    exit()

#
# get coupons
#
couponIndex = 0
q = "select pc.* from productcoupons pc join trade using (productid) where InvestorId = "+format(userId)+" and pc.Date >= '"+format(firstDate)+"' order by Date,ProductId"
cursor.execute(q)
coupons = cursor.fetchall()
if not coupons:
    print("NO coupon DATA")
numCoupons = len(coupons)




#
# compute index
#
currentBids          = {}
currentAsks          = {}
previousBids         = {}
previousAsks         = {}
currentPositions     = {}
previousPositions    = {}
sumCoupons           = {}
indexValue           = 1.0
previousDate         = firstDate
isFirstDate          = True
#
#  Clock is driven by arrival (not necessarily daily) of a date-ordered price for 1 product
#
for productPrice in productPrices:
    thisDate = productPrice.Date
    #
    # ... if a new day arrives, compute portValue as at previousDate and update the index
    #    ... indexReturn is mean(positionReturns) which implies daily rebalancing to equal weights
    #
    if thisDate != previousDate:
        # ... accumulate any coupons ON_OR_BEFORE previousDate for previousPositions
        while couponIndex < numCoupons and [couponIndex].Date <= previousDate:
            thisPid     = coupons[couponIndex].ProductId
            if thisPid in previousPositions:
                thisCoupon  = previousPositions[thisPid] * coupons[couponIndex].Amount
                if thisPid not in sumCoupons:
                    sumCoupons[thisPid] = thisCoupon
                else:
                    sumCoupons[thisPid] += thisCoupon
            couponIndex = couponIndex + 1

        #    ... reflect in currentPositions any trades BEFORE precedingDate (trade.Date could fall between productPrices.Dates)
        while tradeIndex < numTrades and (trades[tradeIndex].Date < previousDate or (trades[tradeIndex].Date <= previousDate and trades[tradeIndex].Position<0.0)):
            tradeDate     = trades[tradeIndex].Date
            tradePid      = trades[tradeIndex].ProductId
            tradePosition = trades[tradeIndex].Position
            #        ... trades assumed executed on precedingDate
            #        ... raise ERROR if no pricing
            if tradePid not in previousBids or  tradePid not in previousAsks:
                print("No prices for",tradePid,"on",tradeDate)
                exit()
            #        ... assert positions either ZERO or ONE
            if tradePid in previousPositions:
                currentPosition = previousPositions[tradePid]
            else:
                currentPosition = 0.0
            newPosition = currentPosition + tradePosition
            if newPosition>1 or newPosition<0:
                print("Position too large",tradePid,"on",tradeDate)
                exit()
            # ... update positions
            currentPositions[tradePid]   = newPosition
            tradeIndex = tradeIndex + 1


        # ... compute portReturn (equal weight, continually rebalanced) as at precedingDate and update the index
        portReturn   = 0.0
        numPositions = 0
        for pid,pos in currentPositions.items():
            if pid not in previousPositions:
                previousPosition = 0.0
            else:
                previousPosition = previousPositions[pid]
            positionChange = pos - previousPosition
            previousMid    = (previousBids[pid] + previousAsks[pid])/2.0
            currentMid     = (currentBids [pid] + currentAsks [pid])/2.0
            if pid not in sumCoupons:
                thisCoupon = 0.0
            else:
                thisCoupon = sumCoupons[pid]
            # ... productReturn = returnSinceLastUsingMIDS - midToBidIfSold - midToAskIfBought + coupons
            if positionChange>0:
                # crap
                # thisReturn  = (currentMid       + thisCoupon)/previousAsks[pid]
                thisReturn  = (currentBids[pid]       + thisCoupon)/previousBids[pid]
                numPositions += 1
            elif positionChange<0:
                # crap
                # thisReturn  = (currentBids[pid] + thisCoupon)/previousMid
                thisReturn  = (currentBids[pid]       + thisCoupon)/previousBids[pid]
                numPositions += 1
            elif pos>0:
                # crap
                # thisReturn  = (currentMid       + thisCoupon)/previousMid
                thisReturn  = (currentBids[pid]       + thisCoupon)/previousBids[pid]
                numPositions += 1
            else:
                thisReturn  = 0.0


            portReturn  += thisReturn                #  previousDate >= datetime.date(2016,1,8)
        # calc index
        if isFirstDate:
            isFirstDate       = False
        else:
            portReturn  /= numPositions
            indexValue  *= portReturn

        #
        # save new index value
        #
        print("Index:",previousDate,indexValue)
        # cursor.execute("insert into products(id, name) values (?, ?)", 'pyodbc', 'awesome library')
        # cnxn.commit()
        #

        # init for next time
        # ... track positions
        for pid,pos in currentPositions.items():
            previousPositions[pid] = currentPositions[pid]
        # ... zero coupons
        for x in sumCoupons:
            sumCoupons[x] = 0.0
        # ... track bids, asks
        for pid,x in currentBids.items():
            previousBids[pid] = x
        for pid,x in currentAsks.items():
            previousAsks[pid] = x
        # ... track date
        previousDate       = thisDate


    # ... otherwise just record bid/ask for this productId
    thisPid              = productPrice.ProductId
    if productPrice.Bid > 0.0:
        currentBids[thisPid] = productPrice.Bid
        if thisPid not in previousBids:
            previousBids[thisPid] = productPrice.Bid
    if productPrice.Ask > 0.0:
        currentAsks[thisPid] = productPrice.Ask
        if thisPid not in previousAsks:
            previousAsks[thisPid] = productPrice.Ask


    #
    # for matured products, get maturityPayoff and DateMatured
    #

