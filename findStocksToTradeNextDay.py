#!/Users/vk/miniconda3/bin/python

from datetime import datetime, timedelta
import time

import pandas as pd
import yfinance as yf
import multiprocessing


from pandasql import sqldf
from datetime import datetime, date


today = date.today()
formatTodayDate = today.strftime("%Y%m%d")

import warnings
warnings.filterwarnings('ignore')

import sys

# Global Variables

day = sys.argv[1]
day = int(day)


month = sys.argv[2]
month = int(month)

year = sys.argv[3]
year = int(year)

end   = datetime(2020, month, day)
endDate = end.replace(hour=17, minute=0)

tPlusOne = end + timedelta(days=1) # so that it considers the end date, otherwise it does only till T-1
previousYearSameDay = endDate - timedelta(days=500)
print ('Date range of data :: ' + str(previousYearSameDay) + ' - ' + str(endDate))


resultPredictedForDate = tPlusOne #basically, next trading day
formatPredictedDate = resultPredictedForDate.strftime("%Y%m%d")
print ('Trade Plan for these below stocks for trade date - ' + str (formatPredictedDate))


volThreshold = 25 
stockPriceThreshold = 20
volumeThreshold = 100000




def findConcentrations(data, stock):
    
    contractionNumber = 0
    priorContractionPercent = []
    priorContractionDays = []
    
    
    while len(data) > 2:

        # start the cluster from highest to lowest price, that is one contraction cluster
        
        data = data.sort_values('TradeDate', ascending=True)
        
        maxDate = data['TradeDate'][data.Close == data.Close.max()]
        maxDate = pd.to_datetime(maxDate)
        maxDate = maxDate.dt.to_pydatetime()
        startDate = maxDate[0]  
        
        query = """ SELECT * FROM data where TradeDate >= '{}'""".format(startDate)

        contraction_start = sqldf(query)   
        #print (contraction_start)


        minDate = contraction_start['TradeDate'][contraction_start.Close == contraction_start.Close.min()]
        minDate = pd.to_datetime(minDate)
        minDate = minDate.dt.to_pydatetime()
        endDate = minDate[0]

        endDate = endDate.replace(hour=23, minute=59)

        query2 = """ SELECT * FROM contraction_start where TradeDate <= '{}'""".format(endDate)

        contraction = sqldf(query2)

        contractionPercent = ((contraction.Close.max()-contraction.Close.min())*100)/contraction.Close.max()
        contractionDays = len(contraction)


        remainingDataQuery = """ SELECT * FROM contraction_start where TradeDate > '{}'""".format(endDate)
        remainingData = sqldf(remainingDataQuery)
        

        # check if the contraction in stock price is going down, this means the stock price is consolidating
        
        if contractionPercent > 0:
            contractionNumber = contractionNumber + 1
            contractionPercent = contractionPercent.round(2)
            
            priorContractionPercent.append(contractionPercent)
            #priorContractionDays.append(contractionDays)
            #print (stock, contractionNumber, contractionPercent, priorContractionPercent, contractionDays, priorContractionDays)

        
        data = remainingData

    return(contractionNumber, priorContractionPercent)



def checkVolumeTheory(lastThreeDays):
    result = 'FAIL'
    
    #print (lastThreeDays)
    
    today = lastThreeDays.tail(1)
    dayBeforeYesterday = lastThreeDays.head(1)

    lastTwoDays = lastThreeDays.tail(2)
    yesterday = lastTwoDays.head(1)

    yesterdayVolume = yesterday['Volume'].iloc[0]
    todayVolume = today['Volume'].iloc[0]
    dayBeforeYesterdayVolume = dayBeforeYesterday['Volume'].iloc[0]

    dayBeforeYesterdayVolumeThreshold = (volThreshold/100) * dayBeforeYesterdayVolume
    dayBeforeYesterdayVolumeAfter = dayBeforeYesterdayVolume - dayBeforeYesterdayVolumeThreshold

    #print (todayVolume, yesterdayVolume, dayBeforeYesterdayVolume, dayBeforeYesterdayVolumeAfter)

    if yesterdayVolume > todayVolume and todayVolume > volumeThreshold:
        if dayBeforeYesterdayVolumeAfter > yesterdayVolume:
            if today['Close'].iloc[0] > stockPriceThreshold:
                #print (todayVolume, yesterdayVolume, dayBeforeYesterdayVolume, dayBeforeYesterdayVolumeAfter)
                result = 'PASS'
    
    return (result, todayVolume, yesterdayVolume, dayBeforeYesterdayVolume, dayBeforeYesterdayVolumeAfter)

def runThroughFilter(data, oneMonth, twoMonth, fiftyTwoWeekLow, fiftyTwoWeekHigh):
    
    result = 'FAIL'
    
    close = float(data['Close'][0])
    closeFiftyPercent = (close* 50)/100
    
    #print (data)
    
    #print (oneMonth, twoMonth)
    
    #current stock price is at least 20-25% less than fifty two week high, close to high the better
    fiftyTwoWeekHigh25Percent = (fiftyTwoWeekHigh*20)/100
    fiftyTwoWeekHighAfter = fiftyTwoWeekHigh - fiftyTwoWeekHigh25Percent
    
    
    # current stock price is at least 150% greater than 52week low
    fiftyTwoWeekLow25Percent = (fiftyTwoWeekLow*150)/100
    fiftyTwoWeekLowAfter = fiftyTwoWeekLow + fiftyTwoWeekHigh25Percent
    
    
    if data['Close'] > data['150DayMovingAvg'] and data['Close'] > data['200DayMovingAvg'] :
        #print ('Pass Filter 1 - Stock price is above both the 150-day and the 200-day moving average ')
        
        if data['150DayMovingAvg'] > data['200DayMovingAvg']:
            #print ('Pass Filter 2 - 150 day Moving Avg is greater than 200 day Moving Average ')
            
            if data['200DayMovingAvg'] > oneMonth['200DayMovingAvg'] and data['200DayMovingAvg'] > twoMonth['200DayMovingAvg']:
                #print ('Pass Filter 3 - 200 day Moving Avg is trending up')
            
                if data['50DayMovingAvg'] > data['150DayMovingAvg']:
                    #print ('Pass Filter 4 - 50 dat Moving Avg is above 150DayMovingAvg')
                    
                    if close >= fiftyTwoWeekLowAfter:
                        #print (close, fiftyTwoWeekLow, fiftyTwoWeekLowAfter )
                        #print ('Pass Filter 5 - current stock price is at least 150% greater than Fifty week low price')
                    
                        if close > fiftyTwoWeekHighAfter and close < fiftyTwoWeekHigh:
                        #if close > fiftyTwoWeekHighAfter:
                            #print ('Pass Filter 6 - current stock price is greater than 25% of Fifty two week high price')
                            
                            if data['Close'] > data['50DayMovingAvg']:
                                #print ('Pass Filter 7 - current stock price is coming out of the consolidation base')

                                result = 'PASS'
                    
    return (result)


def findContractedStock(stock):
    
    try:
        
        ticker = yf.Ticker(stock)
        hist = ticker.history(start=previousYearSameDay, end=endDate, interval="1d")    
        hist['TradeDate'] = hist.index
        hist = hist.sort_values('TradeDate', ascending=True)
        
        lastThreeDays = hist.tail(3)
        
        # check for recent volume decline first
        
        volFilterResult, todayVolume, yesterdayVolume, dayBeforeYesterdayVolume, dayBeforeYesterdayVolumeAfter = checkVolumeTheory(lastThreeDays)

        if volFilterResult == 'PASS':
        
            fiftyTwoWeekLow = min(hist['Close'])
            fiftyTwoWeekHigh = max(hist['Close'])

            hist['3DayMovingAvg'] = hist['Close'].rolling(window=3).mean()
            hist['50DayMovingAvg'] = hist['Close'].rolling(window=50).mean()
            hist['150DayMovingAvg'] = hist['Close'].rolling(window=150).mean()
            hist['200DayMovingAvg'] = hist['Close'].rolling(window=200).mean()
            hist['Stock'] = stock

            hist = hist.filter(['Stock', 'TradeDate', 'Volume', 'Close', '50DayMovingAvg', '150DayMovingAvg', '200DayMovingAvg'])
            hist = hist.sort_values('TradeDate', ascending=False)

            oneMonthAgoData =  hist.head(30)
            oneMonthAgoData = oneMonthAgoData.tail(1)
            oneMonthAgo = oneMonthAgoData.to_dict('list')


            twoMonthAgoData =  hist.head(60)
            twoMonthAgoData = twoMonthAgoData.tail(1)
            twoMonthAgo = twoMonthAgoData.to_dict('list')

            dataDict = hist.head(1).to_dict('list')

            filterResult = runThroughFilter(dataDict, oneMonthAgo, twoMonthAgo, fiftyTwoWeekLow, fiftyTwoWeekHigh)

            #print (filterResult)


            if filterResult == 'PASS':
                numberOfContractions, contractionPercentDetails = findConcentrations(hist, stock)            
                #isContractionConsolidating = all(earlier >= later for earlier, later in zip(contractionPercentDetails, contractionPercentDetails[1:]))

                #if numberOfContractions >= 3 and numberOfContractions <=5 and isContractionConsolidating is True:
                if numberOfContractions >= 2 and numberOfContractions <=5:
                    print (stock + ' had ' + str(numberOfContractions) + ' contractions - Volume & Price consolidating')
                    returnString = str(stock) + ',' + str(todayVolume) + ',' + str(yesterdayVolume) + ',' + str(dayBeforeYesterdayVolume)
                    #print (returnString)
                    return (returnString)
                        
    except:
        print ("Error in Retreiving stock data - Not processing, no data for - " + str(stock))


        
if __name__ == '__main__':
    
    symbolsDF = pd.read_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/stocks.csv')
    #symbolsDF = pd.read_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/dupes.csv')
    symbols = list(symbolsDF['Symbol'])
    
    #symbols = ['ACIA', 'V']
    
    #print (symbols)
    
    
    p = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    async_result = p.map_async(findContractedStock, symbols)
    p.close()
    p.join() 
    
    writeToThisFile = '/Users/vk/Desktop/SchoolAndResearch/StockResearch/Trading/data/TRADE_THESE_STOCKS_ON.' + str(formatPredictedDate) + '.csv'

    resultFyle = open(writeToThisFile,'w')
    resultFyle.write("Symbol, TodayVolume, YesterdayVolume, DayBeforeYesterdayVolume" + "\n")

    res = [] 
    for r in async_result.get(): 
        if r != None : 
            resultFyle.write(r + "\n")

    resultFyle.close()