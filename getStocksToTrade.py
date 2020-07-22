#!/usr/local/bin/python3


import pandas as pd
import yfinance as yf
import multiprocessing

def runThroughFilter(data, oneMonth, twoMonth, fiftyTwoWeekLow, fiftyTwoWeekHigh):
    
    result = 'FAIL'
    
    close = float(data['Close'][0])
    closeFiftyPercent = (close* 50)/100
    
    fiftyTwoWeekHigh25Percent = (fiftyTwoWeekHigh*25)/100
    fiftyTwoWeekHighAfter = fiftyTwoWeekHigh - fiftyTwoWeekHigh25Percent
    
    if data['Close'] > data['150DayMovingAvg'] and data['Close'] > data['200DayMovingAvg'] :
        #print ('Pass Filter 1 - Stock price is above both the 150-day and the 200-day moving average ')
        
        if data['150DayMovingAvg'] > data['200DayMovingAvg']:
            #print ('Pass Filter 2 - 150 day Moving Avg is greater than 200 day Moving Average ')
            
            if data['200DayMovingAvg'] > oneMonth['200DayMovingAvg'] and data['200DayMovingAvg'] > twoMonth['200DayMovingAvg']:
                #print ('Pass Filter 3 - 200 day Moving Avg is trending up')
            
                if data['50DayMovingAvg'] > data['150DayMovingAvg']:
                    #print ('Pass Filter 4 - 50 dat Moving Avg is above 150DayMovingAvg')
                    
                    if closeFiftyPercent >= fiftyTwoWeekLow:
                        #print ('Pass Filter 5 - current stock price is at least 50% greater than Fifty week low price')
                    
                        if close > fiftyTwoWeekHighAfter and close < fiftyTwoWeekHigh:
                            #print ('Pass Filter 6 - current stock price is greater than 25% of Fifty two week high price')
                            
                            if data['Close'] > data['50DayMovingAvg']:
                                #print ('Pass Filter 7 - current stock price is coming out of the consolidation base')

                                result = 'PASS'
                    
    return (result)


def processThisStock(stock):
    try:    
        ticker = yf.Ticker(stock)

        hist = ticker.history(period="365d")

        hist['TradeDate'] = hist.index
        hist = hist.sort_values('TradeDate', ascending=True)



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
        print ('Processed Stock: ' + str(stock) + '     Filter Result: ', filterResult)
        
        if filterResult == 'PASS':
            return(stock)

    except:
        print ("Error in Retreiving stock data - Not processing, no data for - " + str(stock))
        

        
if __name__ == '__main__':
    symbolsDF = pd.read_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/stocks.csv')
    symbols = list(symbolsDF['Symbol'])

    p = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    async_result = p.map_async(processThisStock, symbols)
    p.close()
    p.join() 

    resultFyle = open("/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/FILTERED.csv",'w')
    resultFyle.write("Symbol" + "\n")

    res = [] 
    for r in async_result.get(): 
        if r != None : 
            resultFyle.write(r + "\n")

    resultFyle.close()
    
