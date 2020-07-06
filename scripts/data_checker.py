#libreria para el manejo de dataframes
import pandas as pd
#libreria de funciones matematicas para vectores y matrices 
import numpy as np
#Libreria de de sistemas operativos
import os
#Libreria de manejo de datos de tiempo
from datetime import datetime
from datetime import timedelta

################################################################################ 
# Funciones de lectura, normalizacion y referencias en DataFrames              #
################################################################################

#read pickle files
def read_pickle(full_path, filename):
    path = 'data/{}{}'.format(full_path,filename)
    df = pd.read_pickle(path)
    return df

#normalize dataframe
def normalize_df(dataframe, last_date):
    df = dataframe
    #1. Replace "-" values by NAN
    df = df.replace('-', np.nan)
    #2. Select variables to work with
    df = df[['Date','Close','Volume','Market Cap']]
    #3. Set Date as an index
    df = df.set_index('Date')
    #4. Set last date of Dataframe
    df = df[df.index <= last_date]
    #5. Save Dataframe as float value
    #df = df.astype(float)  
    return df

#reference function
def date_days_ago(date, days):
    date = datetime.strptime(date, '%Y-%m-%d')
    daysago = date - timedelta(days=days)
    return daysago    

################################################################################ 
# Funciones de chequeo del estado de DataFrames en general                     #
################################################################################

# DF empty
def filter01(dataframe):
    #df empty
    df = dataframe
    if not df.empty:
        value = 0
    else:
        value = 1
    return value

# DF with less than 90 days of history
def filter02(dataframe):
    #large of df should have a large enough
    df = dataframe
    if len(df.index) >= 90:
        value = 0
    else:
        value = 1
    return value

# DF large != Days between df.end - df.start 
def filter03(dataframe, days_ago):
    df = dataframe
    df = df[(df.index >= days_ago)]
    start = df.index[0]
    end = df.index[-1]
    days_start_end = int((end - start).days) + 1
    if days_start_end == len(df.index):
        value = 0
    else:
        value = 1
    return value

# Date not inside DF
def filter04(dataframe, until):
    #until date should be inside the df
    df = dataframe
    if until in df.index:
        value = 0
    else:
        value = 1
    return value

################################################################################ 
# Funciones de chequeo del estado de columnas en cada DataFrame                #
################################################################################

# missing values in column
def filter05(dataframe, column, days_ago):
    #dataframe without 
    df = dataframe
    df = df[(df.index >= days_ago)]
    sum_nan = float(sum(pd.isnull(df[column])))
    len_df = float(len(df.index))
    if len_df != 0:   #empty df
        div = sum_nan / len_df
        if div == 1 or div == 0:  #empty dataframe or cero nan value
            value = 0
        else:        
            value = 1 #sum_nan < len_df
    else:
        value = 0
    return value


################################################################################ 
# Funcion de cumplimiento de restriccion de liquidez para indices              #
################################################################################

# 25%
def filter06(dataframe, min_large, days_ago, daily_volume, required_market_presence):
    #coin_presence (liquidity)
    df = dataframe
    coin_presence = df[(df.index >= days_ago) & (df.Volume >= daily_volume)].count()['Volume'] /float(min_large) * 100.0
    if coin_presence >= required_market_presence:
        value = 0
    else:
        value = 1
    return value


################################################################################ 
# Funciones de aplicacion de filtros y chequeos                                #
################################################################################

# apply_filters: Aplicacion de filtros 1 a 6
# Los valores para filtros 2 a 6 no son excluyentes pues son chequeos para todos 
# aquellos dataframes que contienen informacion --> filter01 = 0.

def apply_filters(full_path, last_date, min_large, days_ago, daily_volume, coin_presence, *array):
    #Set empty values, list & variables to work with     
    numbers = [0, 0, 0, 0, 0, 0, 0, 0]
    symbols = {'f01':[], 'f02':[], 'f03':[], 'f04':[], 'f05a':[], 'f05b':[], 'f05c':[], 'f06':[]}        
    #create array list with all files in directory
    if array is None:
        array = os.listdir('data/' + full_path)  
    #create loop
    for symbol in array[0]:
        df = read_pickle(full_path, symbol)
        df = normalize_df(df, last_date)
        filter1 = filter01(df)
        if filter1 == 0: #df with data
            filter2 = filter02(df)
            filter3 = filter03(df, days_ago)
            filter4 = filter04(df, last_date)
            filter5a = filter05(df, 'Close', days_ago)
            filter5b = filter05(df, 'Volume', days_ago)
            filter5c = filter05(df, 'Market Cap', days_ago)     
            filter6 = filter06(df, min_large, days_ago, daily_volume, coin_presence)
            if filter2 == 1:
                # less than 90 days of history
                numbers[1] += filter2
                symbols['f02'].append(symbol)
            if filter3 == 1:
                # DF large =! Days between df.end - df.start
                numbers[2] += filter3
                symbols['f03'].append(symbol)
            if filter4 == 1:
                #Date not within DF
                numbers[3] += filter4
                symbols['f04'].append(symbol) 
            if filter5a == 1:
                numbers[4] += filter5a
                symbols['f05a'].append(symbol)
            if filter5b == 1:
                numbers[5] += filter5b
                symbols['f05b'].append(symbol)      
            if filter5c == 1:
                numbers[6] += filter5c
                symbols['f05c'].append(symbol)
            if filter6 == 1:
                #Date not within DF
                numbers[7] += filter6
                symbols['f06'].append(symbol)              
        else:
            # empty
            numbers[0] += filter1
            symbols['f01'].append(symbol)

    
    
    
    #print details  
    print('#### Total of files stored in path:{} ####'.format(len(array[0])))
    print('#### Last date of Data Frames:{}'.format(last_date))
    print('---> DATAFRAMES(DF) DETAIL <--')    
    print('F1:Empty DF:{}'.format(numbers[0]))
    print('F2:DF w/less than 90 days of history:{}'.format(numbers[1]))    
    print('F3:DF w/difference between len(DF) and days between (df.end - df.start):{}'.format(numbers[2]))
    print('F4:Date of close is not within DF:{}'.format(numbers[3]))
    print('---> VARIABLES(Price, Volume, MarketCap) DETAIL <--')   
    print('F5a:Assets with NAN(0) values in Price colum (from 1 day):{}'.format(numbers[4]))
    print('F5b:Assets with NAN(0) values in Volume colum (from 1 day):{}'.format(numbers[5]))   
    print('F5c:Assets with NAN(0) values in MarketCap colum (from 1 day):{}'.format(numbers[6]))    
    print('---> LIQUIDITY(Volume) DETAIL <--')
    print('F6:Assets w/25% of days over a daily transaction amount of US$40.000):{}'.format(numbers[7]))
    print(' ')
    
    return numbers, symbols


# apply_filters_waterfall: Aplicacion de filtros 1 a 6
# Los valores para filtros 2 a 6 son excluyentes pues son chequeos en cascada. 
# Por ejemplo, el filtro2 solo analizara todos aquellos dataframes que contienen informacion,
# el filtro3 analizara todos aquellos dataframes que pasaron el filtro1 y filtro2.
# el filtro4 seguira realizando el mismo loop hasta que se llegue al filtro6.

def apply_filters_waterfall(full_path, last_date):
    #create array list with all files in directory
    array = os.listdir('data/' + full_path)
    large = len(array)
    #Set empty values, list & variables to work with     
    numbers = [0, 0, 0, 0, 0, 0, 0, 0]
    symbols = {'f01':[], 'f02':[], 'f03':[], 'f04':[], 'f05a':[], 'f05b':[], 'f05c':[], 'f06':[]}       
    #create loop 
    for symbol in array:
        df = read_pickle(full_path, symbol)
        df = normalize_df(df, last_date)
        filter1 = filter01(df)
        if filter1 == 1: #df with data
            numbers[0] += 1
            symbols['f01'].append(symbol)  
        else:
            filter2 = filter02(df)
            if filter2 == 1:
                numbers[1] += 1
                symbols['f02'].append(symbol)
            else:
                filter3 = filter03(df)
                if filter3 == 1:
                    numbers[2] += 1
                    symbols['f03'].append(symbol)
                else:
                    filter4 = filter04(df, last_date)
                    if filter4 == 1:
                        numbers[3] += 1
                        symbols['f04'].append(symbol)
                    else:
                        filter5a = filter05(df, 'Close')
                        if filter5a == 1:
                            numbers[4] += 1
                            symbols['f05a'].append(symbol)
                        else:
                            filter5b = filter05(df, 'Volume')
                            if filter5b == 1:
                                numbers[5] += 1
                                symbols['f05b'].append(symbol)    
                            else:
                                filter5c = filter05(df, 'Market Cap')
                                if filter5c == 1:
                                    numbers[6] += 1
                                    symbols['f05c'].append(symbol)
                                else:
                                    days_ago = date_days_ago(last_date, 89)
                                    filter6 = filter06(df, 90, days_ago, 40000, 25.0)
                                    if filter6  == 1:
                                        numbers[7] += 1
                                        symbols['f06'].append(symbol)             
    return numbers, symbols



# quantity_evolution: evolucion de la cantidad de activos criptograficos registrados
# por CoinMarketCap. Realiza comparacion a la cantidad de informacion en precios,
# volumen y capitalizacion de mercado

def quantity_evolution(full_path, last_date):
    #create array list with all files in directory
    array = os.listdir('data/' + full_path)
    large = len(array)
    #Set empty values, list & variables to work with     
    #save full dates
    if '_' in array[0]:
        df = read_pickle(full_path, 'BTC_bitcoin')
    else:
        df = read_pickle(full_path, 'BTC')
    df = normalize_df(df, last_date)
    date = df.index.tolist()
    vacio = np.zeros((3,len(date)))
    price_none = 0
    volume_none = 0
    marketcap_none = 0
    #create loop 
    for symbol in array:    
        df = read_pickle(full_path, symbol)
        df = normalize_df(df, last_date)
        price = df['Close'].first_valid_index()
        volume = df['Volume'].first_valid_index()
        marketcap = df['Market Cap'].first_valid_index()
        
        if price is not None:
            idx = date.index(price)
            vacio[0, idx] += 1    
        else:
            price_none += 1
            
        if volume is not None:
            idx = date.index(volume)
            vacio[1, idx] += 1    
        else:
            volume_none += 1
            
        if marketcap is not None:
            idx = date.index(marketcap)
            vacio[2, idx] += 1    
        else:
            marketcap_none += 1           
 
    #create dataframe with results
    df = pd.DataFrame({'Date': date, 'Price': vacio[0], 'Volume': vacio[1], 'MarketCap': vacio[2]})
    #create cumulative sums
    df['Price_cum_sum'] = df['Price'].cumsum()
    df['Volume_cum_sum'] = df['Volume'].cumsum()
    df['MarketCap_cum_sum'] = df['MarketCap'].cumsum()
    #set date as index
    df = df.set_index('Date')
    #save last date of cumulative sums
    price_cum_sum = int(df.Price_cum_sum[-1])
    volume_cum_sum = int(df.Volume_cum_sum[-1])
    marketcap_cum_sum = int(df.MarketCap_cum_sum[-1])
    #checking total of filtes
    price_total = price_cum_sum + price_none
    volume_total = volume_cum_sum + volume_none
    marketcap_total = marketcap_cum_sum + marketcap_none   
    
    #print details  
    print('#### Total of files stored in path:{} ####'.format(len(array)))
    print('#### Last date of Data Frames:{}'.format(last_date))
    print('---> PRICE DETAIL <--')
    print('Price_cum_sum:{}'.format(price_cum_sum))
    print('Price_none_value:{}'.format(price_none))
    print('Total_files:{}'.format(price_total))
    print('---> VOLUME DETAIL <--')    
    print('Volume_cum_sum:{}'.format(volume_cum_sum))
    print('Volume_none_value:{}'.format(volume_none))
    print('Total_files:{}'.format(volume_total))
    print('---> MARKET CAP DETAIL <--')        
    print('MarketCap_cum_sum:{}'.format(marketcap_cum_sum))
    print('MarketCap_none_value:{}'.format(marketcap_none))
    print('Total_files:{}'.format(marketcap_total))
    print(' ')
    
    return df