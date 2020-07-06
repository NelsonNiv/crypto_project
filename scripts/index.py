import os
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
pd.options.display.float_format = '{:,.4f}'.format


#read pickle files
def read_pickle(full_path, filename):
    path = '{}/{}'.format(full_path,filename)
    df = pd.read_pickle(path)
    return df

#normalize dataframe
def only_float_values(dataframe):
    df = dataframe
    #1. Replace "-" values by NAN
    df = df.replace('-', np.nan)
    #2. Set data as an index
    df = df.set_index('Date')
    #3. Save df as float value
    df = df.astype(float)  
    return df
   
#last day
def last_day_in_df(dataframe, date):
    df = dataframe
    df = df[df.index <= date]            
    return df

#filter: set df only since market_cap info is available
def market_cap_available(dataframe):
    df = dataframe
    date_filter = df['Market Cap'].first_valid_index()
    df = df[df.index >= date_filter]
    return df
   
#provide the date 
def date_days_ago(date, days):
    date = datetime.strptime(date, '%Y-%m-%d')
    daysago = date - timedelta(days=days)
    return daysago    

#save list into text file
def listSave(filename, my_list):
    with open(filename, "w") as myfile:
        for item in my_list:
            myfile.write("%s\n" % item)

#all filters application
def filters(source, until, full_path, symbol, min_large, daily_volume, required_market_presence, required2):
    filter01, filter02, filter03, filter04, filter05, filter06, filter07, filter08, filter09 = [],[],[],[],[],[],[],[],[] 
    df = read_pickle(full_path, symbol)
    #update raw data as a float values dataset
    df = only_float_values(df)
    #do not include empty dataframes
    if not df.empty:
        filter01.append(symbol)
        sum_nan = float(sum(pd.isnull(df['Market Cap'])))
        len_df = float(len(df.index))     
        div = sum_nan / len_df
        #do not include dataframes with full of nan values of Market Cap
        if div != 1.0:
            filter02.append(symbol)
            #each dataframe starts from where Market Cap data begins
            df = market_cap_available(df)
            sum_nan = float(sum(pd.isnull(df['Market Cap'])))
            len_df = float(len(df.index))     
            div = sum_nan / len_df
            #only with Market Cap info available
            if div == 0:
                filter03.append(symbol)
                start = df.index[0]
                end = df.index[-1]
                days_start_end = int((end - start).days) + 1
                #only data without missing values        
                if days_start_end == len(df.index):
                    filter04.append(symbol) 
                    #each dataframe should have at least x registers
                    df = last_day_in_df(df, until)
                    if len(df.index) > min_large:
                        filter05.append(symbol)
                        #end date settled should be in dataframe
                        if until in df.index:
                            filter06.append(symbol)
                            days_ago = date_days_ago(until, min_large -1)
                            #date should match with date of dataframe
                            if days_ago == df.index[-min_large]:
                                filter07.append(symbol)
                                coin_presence = df[(df.index >= days_ago) & (df.Volume >= daily_volume)].count()['Volume'] /float(min_large) * 100.0
                                #required_market_presence (5% or 25%)
                                if coin_presence >= required_market_presence:
                                    filter08.append(symbol)    
                                    if coin_presence >= required2:
                                        filter09.append(symbol)
                                        
                                    
                               
    return filter01, filter02, filter03, filter04, filter05, filter06, filter07, filter08, filter09

def defineAssetClassCoins(source, until, min_large, daily_volume, required_market_presence, required2):
    #define base path
    base_path = os.getcwd()
    #variable path
    variable_path = '/{}'.format(source) #-->windows 
    #full store path
    full_path = base_path + variable_path
    #store the file names of data path in array
    array = os.listdir(full_path)
    #prepare lists to store symbols by filter
    filter01, filter02, filter03, filter04, filter05, filter06, filter07, filter08, filter09 = [],[],[],[],[],[],[],[],[]
    for symbol in array:
        consulta = filters(source, until, full_path, symbol, min_large, daily_volume, required_market_presence, required2)
        if consulta[0] != []:
            filter01.append(consulta[0][0])
        if consulta[1] != []:
            filter02.append(consulta[1][0])
        if consulta[2] != []:    
            filter03.append(consulta[2][0])
        if consulta[3] != []:
            filter04.append(consulta[3][0])
        if consulta[4] != []:
            filter05.append(consulta[4][0])
        if consulta[5] != []:    
            filter06.append(consulta[5][0])
        if consulta[6] != []:    
            filter07.append(consulta[6][0])
        if consulta[7] != []:    
            filter08.append(consulta[7][0])
        if consulta[8] != []:    
            filter09.append(consulta[8][0])
    print('########### RESUME ###########')
    print('raw dataset : {}'.format(len(array)))    
    print('filter01 : {}'.format(len(filter01)))
    print('filter02 : {}'.format(len(filter02)))
    print('filter03 : {}'.format(len(filter03)))
    print('filter04 : {}'.format(len(filter04)))
    print('filter05 : {}'.format(len(filter05)))
    print('filter06 : {}'.format(len(filter06)))
    print('filter07 : {}'.format(len(filter07)))
    print('filter08 : {}'.format(len(filter08)))
    print('filter09 : {}'.format(len(filter09)))
    listSave('Index/' + until + '_crypto_market_coins.txt', filter08)
    listSave('Index/' + until + '_crypto_market25_coins.txt', filter09)
    return filter01, filter02, filter03, filter04, filter05, filter06, filter07, filter08, filter09

def defineSubAssetClassCoins(source, until):
    #define base path
    base_path = os.getcwd()
    #variable path
    variable_path = '/{}'.format(source) #-->windows 
    #full store path
    full_path = base_path + variable_path
    #store the coins in array
    array = [line.rstrip() for line in open('indexs/{}_crypto_market25_coins.txt'.format(until))]
    #prepare lists to store symbols by filter
    symbols = []
    marketCap = [] 
    for symbol in array:
        df = read_pickle(full_path, symbol)
        df = only_float_values(df)
        mc = float(df['Market Cap'][df.index == until])
        symbols.append(symbol)
        marketCap.append(mc)
    mylist = {'symbol':symbols, 'marketCap':marketCap}
    df = pd.DataFrame(mylist)
    #sort values by market cap
    df = df.sort_values(by=['marketCap'], ascending=False)
    #accumulated sum
    df['cum_sum'] = df['marketCap'].cumsum()
    #value as percentage
    df['cum_perc'] = 100*df['cum_sum']/df['marketCap'].sum()
    #create the list of [large, mid & small cap coins]
    large = []
    mid = []
    small = []
    for x in range(0, len(df.index)):
        symbol = df['symbol'][x]
        cum_perc = df['cum_perc'][x]   
        if cum_perc <= 80:
            large.append(symbol)
        elif cum_perc <= 95:
            mid.append(symbol)
        else:
            small.append(symbol)

    print('########### RESUME ###########')
    print('raw dataset : {}'.format(len(array)))    
    print('large : {}'.format(len(large)))
    print('mid : {}'.format(len(mid)))
    print('small : {}'.format(len(small)))        
    listSave('indexs/{}_large_cap_coins.txt'.format(until), large)
    listSave('indexs/{}_mid_cap_coins.txt'.format(until), mid)
    listSave('indexs/{}_small_cap_coins.txt'.format(until), small)    
    return large, mid, small

def create_index(source, until, index_name):
    import sys
    #define base path
    base_path = os.getcwd()
    #variable path
    variable_path = '/data/{}'.format(source) #-->windows 
    #full store path
    full_path = base_path + variable_path
    #date base
    df = read_pickle(full_path, 'BTC')
    df = only_float_values(df)
    df = df[['Market Cap']]
    df.rename(columns={'Market Cap':'BTC'}, inplace=True)
    #store the coins in array    
    array = [line.rstrip() for line in open('indexs/{}_{}_coins.txt'.format(until, index_name))]
    for symbol in array:
        if symbol != 'BTC':
            df_symbol = read_pickle(full_path, symbol)
            df_symbol = only_float_values(df_symbol)
            df_symbol = df_symbol[['Market Cap']]
            df_symbol.rename(columns={'Market Cap':symbol}, inplace=True)
            df = df.join(df_symbol)
    #delete BTC values in case it's inside the array
    if 'BTC' not in array:
        df.drop('BTC', axis=1, inplace=True)        
    #sum of daily market cap
    df['global_mc'] = df.sum(axis=1)
    #fillup nan values
    df = df.fillna(method='pad')
    df = df.fillna(0)
    #fist day setting
    date_filter = df[df['global_mc'].gt(0)].index[0]
    df = df[df.index >= date_filter]
    #last day setting (until)
    df = last_day_in_df(df, until)
    #preparing array for index
    n_rows = len(df.index)
    n_columns = len(df.columns)
    results = np.zeros((4, n_rows))
    arraycolumns = ['deposits', 'global_mc', 'index_value', 'divisor']
    #preparing array for index values
    for i in range(0, n_rows):
        global_mc = df['global_mc'].iloc[i]
        results[1,i] = global_mc
        #first row setting
        if i == 0:
            results[0,i] = 0                             #--> deposit_withdraw
            results[2,i] = 100                           #--> index_value
            results[3,i] = global_mc / results[2,i]      #--> divisor
        else:
            sum_temp = 0
            for x in range(0,  n_columns):
                column = df.columns[x]
                if column != 'global_mc':
                    anterior = df[column].iloc[i-1]             
                    presente = df[column].iloc[i]
                    if anterior == 0 and presente > 0:
                        sum_temp = sum_temp + presente 
            results[0,i] = sum_temp                                        #--> deposit_withdraw
            results[3,i] = results[3,i -1] + (sum_temp / results[2,i -1])  #--> divisor
            results[2,i] = global_mc / results[3,i]                        #--> index_value

    #create index dataframe       
    index = pd.DataFrame(results.T,columns=arraycolumns,index=df.index)
    index.to_csv('indexs/{}_{}.csv'.format(until, index_name))
    print('The index:{} has been created successfully. Please check inside the folder <<indexs>>'.format(index_name))
    return index

def benchmark_same_axe(my_list, date):
    import plotly as py
    import plotly.graph_objs as go
    from plotly import tools
    import ipywidgets as widgets
    py.offline.init_notebook_mode(connected=True)
    large = len(my_list)
    if large == 1:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]), index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        data = [trace1]
    elif large == 2:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]),index_col='Date')
        df2 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[1]),index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        trace2 = go.Scatter(x=df2.index, y=df2.index_value, name=my_list[1])
        data = [trace1, trace2]      
    elif large == 3:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]),index_col='Date')
        df2 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[1]),index_col='Date')        
        df3 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[2]),index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        trace2 = go.Scatter(x=df2.index, y=df2.index_value, name=my_list[1])
        trace3 = go.Scatter(x=df3.index, y=df3.index_value, name= my_list[2])
        data = [trace1, trace2, trace3]     
    elif large == 4:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]),index_col='Date')
        df2 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[1]),index_col='Date')        
        df3 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[2]),index_col='Date')
        df4 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[3]),index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        trace2 = go.Scatter(x=df2.index, y=df2.index_value, name=my_list[1])
        trace3 = go.Scatter(x=df3.index, y=df3.index_value, name= my_list[2])
        trace4 = go.Scatter(x=df4.index, y=df4.index_value, name=my_list[3])
        data = [trace1, trace2, trace3, trace4]
    elif large == 5:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]),index_col='Date')
        df2 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[1]),index_col='Date')        
        df3 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[2]),index_col='Date')
        df4 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[3]),index_col='Date')
        df5 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[4]),index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        trace2 = go.Scatter(x=df2.index, y=df2.index_value, name=my_list[1])
        trace3 = go.Scatter(x=df3.index, y=df3.index_value, name= my_list[2])
        trace4 = go.Scatter(x=df4.index, y=df4.index_value, name=my_list[3])
        trace5 = go.Scatter(x=df5.index, y=df5.index_value, name=my_list[4])
        data = [trace1, trace2, trace3, trace4, trace5]
    elif large == 6:
        df1 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[0]),index_col='Date')
        df2 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[1]),index_col='Date')        
        df3 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[2]),index_col='Date')
        df4 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[3]),index_col='Date')
        df5 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[4]),index_col='Date')
        df6 = pd.read_csv('indexs/{}_{}.csv'.format(date, my_list[5]),index_col='Date')
        trace1 = go.Scatter(x=df1.index, y=df1.index_value, name=my_list[0])
        trace2 = go.Scatter(x=df2.index, y=df2.index_value, name=my_list[1])
        trace3 = go.Scatter(x=df3.index, y=df3.index_value, name= my_list[2])
        trace4 = go.Scatter(x=df4.index, y=df4.index_value, name=my_list[3])
        trace5 = go.Scatter(x=df5.index, y=df5.index_value, name=my_list[4])
        trace6 = go.Scatter(x=df6.index, y=df6.index_value, name=my_list[5])
        data = [trace1, trace2, trace3, trace4, trace5, trace6]
    else:
        print('Not allow to plot more than 6 datasets')

    layout = go.Layout(title=date, yaxis=dict(title='BASE POINTS'), width=980, height=450)
    fig = go.Figure(data=data, layout=layout)
    py.offline.iplot(fig)