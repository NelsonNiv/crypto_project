import fetch_cmc_usd_history as cmc
import pandas as pd
import requests
import time
import os

#check if coin is whitin directory & provide the patch
def check_cache(symbol, name, until):
    #full store path
    full_path = 'data/{}/{}_{}'.format(until, symbol, name) 
    if not os.path.isfile(full_path):
        return ['File not found', full_path]
    else:
        return ['File found', full_path]

#download ohlcv prices    
def store_cmc(name, symbol, since, until):
    check = check_cache(symbol, name, until)
    if check[0] == 'File not found':
        print('Download Status: {}_{} downloading'.format(symbol, name))
        #request data
        df = cmc.main([name, since, until,'--dataframe'])
        df.to_pickle(check[1])
        print('Download Status: {}_{} downloaded'.format(symbol, name))
    else:
        print('Download Status: {}_{} downloaded'.format(symbol, name))
        return 'in cache'
    
#download ohlcv prices according to list   
def store_cmc_all(since, until):
    #Get the markets
    r = requests.get('https://api.coinmarketcap.com/v2/listings/')
    data = r.json()['data']
    print('Files to download:{}'.format(len(data)))
    #Loop for each market
    for element in data:
        #define market
        name = str(element['website_slug'])
        symbol = str(element['symbol'])
        #loop management
        x = 0 # -->x=1 loop stop
        z = 0 # -->requests with error
        while x != 1:
            try:
                doit = store_cmc(name, symbol, since, until)
                if doit == 'in cache':
                    x = 1
                else:
                    time.sleep(2)
                    x = 1
            except Exception as e:
                if z > 4:
                    time.sleep(28)
                    print('sleeping before ask again for: {}_{}, try: {}'.format(symbol, name, z))
                else:
                    print('something wrong with: {}_{}, error: {}'.format(symbol, name, e))
                    fileSave('log/{}_fetch_cmc.log'.format(until), '{} \n'.format(symbol))
                    x = 1
                    
def fileSave(filename, content):
    with open(filename, "a") as myfile:
        myfile.write(content)
