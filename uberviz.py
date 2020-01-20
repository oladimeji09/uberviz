import zipfile, pandas as pd, requests as r,datetime,time
from pandas.io.json import json_normalize

fp =''          ## TODO: CHANGE THIS LOCATION WHERE THE ZIP FILE FROM UBER IS STORED
tgt_dr = ''     ## TODO: CHANGE THIS TARGET LOCATION FOR EXTRACTION
apikey = ''     ## TODO: VISIT https://www.alphavantage.co FOR A FREE API KEY
gmapkey = ''    ## TODO: VISIT GOOGLE API CONSOLE FOR A FREE GOOGLE MAPS API KEY

with zipfile.ZipFile(fp,"r") as zip_ref: # EXTRACT FILES TO TARGET LOCATION
    zip_ref.extractall(tgt_dr)

user_df  = pd.read_csv(tgt_dr+'/Account and Profile/profile_data.csv')
fullname = str(user_df['First Name'].to_string(index=False).strip()+'_'+ user_df['Last Name'].to_string(index=False).strip()).lower()

trips_df = pd.read_csv(tgt_dr+'/Rider/trips_data.csv')
currency_codes = list(trips_df['Fare Currency'].unique())

cur_df = pd.DataFrame()
link = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={}&to_currency=USD&apikey={}'

def get_rates(code):
    print('getting rates for '+ code)
    posting = r.get(link.format(code,apikey))
    time.sleep(14) ### NOTE: DON'T WANT TO EXCEED LIMITS
    data=posting.json()
    df= json_normalize(data['Realtime Currency Exchange Rate'])
    df= df.loc[:,['1. From_Currency Code','5. Exchange Rate','6. Last Refreshed']]
    df.columns= df.columns.str[3:]
    global cur_df
    cur_df = cur_df.append(df,ignore_index=True)

## NOTE: ALPHAVANTAGE API CAN THROW RANDOM ERRORS SOMETIMES SO TRAVERSE THIS LOOP TWICE
for code in currency_codes:
    try:
        print('first try for ' + code)
        get_rates(code)
    except:
        try:
            print('second try for ' + code)
            get_rates(code)
        except:
            pass

df =  trips_df.merge(cur_df, left_on='Fare Currency', right_on = 'From_Currency Code' , how='inner' ,  left_index=False, right_index=False)
df = df.drop('From_Currency Code', axis=1)

columns =['Request Time','Begin Trip Time','Dropoff Time']
for cols in columns:
    if cols in df[columns]:
        df[cols] = df[cols].str[:19].astype('datetime64[ns]')

## NOTE: RETRIEVE ADDITIONAL LOCATION ATTRIBUTES FROM GOOGLE MAPS API 
def getplace(lat, lon):
    try:
        url = "https://maps.googleapis.com/maps/api/geocode/json?latlng={0},{1}&sensor=false&key={2}".format(lat,lon,gmapkey)
        resp = r.get(url).json()
        inner_text = resp['results'][0]['address_components']
        country = city = post_code = city_level_2 = None
        for c in inner_text:
            if "country" in c['types']:
                country = c['long_name']
            if "administrative_area_level_1" in c['types']:
                city = c['long_name']
            if "locality" in c['types']:
                city_level_2 = c['long_name']
            if "postal_code" in c['types']:
                post_code = c['long_name']
    except IndexError as e:
        country = city = city_level_2 =post_code  = ''
    return city, city_level_2, post_code, country

## NOTE: UPDATE DATAFRAME WITH NEW ATTRIBUTES
for index,row in df.iterrows():
    geo = getplace(row['Begin Trip Lat'],row['Begin Trip Lng'])
    print(geo)
    df.loc[index,'city_new'] =geo[0]
    df.loc[index,'city_level_2'] =geo[1]
    df.loc[index,'post_code'] =geo[2]
    df.loc[index,'country'] =geo[3]

df.to_csv(tgt_dr+'/{}_trips_data.csv'.format(fullname),sep =',',index=False, encoding='utf-8-sig')
