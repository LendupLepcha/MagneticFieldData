# imports
import random
import psycopg2
from calendar import monthrange
import requests
import time
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import time
import math
import copy
from matplotlib import pyplot as plt
from matplotlib import dates as mpl_dates
from datetime import datetime
# %matplotlib inline 
plt.style.use('fivethirtyeight')

# PSQL connection
connection = psycopg2.connect(user="postgres",
                              password="@#bedrock$203",
                              host="1.7.151.13",
                              port="5432",
                              database="cosmosis")

# web issues precaution
def GET_UA():
    uastrings = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"\
                ]
    return random.choice(uastrings)

# get regions' details
def getRegions(connection):
    print('getting list of Regions')
    cursor = connection.cursor()
    postgreSQL_select_Query = "select id,region_name,region_api from public.data_region"
    cursor.execute(postgreSQL_select_Query)
    regions = cursor.fetchall()
    return regions
def getObs(connection,region_id,year):
#     print('getting Observaratory based on regionid'+str(region_id))
    cursor = connection.cursor()
    postgreSQL_select_Query = "select code,station_name,year,id,lat,lon FROM public.data_observatory where year="+str(year)+" and region_id="+str(region_id)
    cursor.execute(postgreSQL_select_Query)
    regions = cursor.fetchall()
    return regions

# get regional code of closest observatory
def get_obs(oobs, lat, lon):
    
    dist = 360
    code = ''
    cname = ''
    for i in oobs:
        x, y = lon, lat
        x1, y1 = i[5], i[4]
        temp = math.sqrt((x-x1)**2 + (y-y1)**2)
#         print(temp, i[0])
        if temp < dist:
            dist = temp
            code = i[0]
            cname = i[1]
    if code == '':
        print('Cannot Access Region Code')
    else:
        return code, cname
    
# Get Data from observatory (within the hour)
def getData(year,month,day,hour,minute,code):
    final_data = []
    in_hour_data = []
    try:
        session = requests.Session()
        retry = Retry(connect=5, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        headers = {'User-Agent': GET_UA()}   
        URL = "https://intermagnet.org/data-donnee/dataplot-1-eng.php?year="+str(year)+"&month="+str(month)+"&day="+str(day)+"&start_hour="+str(hour)+"&end_hour="+str(hour+1)+"&filter_region%5B%5D=America&filter_region%5B%5D=Asia&filter_region%5B%5D=Europe&filter_region%5B%5D=Pacific&filter_region%5B%5D=Africa&filter_lat%5B%5D=NH&filter_lat%5B%5D=NM&filter_lat%5B%5D=E&filter_lat%5B%5D=SM&filter_lat%5B%5D=SH&sort=iaga&iaga_code="+code+"&type=xyz&fixed_scale=1&format=html"
        r =  session.get(URL, headers=headers) 
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find(lambda tag: tag.name=='table')
        time_s = []
        for tt in [year, month, day, hour, minute]:
            if(tt<10):
                temp = '0'+str(tt)
            else:
                temp = str(tt)
            time_s.append(temp)
        time_check = time_s[0]+'-'+time_s[1]+'-'+time_s[2]+' '+time_s[3]+':'+time_s[4]+':00'
#         print(time_check)
        try:
            rows = table.findAll(lambda tag: tag.name=='tr')
            for row in rows:
                cols = row.findAll('td')
                try :
                    if (cols[0]!=None):
                        if(str(cols[0].text) == time_check):
#                             print(time_check)
                            final_data.extend([str(cols[0].text),cols[1].text,cols[2].text,cols[3].text,cols[4].text, code])
                        in_hour_data.append([cols[0].text,cols[1].text,cols[2].text,cols[3].text,cols[4].text,code])

                except Exception as err:
                    print(err)
                    pass
            return final_data, in_hour_data
#             print("Data Saved")
        except:
            print('Data not present')
            pass
    except requests.exceptions.RequestException as e:
        print(e)
        time.sleep(300)
    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))            
        renewIPadress()       
    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
        renewIPadress()       
    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
        renewIPadress()
    except KeyboardInterrupt:
        print("Someone closed the program") 
        
# main Function
def get_magnetic_data(latitude, longitude, year, month, day, hour, minute):
    regions=getRegions(connection)
    oobs = [] 
    for region in regions:

        region_id=region[0]
        region_name=region[1]
        region_api=region[2]
        obs=getObs(connection, region_id, year) 
        for ob in obs:
            code=ob[0]
            ob_name=ob[1]
            year=ob[2]
            obj_id=ob[3]
            lat=float(ob[4])
            if (float(ob[5])>180):
                lon = float(ob[5]) - 360
            else:
                lon=float(ob[5])
            oobs.append([code, ob_name, year, obj_id, lat, lon])
    code, obname = get_obs(oobs, latitude, longitude)
    datass, hour_data = getData(year,month,day,hour,minute,code)

    hour_data = pd.DataFrame(hour_data, columns = ['DateTime', 'Xn(T)', 'Yn(T)', 'Zn(T)', 'Fn(T)', 'code'])
    # print(hour_data.head(10))
    
    # Plot Data 
    x_axis = hour_data['DateTime']
    x = []
    for i in x_axis:
        x.append(datetime.fromisoformat(i))
    y = []
    labels = ['Xn(T)', 'Yn(T)', 'Zn(T)', 'Fn(T)']
    for i in labels:
        temp = hour_data[i]
        temp = temp.astype('float64')
        y.append(temp)
    fig, ax = plt.subplots(nrows = 4, ncols = 1)
    date_format = mpl_dates.DateFormatter('%H:%M')
    st = fig.suptitle('Data from '+obname+' observatory')
#     st.set_y(1.01)
    for i in range(4):
        ax[i].plot(x, y[i], linewidth = 1)
        ax[i].plot(datetime.fromisoformat(datass[0]), float(datass[i+1]), 'ro');
        ax[i].text(datetime.fromisoformat(datass[0]), float(datass[i+1]), " - "+labels[i]+"="+str(datass[i+1]))
        ax[i].xaxis.set_major_formatter(date_format)
        ax[i].set_ylabel(labels[i], rotation = 0, labelpad=30, fontsize=15)
    plt.xlabel('Observation time (within the hour)' )
    fig.set_size_inches(12, 8)
    fig.tight_layout()
    plt.savefig('MagneticXYZ.jpg')