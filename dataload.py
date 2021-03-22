import random
import psycopg2
from calendar import monthrange
import requests
import time
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

total_region=[]
total_ob=[]

def getRegions(connection):
    print('getting list of Regions')
    cursor = connection.cursor()
    postgreSQL_select_Query = "select id,region_name,region_api from public.data_region"
    cursor.execute(postgreSQL_select_Query)
    regions = cursor.fetchall() 
    return regions

def getObs(connection,region_id):
    print('getting Observaratory based on regionid'+str(region_id))
    cursor = connection.cursor()
    postgreSQL_select_Query = "select code, station_name,year,id FROM public.data_observatory where year=2020 and region_id="+str(region_id)
    cursor.execute(postgreSQL_select_Query)
    regions = cursor.fetchall() 
    return regions 

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

def getData(connection,year,region_api,region_name,code,ob_name,region_id,obj_id):
    months=[]
    final_data=[]
    print("YEAR",year)
    print('REGION _ID:',region_id)
    print('OB_ID:',obj_id)
    for i in range(1,13):
        months.append(i)
    for month in months:
        print('MONTH',month)
        days=monthrange(year,month)
        no_of_day=days[1]
        print("No of days in month:"+str(month)+ '\t is:' + str(no_of_day))
        for day in range(1,no_of_day+1):
            try:
                session = requests.Session()
                retry = Retry(connect=5, backoff_factor=0.5)
                adapter = HTTPAdapter(max_retries=retry)
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                headers = {'User-Agent': GET_UA()}
                URL = "https://www.intermagnet.org/data-donnee/dataplot-1-eng.php?year="+ str(year)+"&month="+str(month)+"&day="+str(day)+"&start_hour=0&end_hour=24&filter_region%5B%5D="+region_api+"&filter_lat%5B%5D=NH&filter_lat%5B%5D=NM&filter_lat%5B%5D=E&filter_lat%5B%5D=SM&filter_lat%5B%5D=SH&sort=name&iaga_code="+code+"&type=xyz&fixed_scale=1&format=html"
                r =  session.get(URL, headers=headers)
                soup = BeautifulSoup(r.content, 'html.parser')
                table = soup.find(lambda tag: tag.name=='table')
                try:
                    rows = table.findAll(lambda tag: tag.name=='tr')
                    for row in rows:
                        cols = row.findAll('td')
                        try :
                            print("No of days in month:"+str(month)+ '\t is:' + str(day))
                            insertdata(connection,cols[0].text,cols[1].text,cols[2].text,cols[3].text,obj_id,region_id,cols[4].text,year)
                        except Exception as err:
                            print(err)
                            pass
                except:
                    print("data not available for region:" + str(region_id) + " ob_id: " + str(obj_id) + " year:" + str(year) + "-" + str(month) + "-"+str(day))
                    pass
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(300)
            except requests.ConnectionError as e:
                print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
                print(str(e))
                renewIPadress()
                continue
            except requests.Timeout as e:
                print("OOPS!! Timeout Error")
                print(str(e))
                renewIPadress()
                continue
            except requests.RequestException as e:
                print("OOPS!! General Error")
                print(str(e))
                renewIPadress()
                continue
            except KeyboardInterrupt:
                print("Someone closed the program")

def insertdata(connection,Date_Time_UT,X_nT,Y_nT,Z_nT,observatory_id,region_id,F_nT,year):
    cursor = connection.cursor()
    print('here i am')
    postgres_insert_query ='INSERT INTO public.data_data("Date_Time_UT","X_nT","Y_nT","Z_nT","observatory_id","region_id","F_nT","year") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT ("id") DO NOTHING;'
    print(postgres_insert_query)
    record_insert= (Date_Time_UT,X_nT,Y_nT,Z_nT,observatory_id,region_id,F_nT,year)
    cursor.execute(postgres_insert_query,record_insert)
    connection.commit()
    count = cursor.rowcount
    print (count, "Record inserted successfully")
    print("YEAR",year)
    print('REGION _ID:',region_id)
    print('OB_ID:',observatory_id)
    print(observatory_id,'comming from db')
try:
    connection = psycopg2.connect(user="postgres",
                                  password="@#bedrock$203",
                                  host="1.7.151.13",
                                  port="5432",
                                  database="cosmosis")
    regions=getRegions(connection)
    for region in regions:
        region_id=region[0]
        region_name=region[1]
        region_api=region[2]
        obs=getObs(connection,region_id)
        for ob in obs:
            code=ob[0]
            ob_name=ob[1]
            year=ob[2]
            obj_id=ob[3]
            getData(connection,year,region_api,region_name,code,ob_name,region_id,obj_id)
except (Exception, psycopg2.Error) as error :
    print ("Error while fetching data from PostgreSQL", error)
finally:
    if(connection):
        connection.close()
        print("PostgreSQL connection is closed")

