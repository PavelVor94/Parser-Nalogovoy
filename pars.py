import requests
import time
import re
from pandas import DataFrame
import sys

OKVED = 63
LIMIT = 15000
STEP = 500
FOR_PAGE = 2000

URL_CARD = 'https://bo.nalog.ru/organizations-card/'
URL_INFO = 'https://bo.nalog.ru/nbo/organizations/'
URL_SUB_DETAILS = 'https://bo.nalog.ru/nbo/organizations/number-company/bfo/'
URL_DETAILS = "https://bo.nalog.ru/nbo/bfo/number-company/details"



HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
        }

start = time.time()
list_dicts=[]
added = []
count=0

def load_main_page():
        for inn in generate_inn():
            url = f"https://bo.nalog.ru/nbo/organizations/?allFieldsMatch=false&inn={inn}&okved={OKVED}&page=0"
            try:
                response = requests.get(url,  headers=HEADERS , params = {"size": FOR_PAGE})
                if response.content.decode():
                    links = response.json()['content']
                    for link in links:
                        if link['id'] not in added:
                            load_company_page(str(link['id']))
                            if count >= LIMIT : break
            except:
                print("Ошибка загрузки страницы")
                print(sys.exc_info())
            if count >= LIMIT: break

        DataFrame(list_dicts).to_excel('./result.xlsx' , ';' , index=False)
        print(time.time() - start)

def generate_inn():
    for f in range(10):
        for m in range(10):
            for l in range(10):
                yield f'{f}{m}{l}'


def load_company_page(id_company):
    global count
    info = connect_to_info(id_company)
    details = connect_to_result(id_company)
    if info and details:
        count += 1
        list_dicts.append({
            "#": count,
            "ИНН": info['inn'],
            "ОКВЭД": info['okved2']['id'],
            "Расшифровка ОКВЭД": info['okved2']['name'],
            "Наименование": info['fullName'].replace('ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ' , 'ООО'),
            "Ссылка на сайте": URL_CARD+id_company,
            "Адрес": create_adress(info),
            "Действует с": reformat_date(info['registrationDate']),
            "Отчетный период": choose_year(info),
            "1600:Активы 2017": create_aktiv('1600' , 2017 , details),
            "1600:Активы 2018": create_aktiv('1600' , 2018 , details),
            "1600:Активы 2019": create_aktiv('1600' , 2019 , details),
             "1600:Активы 2020": create_aktiv('1600' , 2020 , details),
             "2110:Выручка 2017": create_aktiv('2110' , 2017 , details),
             "2110:Выручка 2018": create_aktiv('2110' , 2018 , details),
             "2110:Выручка 2019": create_aktiv('2110' , 2019 , details),
             "2110:Выручка 2020": create_aktiv('2110' , 2020 , details),
             "2400:Чистая прибыль(Убыток) 2017": create_aktiv('2400' , 2017 , details),
             "2400:Чистая прибыль(Убыток) 2018": create_aktiv('2400' , 2018 , details),
             "2400:Чистая прибыль(Убыток) 2019": create_aktiv('2400' , 2019 , details),
             "2400:Чистая прибыль(Убыток) 2020": create_aktiv('2400' , 2020 , details),
             "4322:Дивиденды 2017": create_aktiv('4322' , 2017 , details),
              "4322:Дивиденды 2018": create_aktiv('4322' , 2018 , details),
              "4322:Дивиденды 2019": create_aktiv('4322' , 2019 , details),
              "4322:Дивиденды 2020": create_aktiv('4322' , 2020 , details),
        })
        added.append(int(id_company))
        print(f'сделано {count} из {LIMIT} --- {count/LIMIT*100:.2f}%')

        if count % STEP == 0:
            DataFrame(list_dicts).to_excel('./result.xlsx' , ';' , index=False)


def reformat_date(old_date):
    l = old_date.split('-')
    d = l[2]
    m = l[1]
    y = l[0]
    return f'{d}.{m}.{y}'


def create_aktiv(code : str, year, details):
    last_year = 0
    previus_year = 0
    before_year = 0
    index_last_year = 0
    index_previus_year = 0
    index_before_year = 0
    for (i,detail) in enumerate(details):
        if int(detail['datePresent'].split('-')[0]) > int(last_year):
            last_year=detail['datePresent'].split('-')[0]
            index_last_year = i
    if len(details) > 1:
        for (i, detail) in enumerate(details):
            if int(detail['datePresent'].split('-')[0]) == (int(last_year)-1):
                previus_year = detail['datePresent'].split('-')[0]
                index_previus_year = i
        for (i, detail) in enumerate(details):
            if int(detail['datePresent'].split('-')[0]) == (int(previus_year)-1):
                before_year = detail['datePresent'].split('-')[0]
                index_before_year = i

    if code == '1600':
        if last_year == '2021':
            if year == 2017:
                if previus_year:
                    for (key,value) in details[index_previus_year].items():
                        if isinstance(value, dict):
                            if res := value.get('beforePrevious'+code, ""): return res
                else: return ""
            if year == 2018:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('beforePrevious'+code, ""): return res
            if year == 2019:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('previous'+code, ""): return res
            if year == 2020:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('current'+code, ""): return res
        else:
            if year == 2017:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('beforePrevious'+code, ""): return res
            if year == 2018:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('previous'+code, ""): return res
            if year == 2019:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('current'+code, ""): return res
            if year == 2020: return ''
    else:
        if last_year == '2021':
            if year == 2017:
                if before_year:
                    for (key,value) in details[index_before_year].items():
                        if isinstance(value, dict):
                            if res := value.get('previous'+code, ""): return res
                else: return ""
            if year == 2018:
                if previus_year:
                    for (key,value) in details[index_previus_year].items():
                        if isinstance(value, dict):
                            if res := value.get('previous'+code, ""): return res
                else: return ''
            if year == 2019:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('previous'+code, ""): return res
            if year == 2020:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('current'+code, ""): return res
        else:
            if year == 2017:
                if previus_year:
                    for (key,value) in details[index_previus_year].items():
                        if isinstance(value, dict):
                            if res := value.get('previous'+code, ""): return res
                else: return ""
            if year == 2018:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('previous'+code, ""): return res
            if year == 2019:
                for (key,value) in details[index_last_year].items():
                    if isinstance(value, dict):
                        if res := value.get('current'+code, ""): return res
            if year == 2020: return ''





def choose_year(info):
    year = 0
    for i in info['bfo']:
        if (int(i['period']) > int(year)): year = i['period']
    return year


def create_adress(info):
    result=[]
    if info['index']: result.append(info['index'])
    if info['region']:result.append(info['region'])
    if info['district']: result.append(info['district'])
    if info['city']: result.append(info['city'])
    if info['settlement']: result.append(info['settlement'])
    if info['street']: result.append(info['street'])
    if info['house']: result.append(info['house'])
    if info['building']: result.append(info['building'])
    if info['office']: result.append(info['office'])
    return str(result)[1:-1].replace('\'' , '')


def connect_to_info(id_company):
    time.sleep(0.03)
    try:
        response = requests.get(URL_INFO+id_company , headers=HEADERS)
        return response.json()
    except:
        print("failed info", URL_CARD+id_company)
        print(sys.exc_info())

def connect_to_result(id_company):
        time.sleep(0.03)
        try:
            response = requests.get(URL_SUB_DETAILS.replace('number-company' , id_company) , headers=HEADERS)
            id_results=re.findall(r'("id":[0-9]+)' , response.content.decode())
            ids = []
            for i in id_results:
                ids.append(i.split(':')[1])
            list_results = []
            for l in ids:
                time.sleep(0.03)
                response = requests.get(URL_DETAILS.replace('number-company', l), headers=HEADERS)
                if response.content.decode():
                    for i in response.json():
                        list_results.append(i)
            return list_results
        except:
            print("failed detail", URL_CARD+id_company)
            print(sys.exc_info())


load_main_page()

