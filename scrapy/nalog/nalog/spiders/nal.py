import scrapy
import time
import re
from pandas import DataFrame
import sys
import json
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
        }

class NalSpider(scrapy.Spider):
    name = 'nal'
    allowed_domains = ['bo.nalog.ru']
    start_urls = ['https://bo.nalog.ru']

    OKVED = 63
    LIMIT = 15000
    STEP = 5000

    URL_CARD = 'https://bo.nalog.ru/organizations-card/'
    URL_INFO = 'https://bo.nalog.ru/nbo/organizations/'
    URL_SUB_DETAILS = 'https://bo.nalog.ru/nbo/organizations/number-company/bfo/'
    URL_DETAILS = "https://bo.nalog.ru/nbo/bfo/number-company/details"

    added = []
    list_dicts = []
    count = 0
    start = time.time()


    def generate_inn(self):
        for f in range(10):
            for m in range(10):
                for l in range(10):
                    yield f'{f}{m}{l}'

    def parse(self, response):
        for inn in self.generate_inn():
            yield scrapy.FormRequest(
                url=f"https://bo.nalog.ru/nbo/organizations/?allFieldsMatch=false&inn={inn}&okved={self.OKVED}&page=0",
                method='GET',
                formdata={"size": '2000'},
                callback=self.parse_search,
                dont_filter=True
            )

    def parse_search(self, response):
        links = json.loads(response.text)['content']
        for link in links:
            if link['id'] not in self.added:
                yield  scrapy.Request(self.URL_INFO+str(link['id']), callback=self.load_page, meta={"id_company": str(link['id'])}, dont_filter=True)

    def load_page(self, response):
        info = json.loads(response.text)
        yield scrapy.Request(self.URL_SUB_DETAILS.replace('number-company', response.meta['id_company']), callback=self.load_company, meta={"id_company": response.meta['id_company'],
                                                                                                                                            "info": info}, dont_filter=True)

    def load_company(self, response):
        id_results = re.findall(r'("id":[0-9]+)', response.text)
        ids = []
        for i in id_results:
            ids.append(i.split(':')[1])
        details = []
        for l in ids:
            respons = requests.get(self.URL_DETAILS.replace('number-company', l), headers=HEADERS)
            if respons.content.decode():
                for i in respons.json():
                    details.append(i)
        info = response.meta['info']
        if info and details:
            self.list_dicts.append({
                "#": self.count,
                "ИНН": info['inn'],
                "ОКВЭД": info['okved2']['id'],
                "Расшифровка ОКВЭД": info['okved2']['name'],
                "Наименование": info['fullName'].replace('ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ', 'ООО'),
                "Ссылка на сайте": self.URL_CARD + response.meta['id_company'],
                "Адрес": self.create_adress(info),
                "Действует с": self.reformat_date(info['registrationDate']),
                "Отчетный период": self.choose_year(info),
                "1600:Активы 2017": self.create_aktiv('1600', 2017, details),
                "1600:Активы 2018": self.create_aktiv('1600', 2018, details),
                "1600:Активы 2019": self.create_aktiv('1600', 2019, details),
                "1600:Активы 2020": self.create_aktiv('1600', 2020, details),
                "2110:Выручка 2017": self.create_aktiv('2110', 2017, details),
                "2110:Выручка 2018": self.create_aktiv('2110', 2018, details),
                "2110:Выручка 2019": self.create_aktiv('2110', 2019, details),
                "2110:Выручка 2020": self.create_aktiv('2110', 2020, details),
                "2400:Чистая прибыль(Убыток) 2017": self.create_aktiv('2400', 2017, details),
                "2400:Чистая прибыль(Убыток) 2018": self.create_aktiv('2400', 2018, details),
                "2400:Чистая прибыль(Убыток) 2019": self.create_aktiv('2400', 2019, details),
                "2400:Чистая прибыль(Убыток) 2020": self.create_aktiv('2400', 2020, details),
                "4322:Дивиденды 2017": self.create_aktiv('4322', 2017, details),
                "4322:Дивиденды 2018": self.create_aktiv('4322', 2018, details),
                "4322:Дивиденды 2019": self.create_aktiv('4322', 2019, details),
                "4322:Дивиденды 2020": self.create_aktiv('4322', 2020, details),
            })
            self.added.append(int(response.meta['id_company']))
            self.count += 1
            print(f'сделано {self.count} из {self.LIMIT} --- {self.count/self.LIMIT*100:.2f}%')
            if self.count >= self.LIMIT: raise scrapy.exceptions.CloseSpider('MySpider is quitting now.')

            if self.count % self.STEP == 0:
                DataFrame(self.list_dicts).to_excel('./result.xlsx' , ';' , index=False)



    def reformat_date(self, old_date):
        l = old_date.split('-')
        d = l[2]
        m = l[1]
        y = l[0]
        return f'{d}.{m}.{y}'

    def create_aktiv(self, code: str, year, details):
        last_year = 0
        previus_year = 0
        before_year = 0
        index_last_year = 0
        index_previus_year = 0
        index_before_year = 0
        for (i, detail) in enumerate(details):
            if int(detail['datePresent'].split('-')[0]) > int(last_year):
                last_year = detail['datePresent'].split('-')[0]
                index_last_year = i
        if len(details) > 1:
            for (i, detail) in enumerate(details):
                if int(detail['datePresent'].split('-')[0]) == (int(last_year) - 1):
                    previus_year = detail['datePresent'].split('-')[0]
                    index_previus_year = i
            for (i, detail) in enumerate(details):
                if int(detail['datePresent'].split('-')[0]) == (int(previus_year) - 1):
                    before_year = detail['datePresent'].split('-')[0]
                    index_before_year = i

        if code == '1600':
            if last_year == '2021':
                if year == 2017:
                    if previus_year:
                        for (key, value) in details[index_previus_year].items():
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





    def choose_year(self, info):
        year = 0
        for i in info['bfo']:
            if (int(i['period']) > int(year)): year = i['period']
        return year


    def create_adress(self, info):
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

    def closed(self, reason):
        DataFrame(self.list_dicts).to_excel('./result.xlsx' , ';' , index=False)
        print(time.time()-self.start)
