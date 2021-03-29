from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import lxml.html as html
from io import StringIO
import aiohttp
import asyncio
import time

OKVED = 63
URL_SEARCH = f"https://bo.nalog.ru/search?allFieldsMatch=false&okved={OKVED}&page=1"
URL_BASE = 'https://bo.nalog.ru'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"
        }

async def load_main_page():
    count = 0
    driver = webdriver.Chrome()
    driver.get(URL_SEARCH)
    time.sleep(3)
    actions = ActionChains(driver)
    actions.move_by_offset(100, 100).perform()
    actions.click().perform()
    while driver.find_elements_by_class_name("button_sm") and count < 100:
        driver.find_elements_by_class_name("button_sm")[0].click()
        time.sleep(0.15)
        count+=20
    main_page = driver.find_element_by_tag_name('html')
    tree = html.parse(StringIO(main_page.get_attribute(('innerHTML'))))
    links = tree.xpath("//div[contains(@class, 'results-search-tbody')]/a")
    print(links[0].attrib['href'])
    for link in links:
        await load_company_page(URL_BASE+link.attrib['href'])

async def load_company_page(url):
        tree = await connect_to_page(url)
        print (html.etree.tostring(tree, pretty_print= True).decode('utf-8'))

async def connect_to_page(url):
    async with aiohttp.ClientSession() as session:
        response = await session.get(url, headers=HEADERS)
        print(url)
        print(StringIO(await response.text()))
        return html.parse(StringIO(await response.text()))

loop = asyncio.get_event_loop()
loop.run_until_complete(load_main_page())