import pandas as pd
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time

url = 'https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html'






driver = webdriver.Chrome(executable_path = '/usr/bin/chromedriver')
driver.get(url)



# scrolling

lastHeight = driver.execute_script("return document.body.scrollHeight")


pause = 0.5
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(pause)
    newHeight = driver.execute_script("return document.body.scrollHeight")
    if newHeight == lastHeight:
        break
    lastHeight = newHeight


soup = BeautifulSoup(driver.page_source, 'html.parser')

mydivs = soup.find_all("div", {"class": "date"})
list_of_dates = [row.text for row in mydivs]

df = pd.DataFrame({'date': list_of_dates, 'type': 'Monetary Policy Decision'})
df.date = pd.to_datetime(df.date)
df.to_csv("ecb_decision_dates.csv", index = False)
