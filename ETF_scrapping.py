import os
import re
import time
from pathlib import Path

from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox import options as firefox_options
from selenium.webdriver.common.by import By

import concurrent.futures

def get_page(urls):

  pages = []     
  
  def request_url(url):
    
    driver = webdriver.Firefox()
    
    driver.get(url)
    time.sleep(1)
    pages.append(driver.page_source.encode("utf-8"))
    driver.close()
  
  with concurrent.futures.ThreadPoolExecutor() as executor :
    executor.map(request_url, urls)
    
  return pages

def save_pages(folder_name, name_page, pages):

  os.makedirs(f"Dataset/pages/{folder_name}", exist_ok=True)
  for page_nb, page in enumerate(pages):
    with open(f"Dataset/pages/{folder_name}/{name_page}_{page_nb}.html", "wb") as f_out:
      f_out.write(page) 

def parse_pages(folder_name):
    pages_paths = os.listdir(f"Dataset/pages/{folder_name}")

    #   results = pd.DataFrame()
    results = []

    for page_path in pages_paths:
        with open(f"Dataset/pages/{folder_name}/" + page_path, "rb") as f_in:
            page = f_in.read().decode("utf-8")
            result = parse_page(page)
            results += result
    return results

def parse_page(page):

    # result = pd.DataFrame()
    result = []
    soup = BeautifulSoup(page, "html.parser")

    rows = soup.find("table").find("tbody").find_all("tr")
    for row in rows:
        cells = row.find("a")
        result.append(cells['href'])
    
    return result


def request_save_list_etf_pages():
  
  urls = []
  max_pages = 7

  for page_nb in range(1, max_pages + 1):
    page_url = f"https://www.boursorama.com/bourse/trackers/recherche/autres/page-{page_nb}?beginnerEtfSearch[current]=longTerm&beginnerEtfSearch[isEtf]=1&beginnerEtfSearch[taxation]=1"
    urls.append(page_url) 

  pages = get_page(urls)
  save_pages('list_etf', 'list_etf_page', pages)

def request_download_etf(list_etf_links):

  urls = ['https://www.boursorama.com' + etf_link for etf_link in list_etf_links]
  print(urls[0:2])


  def request_url(url):
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", True)
    profile.set_preference("browser.download.dir", 'C:\\Users\\tesch\\OneDrive\\Bureau\\Portfolio\\Dataset\\ETF')  
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")

    driver = webdriver.Firefox(firefox_profile=profile)

    try:

      driver.get(url)

      # Decline Cookies
      # cookie_accepting = driver.find_element(By.CLASS_NAME, "didomi-continue-without-agreeing")
      cookie_accepting = driver.find_element(By.ID, "didomi-notice-agree-button")
      
      cookie_accepting.click()
      
      # Change temporality to 10 years
      time_10_years = driver.find_element(By.XPATH, "//div[@data-brs-quote-chart-duration-length='3650']")
      time_10_years.click()

      # Download
      # download_button = driver.find_element(By.CLASS_NAME, "c-quote-chart__quick-command")
      download_button = driver.find_element(By.XPATH, '//div[@aria-label="Télécharger les cotations"]')
      download_button.click()

      time.sleep(1)

      driver.quit()

    except Exception as e:
      print (e)


  with concurrent.futures.ThreadPoolExecutor() as executor :
    executor.map(request_url, urls[0:10])

  # for url in urls[0:10] :
  #   request_url(url)

def main():

  
  # print("File      Path:", Path(__file__).absolute())
  # print("Directory Path:", Path().absolute()) # Directory of current working directory, not __file__
  
  # URLs pages with list of ETFs

  # request_save_list_etf_pages()

  # Load list_etf pages
  list_etf_links = parse_pages('list_etf')

  # Request each ETF and Download Data
  request_download_etf(list_etf_links)

#   results.to_csv('df/200_movies.csv')
#   df = pd.read_csv("df/200_movies.csv")
#   print(df.describe())
    
if __name__ == "__main__":
    main()
    