import os
import time
from pathlib import Path

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as firefoxOptions
from selenium.webdriver.common.by import By

import concurrent.futures
from concurrent.futures import as_completed

from threading import Lock

path_folder = Path(os.getcwd())

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
    # pages_paths = os.listdir(f"Dataset/pages/{folder_name}")
    pages_paths = os.listdir(str(path_folder / f'Dataset/pages/{folder_name}'))

    #   results = pd.DataFrame()
    results = []

    for page_path in pages_paths:
        # with open(f"Dataset/pages/{folder_name}/" + page_path, "rb") as f_in:
        with open(str(path_folder / f'Dataset/pages/{folder_name}/{page_path}'), "rb") as f_in:
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

    def request_url(url):

        options = firefoxOptions()
        options.add_argument("--headless")
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", True)
        options.set_preference("browser.download.dir", str(path_folder / 'Dataset/ETF/'))
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")

        driver = webdriver.Firefox(options=options)

        try:

            driver.get(url)
            
            # time.sleep(1)
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

            time.sleep(10)

            driver.quit()

        except Exception as e:
            print(e)
            driver.quit()

    # simple progress indicator callback function
    def progress_indicator(future):
        nonlocal lock, tasks_total, tasks_completed
        # obtain the lock
        with lock:
            # update the counter
            tasks_completed += 1
            # report progress
            print(f'{tasks_completed}/{tasks_total} completed, {tasks_total-tasks_completed} remain.')

    # create a lock for the counter
    lock = Lock()
    # total tasks we will execute
    tasks_total = len(urls)
    # total completed tasks
    tasks_completed = 0

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # executor.map(request_url, urls, timeout=5)
        futures = [executor.submit(request_url, url) for url in urls]

        for future in as_completed(futures, timeout=None):
            try:
                future.add_done_callback(progress_indicator)
                # print(future.result(timeout=20))

            except concurrent.futures.TimeoutError:
                print("Timeout")


        executor.shutdown()

    print("--- %s seconds ---" % (time.time() - start_time))
    return 'Done'

def main():
    # geckodriver_autoinstaller.install()

  # 1. URLs pages with list of ETFs
  # request_save_list_etf_pages()

  # 2. Load list_etf pages
    list_etf_links = parse_pages('list_etf')

  # 3. Request each ETF and Download Data in folder ETF
    request_download_etf(list_etf_links[0:2])

if __name__ == "__main__":
    main()
    