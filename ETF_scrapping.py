import os
import time
from pathlib import Path
import pickle
import re
from os import walk

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as firefoxOptions
from selenium.webdriver.common.by import By

import concurrent.futures
from concurrent.futures import as_completed

from threading import Lock

# import pywatch

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

        nonlocal dict_etf

        options = firefoxOptions()
        options.add_argument("--headless")
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", True)
        options.set_preference("browser.download.dir", str(path_folder / 'Dataset/ETF/'))
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")

        driver = webdriver.Firefox(options=options)

        try:
            driver.get(url)

            # Accept Cookies
            # cookie_accepting = driver.find_element(By.CLASS_NAME, "didomi-continue-without-agreeing")
            cookie_accepting = driver.find_element(By.ID, "didomi-notice-agree-button")

            cookie_accepting.click()

            '''
            dict_etf = {
                        codeisin : codeisin
                        dict_copmanies : {},
                        dict_countries : {},
                        dict_activites : {}
                        }
            '''
            # Cours Tab

            # Code ISIN of the ETF (12 characters)
            code_isin = driver.find_element(By.XPATH, "//h2[@class='c-faceplate__isin']").text[0:12]

            # Name of the ETF
            name_etf = driver.find_element(By.XPATH, "//a[@class='c-faceplate__company-link ']").text.replace(' ', '')
            name_etf = re.sub('[^A-Za-z0-9]+', '', name_etf)
            name_etf = name_etf.upper()

            time.sleep(4)

            # Composition Tab
            composition_tab = driver.find_element(By.XPATH, "//nav[@class='c-submenubar / o-list-inline']/ul/li[3]")
            composition_tab.click()

            # 1. Companies
            dict_companies = {}
            table_companies = driver.find_element(By.XPATH, "//table[@class='c-table c-table--bottom-space']")
            rows_companies = table_companies.find_elements(By.TAG_NAME, "tr")

            for row in rows_companies:
                row_companie_name = row.find_elements(By.TAG_NAME, "td")[0].text
                row_companie_td = row.find_elements(By.TAG_NAME, "td")[1]
                row_companie_percentage = row_companie_td.find_element(By.TAG_NAME, "div").get_attribute('data-gauge-current-step')

                dict_companies[row_companie_name] = row_companie_percentage


            # 2. Countries
            dict_countries = {}
            # table_countries_g = driver.find_element(By.XPATH, "//div[@id='regional']").find_element(By.XPATH, "//div[@class='amChartsLegend amcharts-legend-div']").find_element(By.TAG_NAME, "g").find_element(By.TAG_NAME, "g")
            table_countries_g = driver.find_elements(By.XPATH, "//div[@class='amChartsLegend amcharts-legend-div']")[0].find_element(By.TAG_NAME, "g").find_element(By.TAG_NAME, "g")
            table_countries = table_countries_g.find_elements(By.TAG_NAME, "g")

            for row in table_countries:
                if row.get_attribute('aria-label') is None :
                    continue
                else:
                    row_country_properties = re.split('(\d+)', row.text)

                    # Name Country
                    row_country_name = row_country_properties[0]
                    # Percentage Country
                    row_country_percentage = ''.join(row_country_properties[1:])
                    dict_countries[row_country_name] = row_country_percentage

            # 3. Activites
            dict_activities = {}
            # table_activities_div_sector = driver.find_element(By.XPATH, "//div[@id='sector']")

            # table_activities_g = table_activities_div_sector.find_elements(By.XPATH, "//div[@class='amChartsLegend amcharts-legend-div']")[2]
            try:
                table_activities_g = driver.find_elements(By.XPATH, "//div[@class='amChartsLegend amcharts-legend-div']")[2]
                table_activities = table_activities_g.find_element(By.TAG_NAME, "g").find_element(By.TAG_NAME, "g").find_elements(By.TAG_NAME, "g")

                for row_activity in table_activities:
                    if row_activity.get_attribute('aria-label') is None:
                        continue
                    else:
                        row_activities_properties = re.split('(\d+)', row_activity.text)

                        # Activity Name
                        activity_name = row_activities_properties[0]
                        activity_percentage = ''.join(row_activities_properties[1:])

                        dict_activities[activity_name] = activity_percentage
            except Exception as e:
                print(url)

            etf_properties = {
                'codeisin': code_isin,
                'dict_companies': dict_companies,
                'dict_countries': dict_countries,
                'dict_activities': dict_activities
            }

            dict_etf[name_etf] = etf_properties
            '''
            # Swtich to Cours tab
            cours_tab = driver.find_element(By.XPATH, "//a[@class='c-submenubar__link ']")
            cours_tab.click()

            time.sleep(3)
            
            # Change temporality to 10 years
            time_10_years = driver.find_element(By.XPATH, "//div[@data-brs-quote-chart-duration-length='3650']")
            time_10_years.click()

            # Download
            download_button = driver.find_element(By.CLASS_NAME, "c-quote-chart__quick-command")
            download_button = driver.find_element(By.XPATH, '//div[@aria-label="Télécharger les cotations"]')
            download_button.click()
            '''
            time.sleep(8)

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

    # Dictionnary of (ETF name and Code ISIN)
    dict_etf = {}
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

    with open('./Dataset/Pickles/dict_etf.pickle', 'wb') as handle:
        pickle.dump(dict_etf, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return dict_etf

def rename_etf_files():

    start_time = time.time()

    with open('./Dataset/Pickles/dict_etf.pickle', 'rb') as handle:
        dict_etf = pickle.load(handle)

        for (_, _, filenames) in walk('./Dataset/ETF/'):
            for filename in filenames:
                for name_etf in dict_etf.keys():
                    if name_etf in filename:
                        # Rename as : nameETF_ISNCode.txt
                        new_filename = name_etf + "_" + dict_etf[name_etf]['codeisin'] + '.txt'
                        os.rename(path_folder/f'Dataset/ETF/{filename}', path_folder/f'Dataset/ETF/{new_filename}')
                        print(f"PAST NAME : {filename}, NEW NAME: {new_filename}")
        print("--- %s seconds ---" % (time.time() - start_time))

def main():
    # geckodriver_autoinstaller.install()

    # 1. URLs pages with list of ETFs
    # request_save_list_etf_pages()

    # 2. Load list_etf pages
    list_etf_links = parse_pages('list_etf')

    # 3. Request each ETF and Download Data in folder ETF
    # dict_etf = request_download_etf(list_etf_links)
    # 4. Rename Files with (name_etf and ISIN code)
    # rename_etf_files()
    # 5. Load dict_etf
    with open('./Dataset/Pickles/dict_etf.pickle', 'rb') as handle:
        dict_etf = pickle.load(handle)

    print(dict_etf['HSBCEUROSTOXX50ETF'])

if __name__ == "__main__":
    main()
    