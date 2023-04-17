import time
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Callable, Dict
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lib.helper import configs, setup_logger

logger = setup_logger(' SCRAPER:PY ', 'lib/log.xml', level="INFO")
header = configs.header
timeout = configs.timeout


def get_request(url, proxies=None):
    try:
        response = requests.get(url, headers=header, timeout=timeout, proxies=proxies)  # TODO Proxy ayarları eklenecek
        response.raise_for_status()
        return response

    except requests.exceptions.HTTPError as errh:
        logger.error("HTTP Hatası: %s. Proxy: %s", errh, proxies)
        raise errh
    except requests.exceptions.ConnectionError as errc:
        logger.error("Bağlantı Hatası: %s.  Proxy: %s", errc, proxies)
        raise errc
    except requests.exceptions.Timeout as errt:
        logger.error("Zaman Aşımı Hatası: %s.  Proxy: %s", errt, proxies)
        raise errt
    except requests.exceptions.RequestException as err:
        logger.error("Bir Hata Oluştu: %s.  Proxy: %s", err, proxies)
        raise err


def amazon(url):
    response = get_request(url)
    try:

        runtime = datetime.now()
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.select_one('#productTitle').getText().strip()
        try:
            price = soup.select_one(
                '#corePriceDisplay_desktop_feature_div > div.a-section.a-spacing-none.aok-align-center > span > span:nth-child(2)').getText()
        except:
            price = soup.find(id="price").getText()

        ean = soup.find(id="ASIN").get("value")
        stock = False if soup.find('input', value="Add to Basket") == None else True

        results = {"Product ID": "", "Runtime DateTime": runtime, "Product Title": title, "Price": price,
                   "EAN": ean,
                   "Stock Availability": stock}

        return results
    except (AttributeError, TypeError, ValueError) as err:
        logger.error("Amazon BeautifulSoup Hatası: %s. URL: %s", err, url)
        raise
    except Exception as e:
        raise e


def argos(url):
    response = get_request(url)
    try:

        soup = BeautifulSoup(response.text, 'html.parser')
        runtime = datetime.now()
        title = soup.select_one(
            '#content > main > div:nth-child(2) > div.xs-block > div:nth-child(1) > section.xs-12--none.md-7.xl-8.pdp-core.bolt-v2 > div.Namestyles__ProductName-sc-269llv-0.kEQsqD.bolt-v2 > h1 > span:nth-child(1)'
        ).getText().strip()

        price = soup.select_one(
            '#content > main > div:nth-child(2) > div.xs-block > div:nth-child(1) > section.xs-12--none.md-5--none.xl-4--none.pdp-right > section > ul > li > h2'
        ).getText()
        ean = soup.select_one(
            '#content > main > div:nth-child(2) > div.xs-block > div:nth-child(1) > section.xs-12--none.md-7.xl-8.pdp-core.bolt-v2 > div.Namestyles__ProductName-sc-269llv-0.kEQsqD.bolt-v2 > h1 > span.Namestyles__CatNumber-sc-269llv-2.lbKeLk'
        ).getText()

        stock = False if soup.find('button', {"data-test": "add-to-trolley-button-button"}) == None else True

        results = {"Product ID": "", "Runtime DateTime": runtime, "Product Title": title, "Price": price,
                   "EAN": ean,
                   "Stock Availability": stock}

        return results
    except (AttributeError, TypeError, ValueError) as err:
        logger.error("Argos BeautifulSoup Hatası: %s. URL: %s", err, url)
        raise
    except Exception:
        pass


def bestwaywholesale(url):
    response = get_request(url)
    try:

        soup = BeautifulSoup(response.text, 'html.parser')
        runtime = datetime.now()
        title = soup.select_one(
            '#shop-products > div.productpagedetail-inner > div.right > h1'
        ).getText().strip()
        table = soup.find('table', {'class': 'prodtable'})
        price = ""
        ean = ""
        for tr in table.find_all('tr'):
            th = tr.find('th').text
            td = tr.find('td').text
            if ("RSP" in th):
                price = td
            if ("EAN" in th):
                ean = td

        stock = False if soup.find('span', {"class": "must"}) == None else True
        results = {"Product ID": "", "Runtime DateTime": runtime, "Product Title": title, "Price": price,
                   "EAN": ean, "Stock Availability": stock}

        return results
    except (AttributeError, TypeError, ValueError) as err:
        logger.error("Bestwaywholesale BeautifulSoup Hatası: %s. URL: %s", err, url)
        raise
    except Exception:
        raise


def read_data():
    # Son bir saat içindeki okunmamış dataları DataFrame olarak döndürür.
    try:
        df = pd.read_csv(configs.input_file)
    except Exception as e:
        logger.error("İnput file okuma hatası ERR: {}".format(e))
        raise e
    else:
        df.dropna(subset=['Products'], inplace=True)
        try:
            df['LastRead'] = pd.to_datetime(df['LastRead'])  # TODO Dt atama işlemi gereksiz olabilir bak!
            df['LastScrape'] = pd.to_datetime(df['LastScrape'])
        except Exception as e:
            logger.error("İnput file sütun isimleri hatalı ERR: {}".format(e))
            raise e
        return df


scrapers: Dict[str, Callable[[str], dict]] = {
    "www.amazon.co.uk": amazon,
    "www.argos.co.uk": argos,
    "www.bestwaywholesale.co.uk": bestwaywholesale

}


def run_scraper():
    try:
        df = read_data()
        last_read = datetime.now() - timedelta(minutes=configs.run_period)
        selected_product_df = df[df['LastRead'] < last_read][:configs.max_product]
        selected_product_df['LastRead'] = datetime.now()

        scraped_product_list = []

        if selected_product_df is not None:
            for index, row in selected_product_df.iterrows():

                for url in row['Products'].split(';'):  # TODO Loglama ve hata handle
                    try:
                        scraper = scrapers.get(urlparse(url).netloc)
                        print(url)

                        result = scraper(url)
                        if result is not None:
                            dict_data = result
                            dict_data['Product ID'] = str(index)
                            dict_data['Runtime DateTime'] = str(datetime.now())
                            scraped_product_list.append(dict_data)

                            time.sleep(configs.frequency)
                    except TypeError:
                        logger.error("Scraper Bulunamadı : {} ".format(url))
                    except Exception as e:
                        # logger.error(e)
                        pass
                selected_product_df.at[
                    index, 'LastScrape'] = datetime.now()  # TODO işlem başarıyla tamamlanınca değişmeli
            df.update(selected_product_df)
            df.to_csv(configs.input_file, header=True, mode='w', index=False)

            output_data = pd.DataFrame(scraped_product_list,
                                       columns=["Product ID", "Runtime DateTime", "Product Title", "Price",
                                                "EAN", "Stock Availability"])
            output_data.to_csv(configs.output_file, header=False, mode='a', index=False)

    except Exception as e:
        logger.error(e)
        # raise e


if __name__ == "__main__":
    while True:
        run_scraper()
        print("Scraper çalışıyor : ", datetime.now())
        time.sleep(5)
