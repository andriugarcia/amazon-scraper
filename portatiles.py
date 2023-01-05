import requests  # required for HTTP requests: pip install requests
from bs4 import BeautifulSoup  # required for HTML and XML parsing                                                              # required for HTML and XML parsing: pip install beautifulsoup4
import pandas as pd  # required for getting the data in dataframes : pip install pandas
import time  # to time the requests
from multiprocessing import Process, Queue, Pool, Manager
import threading
import sys
import re
from lxml.html import fromstring
from itertools import cycle
import traceback

# productsreduced
# C, D, F. G, H
# A, E

# Camisetas
# D


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([i.xpath('.//td[1]/text()')[0],
                              i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies


# link = "https://www.amazon.es/s?rh=n%3A2846220031%2Cn%3A%212846221031%2Cn%3A3074028031%2Cn%3A3074055031%2Cn%3A3074256031"
# link = "https://www.amazon.es/b/?node=3074256031&ref_=Oct_s9_apbd_odnav_hd_bw_b3M2J7H&pf_rd_r=FDX0G4PR11457AFVBBQJ&pf_rd_p=89d7e593-8b4c-5596-8547-3c12fceab0ad&pf_rd_s=merchandised-search-11&pf_rd_t=BROWSE&pf_rd_i=3074028031"
# link = 'https://www.amazon.es/s?i=apparel&bbn=2846220031&rh=n%3A13901096031'
no_pages = 8  # no of pages to scrape in the website (provide it via arguments)

findingElement = 'div'
findingClass = 'sg-col-4-of-24 sg-col-4-of-12 sg-col-4-of-36 s-result-item s-asin sg-col-4-of-28 sg-col-4-of-16 sg-col sg-col-4-of-20 sg-col-4-of-32'

findingName = 'a-size-base-plus a-color-base a-text-normal'
findingBrand = 'a-size-base-plus a-color-base'
findingPrice = 'a-offscreen'
findingRating = 'a-icon-alt'
findingImage = 's-image'
findingUrl = 'a-link-normal a-text-normal'

# proxies = { # define the proxies which you want to use
#   'http': 'http://167.172.191.249:39193',
#   'https': 'http://195.22.121.13:443',
# }
# proxies = get_proxies()
# proxy_pool = cycle(proxies)


def get_data(pageNo, q, link):
    # proxy = next(proxy_pool)
    # proxies={"http": proxy, "https": proxy}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64;     x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding": "gzip, deflate",
               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1", "Connection": "close", "Upgrade-Insecure-Requests": "1"}
    r = requests.get(link+'&page='+str(pageNo), headers=headers)
    content = r.content
    soup = BeautifulSoup(content)
    for d in soup.findAll(findingElement, attrs={'class': findingClass}):
        name = d.find('span', attrs={'class': findingName})
        brand = d.find('span', attrs={'class': findingBrand})
        price = d.find('span', attrs={'class': findingPrice})
        rating = d.find('span', attrs={'class': findingRating})
        image = d.find('img', attrs={'class': findingImage})
        url = d.find('a', attrs={'class': findingUrl})
        # print(name.text, brand.text, price.text, rating.text)

        all = []
        all.append(re.findall(r"(?<=dp/)[A-Z0-9]{10}", url['href'])[0])
        if name is not None:
            all.append(name.text)
        else:
            all.append("unknown-product")
        if price is not None:
            all.append(price.text)
        else:
            all.append('0€')
        if brand is not None:
            all.append(brand.text)
        else:
            all.append('unknown-brand')
        if rating is not None:
            # print(rating.text)
            all.append(rating.text)
        else:
            all.append('-1')
        if image is not None:
            all.append(image['src'])
        else:
            all.append('')
            all.append('')
        if url is not None:
            all.append(url['href'])
        else:
            all.append('')
        q.put(all)
    print("---------------------------------------------------------------")
    results = []


if __name__ == "__main__":
    links = [
        # 'https://www.amazon.es/s?rh=n%3A2846220031%2Cn%3A%212846221031%2Cn%3A3074028031%2Cn%3A3074052031%2Cn%3A3074245031'
        'https://www.amazon.es/s?i=computers&bbn=938008031&rh=n%3A667049031%2Cn%3A667050031%2Cn%3A938008031%2Cp_n_style_browse-bin%3A949809031%2Cp_n_feature_fifteen_browse-bin%3A21761322031%7C8178954031%7C8178957031%2Cp_n_feature_browse-bin%3A1482669031%7C1482670031%7C1482671031%2Cp_n_pattern_browse-bin%3A5123245031%7C5123246031%7C949814031%2Cp_n_feature_seven_browse-bin%3A8179048031%2Cp_n_feature_five_browse-bin%3A7457506031&dc&fst=as%3Aoff&qid=1604658814&rnid=831271031&ref=sr_nr_p_72_1'
    ]
    for step in range(1, 50):
        linkIndex = 0
        while linkIndex < len(links):
            m = Manager()
            q = m.Queue()
            p = {}
            startTime = time.time()
            qcount = 0  # the count in queue used to track the elements in queue
            asins = []  # List to store asin of the product
            products = []  # List to store name of the product
            brands = []  # Brands
            prices = []  # List to store price of the product
            ratings = []  # List to store ratings of the product
            images = []  # Images
            urls = []  # URLS
            for i in range(1, no_pages):
                print("starting thread: ", i)
                p[i] = threading.Thread(target=get_data, args=(
                    i+step*8, q, links[linkIndex]))
                p[i].start()
            for i in range(1, no_pages):  # join all the threads/processes
                p[i].join()
            while q.empty() is not True:
                qcount = qcount+1
                queue_top = q.get()
                asins.append(queue_top[0])
                products.append(queue_top[1])
                prices.append(queue_top[2])
                brands.append(queue_top[3])
                ratings.append(queue_top[4])
                images.append(queue_top[5])
                srcsets.append(queue_top[6])
                urls.append(queue_top[7])
            print("total time taken: ", str(
                time.time()-startTime), " qcount: ", qcount)
            print([len(products), len(brands), len(prices),
                   len(ratings), len(images), len(urls)])
            # print([len(products), len(prices)])
            df = pd.DataFrame({'asin': asins, 'name': products, 'Price': prices,
                               'ratings': ratings, 'image': images, 'url': urls})
            # df = pd.DataFrame({'Product Name':prices})
            df.to_csv('portatilesGaming.csv', mode='a',
                      index=False, header=False, encoding='utf-8')
