import requests
import psycopg2

lastFetchedIndex = -1


def getProxy(proxyEnable=True):
    global lastFetchedIndex
    if not proxyEnable:
        return None
    conn = psycopg2.connect(host="10.10.10.227", database="crawling_db", user="enrgtech", password="Enrgtech@50")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT protocol, ip_address FROM ip_addresses")
            proxies = cur.fetchall()
            if lastFetchedIndex + 1 >= len(proxies):
                lastFetchedIndex = -1
            lastFetchedIndex += 1
            proxy = proxies[lastFetchedIndex]
            return {
                "http": "{}://{}".format(proxy[0], proxy[1]),
            }
    finally:
        conn.close()


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Mode': 'navigate',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

# response = requests.get('http://httpbin.org/ip', headers=HEADERS, proxies=proxies)
# response = requests.get('https://uk.farnell.com/3m/3m-1350f-1-0-25-x-72yd-black/tape-65-83m-x-6-35mm-pet-film/dp/2820165', headers=HEADERS, proxies=proxies)
for i in range(1, 101):
    try:
        proxies = getProxy()
        print(proxies)
        response = requests.get('http://www.digikey.com/en/products/detail/jae-electronics/MM60-EZH039-B5-R850/2071036', headers=HEADERS, proxies=proxies)
        print(response)
    except:
        print('er')
