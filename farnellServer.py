from logging.handlers import QueueHandler, QueueListener
from flask import Flask, request, jsonify, Response
from multiprocessing import Process, Queue
from user_agents import USER_AGENTS
from urllib.parse import urlparse
import psycopg2
import requests
import logging
import json


def loggingQueue():
    log_queue = Queue()
    file_handler = logging.FileHandler('Logs.log', mode='w')
    formatter = logging.Formatter("%(levelname)s [%(asctime)s] - %(message)s", "%d-%m-%Y %H:%M:%S")
    file_handler.setFormatter(formatter)

    queue_listener = QueueListener(log_queue, file_handler)
    queue_listener.start()
    return log_queue


class Server:
    def __init__(self, name, port, identifier, log_queue):
        self.app = Flask(name)
        self.port = port
        self.identifier = identifier
        self.session = requests.Session()

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(QueueHandler(log_queue))
        self.app.logger = logger

        self.lastFetchedIndex = -1
        self.indexNum = 0
        self.defaultUserAgents = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self.app.add_url_rule('/', 'handleRequest', self.handleRequest, methods=['GET'])

    def getProxy(self, proxyEnable=True):
        if not proxyEnable:
            return None
        conn = psycopg2.connect(host="10.10.10.227", database="crawling_db", user="enrgtech", password="Enrgtech@50")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT protocol, ip_address FROM ip_addresses")
                proxies = cur.fetchall()
                if self.lastFetchedIndex + 1 >= len(proxies):
                    self.lastFetchedIndex = -1
                self.lastFetchedIndex += 1
                proxy = proxies[self.lastFetchedIndex]
                self.app.logger.info(f'Using proxy: {proxy[0]}://{proxy[1]}')
                return {"http": "{}://{}".format(proxy[0], proxy[1])}
        finally:
            conn.close()

    def run(self):
        self.app.logger.info(f'Initialized {self.identifier} on port {self.port}')
        self.app.run(debug=True, host='0.0.0.0', port=self.port, use_reloader=False)

    @staticmethod
    def checkUrl(url):
        try:
            result = urlparse(url)
            if all([result.scheme, result.netloc]):
                return True, url
            return False, None
        except Exception as e:
            return False, e

    def handleRequest(self):
        url = request.args.get('url')
        if not url:
            response = Response(response=json.dumps({'URL': 'N/A', "StatusCode": 500, "ErrorMessage": 'Url Not Provided'}), mimetype='application/json')
            response.headers['responseStatus'] = '500'
            self.app.logger.error(f'Error occured on {self.identifier} Url is not provided')
            return response

        try:
            checkUrl = self.checkUrl(url)
            if checkUrl[0]:
                targetedUrl = checkUrl[1]
                data = self.visitUrl(targetedUrl)
                flaskResponse = Response(data.content)
                flaskResponse.headers['responseStatus'] = str(data.status_code)
                self.app.logger.info(f'Handled Request On {self.identifier} for ({targetedUrl}) Status: {data.status_code}')
                return flaskResponse
            errorResponse = Response(json.dumps({'URL': url, "StatusCode": 500, "ErrorMessage": "Invalid Url Format"}), mimetype='application/json')
            errorResponse.headers['responseStatus'] = str(500)
            self.app.logger.error(f'Error occured On {self.identifier} Invalid Url Format')
            return errorResponse
        except Exception as e:
            self.app.logger.error(f'Error Handling Request On {self.identifier}: {e}')
            return jsonify({'error': str(e)}), 500

    def visitUrl(self, targetUrl):
        headers = self.getUserAgent()[0]
        # proxies = getProxy(True)
        # targetUrl = targetUrl.replace('https://', 'http://')
        response = self.session.get(url=targetUrl, headers=headers, proxies=None, timeout=5)
        self.session.close()
        return response

    def setHeaders(self, userAgent=None):
        userAgent = userAgent if userAgent else self.defaultUserAgents
        HEADERS = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Fetch-Mode': 'navigate',
            'User-Agent': userAgent,
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        return HEADERS

    def getUserAgent(self, rotateEnable=False):
        isLastValue = False
        if rotateEnable is True:
            if self.indexNum >= len(USER_AGENTS):
                self.indexNum = 0
            userAgent = USER_AGENTS[self.indexNum]
            self.indexNum += 1
            if self.indexNum == len(USER_AGENTS):
                isLastValue = True
        else:
            userAgent = self.defaultUserAgents
        return self.setHeaders(userAgent), isLastValue


def runServer(name=__name__, startPort=8000, instanceNumber=1, logQ=None):
    identifier = f"S-{instanceNumber}"
    port = startPort + instanceNumber
    server = Server(name, port, identifier, logQ)
    server.run()


def runServers(instances):
    processes = []
    logQueue = loggingQueue()
    for i in range(instances):
        process = Process(target=runServer, args=(__name__, 8000, i + 1, logQueue))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()


if __name__ == '__main__':
    numInstances = 10
    runServers(numInstances)
