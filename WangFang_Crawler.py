import concurrent.futures
import os

import redis

from oss_client import OSSClient
import requests
from rbq import RBQ_Client
from settings import headers,cookies
from urllib.parse import  urlparse,parse_qs
import json
# 获得爬取的链接后，检查redis中的记录是否爬取过了
from concurrent.futures.thread import ThreadPoolExecutor
import traceback

class WanFang:

    def __init__(self):
        self.redis_client=redis.StrictRedis(
            host="61.147.247.138",
            password=os.getenv("RBQ_PASS"),
            port=32012
        )
        self.rbq=RBQ_Client("wanfang_qikan")
        os.makedirs("./save",exist_ok=True)



    def get_download_url(self,domain_name="oss-wanfangdata-com-cn-443.webvpn.cams.tsgvip.top"):
        domain_name="c-wanfangdata-com-cn-443.webvpn.cams.tsgvip.top"
        # return f"http://oss-wanfangdata-com-cn-443.webvpn.cams.tsgvip.top/file/download/perio_{resourceid}.aspx"
        url_download = f"https://{domain_name}/NewFulltext"
        return url_download
    def check_exists(self,resourceId):
        res=self.redis_client.sismember(
            name=self.get_downloaded_key(),
            value=resourceId
        ) == 1
        return res
    def get_url(self,resourceid):
        return f"http://oss-wanfangdata-com-cn-443.webvpn.cams.tsgvip.top/file/download/perio_{resourceid}.aspx"

    def get(self,url):
        file_name = os.path.basename(url)[6:-5]
        # url = "http://oss.wf.g.yyttgd.top/file/download/perio_hwyhmb202402001.aspx"
        try:
            response = requests.get(url, headers=headers, cookies=cookies, verify=False, allow_redirects=False)
            if response.status_code == 404:
                return False
        except:
            print(traceback.format_exc())
            return

        pdf_url = response.headers.get("Location", None)
        # print(pdf_url)
        if pdf_url is not None:
            response = requests.get(pdf_url, headers=headers, cookies=cookies, allow_redirects=False)
            if response.status_code == 404:
                return False
            pdf_url = response.headers.get("Location", None)
            url_ = urlparse(pdf_url)
            params = parse_qs(url_.query)
            params = {
                "type": "perio",
                "resourceId": file_name,
                "transaction": params["transaction"],
            }
            # return json.dumps(params)
            response=requests.get(self.get_download_url(),params=params,headers=headers,cookies=cookies)
            if response.status_code==404:
                return False
            with open(f"./save/{file_name}.pdf","wb") as f:
                f.write(response.content)
            self.record_downloaded_item(file_name)
            print(file_name,"done")
            return True
        else:
            print("Cookie失效了")
            exit()

    def get_downloaded_key(self):
        return f"wanfang:downloaded"

    def record_downloaded_item(self,resourceId):
        self.redis_client.sadd("wanfang:downloaded",resourceId)
    def process_message(self,msg):
        msg = msg.decode()[1:-1]
        for year in range(2024, 1980, -1):
            for month in range(1, 13):
                for day in range(1, 32):
                    the_date = f"{year}{month:02}{day:03}"
                    resourceid = f"{msg}{the_date}"
                    if self.check_exists(resourceid):
                        print(resourceid,"跳过")
                        continue
                    the_url = self.get_url(resourceid)
                    result = self.get(the_url)
                    if not result:
                        if month==1 and day==1:
                            return
                        break
    def download(self):

        while True:
            method,msg=self.rbq.get_message()
            self.process_message(msg)
            self.rbq.complete(method)

    def run(self):
        ls=[]
        max_workers=2
        pool=ThreadPoolExecutor(max_workers=max_workers)

        for x in range(max_workers):
            ls.append(pool.submit(self.download))

        concurrent.futures.wait(ls)


if __name__ == '__main__':
    ins=WanFang()
    ins.download()