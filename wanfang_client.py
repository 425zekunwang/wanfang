import json
import os

import redis
import requests
from demo import get
from import_rbq import RBQ_Client
from concurrent.futures import ThreadPoolExecutor, as_completed



class wanfang:
    def __init__(self):
        self.rbq_client = RBQ_Client("wanfang_qikan")
        self.redis_client=redis.StrictRedis(
            host="61.147.247.138",
            port=32012,
            password="wzkatopencsg"
        )
        self.save_dir = "./save"

    def get_url(self,resourceid):
        return f"http://oss-wanfangdata-com-cn-443.webvpn.cams.tsgvip.top/file/download/perio_{resourceid}.aspx"

    def save_params(self,params):
        # rbq_client_task.publish_message(json.dumps(params))
        self.redis_client.sadd("wanfang:download",json.dumps(params,ensure_ascii=False))
        # with open("record_bak.txt", "a") as f:
        #     f.write(params)
        #     f.write("\n")

    def process_message(self,msg):
        msg = msg.decode()[1:-1]
        print(msg)
        for year in range(2024, 1980, -1):
            for month in range(1, 13):
                for day in range(1, 32):
                    the_date = f"{year}{month:02}{day:03}"
                    resourceid = f"{msg}{the_date}"
                    the_url = self.get_url(resourceid)
                    result = get(the_url)
                    if not result:
                        break
                    print(resourceid,"done")
                    self.save_params(result)

    def main(self):
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            while True:
                method_frame, msg = self.rbq_client.get_message()
                if msg is None:
                    print("没有消息可处理了")
                    break
                # process_message(msg)
                future = executor.submit(self.process_message, msg)
                futures.append((future, method_frame))
                if len(futures) == 8:
                    for future, method_frame in as_completed(futures):
                        try:
                            future.result()
                            self.rbq_client.complete(method_frame)
                        except Exception as e:
                            print(f"Error processing message: {e}")
            for future, method_frame in as_completed(futures):
                try:
                    future.result()
                    self.rbq_client.complete(method_frame)
                except Exception as e:
                    print(f"Error processing message: {e}")

if __name__ == "__main__":
    wf=wanfang()
    wf.main()