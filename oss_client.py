import os
import oss2
from urllib.parse import urlparse

class OSSClient:
    def __init__(self):
        access_key_id=os.getenv("OSS_ACCESS_KEY_ID",None)
        access_key_secret=os.getenv("OSS_ACCESS_KEY_SECRET",None)
        endpoint="oss-cn-beijing.aliyuncs.com"
        bucket_name="opencsg-craw-data"
        auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(auth, endpoint, bucket_name)


    def download(self, oss_uri, target_dir):
        """Download a file from OSS to a local directory."""
        bucket_name, key = parse_oss_uri(oss_uri)
        filename = os.path.basename(oss_uri)
        path = os.path.join(target_dir, filename)
        self.bucket.get_object_to_file(key, path)
        return path

    # def upload(self, file_path, oss_key):
    #     """Upload a file to OSS."""
    #     # print(f"Uploading {file_path} to {bucket} {key}")
    #     self.bucket.put_object_from_file(oss_key, file_path)

    def upload(self, oss_key,data):
        """Upload a file to OSS."""
        # print(f"Uploading {file_path} to {bucket} {key}")
        self.bucket.put_object(key=oss_key, data=data)

    def walk(self, prefix):
        """Walk through OSS prefix and yield objects."""
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix):
            yield {
                "key": obj.key,
                "size": obj.size,
                "last_modified": obj.last_modified,
                "etag": obj.etag,
                "storage_class": None,  # OSS没有 StorageClass 的概念
            }

def parse_oss_uri(uri: str):
    parsed_url = urlparse(uri)
    if parsed_url.scheme != "oss":
        raise ValueError(
            "Expecting 'oss' scheme, got: {} in {}.".format(parsed_url.scheme, uri)
        )
    return parsed_url.netloc, parsed_url.path.lstrip("/")

if __name__ == '__main__':
    # 初始化 OSS 客户端
    oss_client = OSSClient()

    # oss_client.upload("rawdata/data.txt")
    # 使用示例：
    # 下载文件
    # oss_uri = "oss://opencsg-craw-data/data.json"
    # local_dir = "./"
    # downloaded_file_path = oss_client.download(oss_uri, local_dir)
    # print(f"File downloaded to: {downloaded_file_path}")

    # 上传文件
    # local_file_path = "/path/to/local/file"
    # oss_key = "your-object-key"
    # oss_client.upload(local_file_path, oss_key)

    # # 遍历 OSS 对象
    # prefix = "/"
    # for obj in oss_client.walk(prefix):
    #     print(obj)