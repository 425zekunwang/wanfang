import pika
import pika.credentials
import os
import tqdm
import json

class RBQ_Client:

    def __init__(self, queue_name) -> None:
        self.queue_name = queue_name
        credentials = pika.PlainCredentials(username=os.getenv("RBQ_USER"),
                                            password=os.getenv("RBQ_PASS"))
        self.rbq = pika.ConnectionParameters(host="61.147.247.138", port=32010, virtual_host="/", credentials=credentials,
                                             heartbeat=0
                                             )
        self.connection = pika.BlockingConnection(self.rbq)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.queue_declare(queue=self.queue_name,durable=True)

    def publish_message(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print("Message published successfully",message)

    def consume(self, callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def complete(self,method):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def get_message(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=False)
        if method_frame:
            return method_frame,body
        else:
            return None,None

    def close_connection(self):
        self.connection.close()


# 使用示例
if __name__ == '__main__':
    rbq_client = RBQ_Client("wanfang_patent")