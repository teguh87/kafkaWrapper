import logging
import sys
import json
import base64
import signal
# import threading

from http import HTTPStatus

"""
    Implement the logic to connect to kafka and consume messages.
    kafkaWrapper is a wrapper around kafka-python KafkaConsumer.
    kafkaWrapper relies on it in order to consume messages from kafka.
    kafkaWrapper does not catch exceptions raised by kafka-python.
"""
class KafkaWrapper:
    def __init__(self, **kwargs):
        super(KafkaWrapper, self).__init__()
        self.consumer = kwargs.get('consumer')
        self.producer = kwargs.get('producer')

        self.message_handlers = {}

        logger = logging.getLogger(__name__)

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

        self.logger = logger

    """
        Docoretor for consumed all handler for kafka consumer
        :topic kafka topic
    """
    def consumed(self, topic):
        def decorator(f):
            self.__add_message_handlers(topic, f)
            return f
        return decorator

    def produced(self, topic):
        def decorator(f):
            def func_message(message):
                event = self.__run__producer_handler(topic, message)
                return f(event)
            return func_message
        return decorator

    def __add_message_handlers(self, topic, handler):
        if self.message_handlers.get(topic) is None:
            self.message_handlers[topic] = []
        self.message_handlers[topic].append(handler)

    """
        :topic kafka topics
        :message kafka content message
    """
    def __run__producer_handler(self, topic,  message):
        # produce asynchronously send message
        future = self.producer.send(topic,  self.__encode_message(message))
        try:
            record_metadata = future.get(timeout=10)
            message_return =  json.dumps({
                'topic' : record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'message': message,
                'status': HTTPStatus.OK
            })
            # self.logger.info('kafka response %s'%(message_return))
            return message_return
        except Exception as e:
            self.logger.critical(e)
            self.producer.close
       
        self.producer.flush() 
        return None

    def __run_consumed_handler(self, msg):
        try:
            handlers = self.message_handlers[msg.topic]
            for handler in handlers:
                handler(self.__decode_message(msg))
            self.consumer.commit()
        except Exception as e:
            self.logger.critical(e)
            # self.consumer.close()
            # sys.exit("error detection, system critical")

    def run(self):
        self.__consumer_event_driven()

    def signal_term_handler(self, signal, frame):
        self.logger.info("closing consumer")
        self.consumer.close()
        sys.exit(0)

    def __consumer_event_driven(self):
        self.consumer.subscribe(topics=tuple(self.message_handlers.keys()))
        self.consumer.poll(timeout_ms=100)
        self.logger.info("starting consumer...registered signterm")

        signal.signal(signal.SIGTERM, self.signal_term_handler)
        signal.signal(signal.SIGINT, self.signal_term_handler)
        signal.signal(signal.SIGQUIT, self.signal_term_handler)
        signal.signal(signal.SIGHUP, self.signal_term_handler)

        while True:
            for msg in self.consumer:
                self.__run_consumed_handler(msg)
    
    def __encode_message(self, message):
        _str_dict = str(message).encode('utf-8')
        return base64.b64encode(_str_dict)

    def __decode_message(self, msg):
        return eval(base64.b64decode(msg.value))