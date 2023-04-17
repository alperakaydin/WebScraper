import logging
import logging.handlers
import yaml
import xml.etree.ElementTree as ET


def setup_logger(logger_name, log_file, level=logging.DEBUG):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime) s :%(name) s : %(message)s')
    xmlHandler = logging.handlers.RotatingFileHandler(log_file, mode='a', maxBytes=5 * 1024 * 1024, backupCount=2)
    xmlHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(xmlHandler)

    return l


logger = setup_logger('my_logger', 'lib/log.xml', level="INFO")


class Config():

    def __init__(self):
        tree = ET.parse('/Users/alper/PycharmProjects/WebScraper/config.xml')
        root = tree.getroot()

        self.max_product = int(root.find('limits/max_product').text)
        self.timeout = int(root.find('request_settings/timeout').text)
        self.frequency = int(root.find('limits/frequency').text)
        self.run_period = int(root.find('limits/run_period').text)
        self.input_file = (root.find('file_name/input_file').text)
        self.output_file = (root.find('file_name/output_file').text)

        header_dict = {}
        for e in root.findall('request_settings/header/*'):
            header_dict[e.tag] = e.text
        self.header = header_dict




configs = Config()
