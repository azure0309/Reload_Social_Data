# -*- coding: utf-8 -*-
import os
from os import path
import logging
from datetime import datetime
import watchtower
from distutils.util import strtobool

# ログ出力クラス
class Logger:
    _instance = None

    # 唯一のインスタンスを生成するコンストラクタ
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    # コンストラクタ
    def __init__(self):
        logger = logging.getLogger()

        os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'
        self.process_name=os.environ['ENV_Logger_LogStreamName_prefix']
        self.log_date_str = datetime.today().strftime('%Y%m%d')
        self.write_to_cloudwatch = strtobool(os.environ['ENV_Logger_CloudWatch_enable'])
        if self.write_to_cloudwatch and not logger.hasHandlers():
            self._logger_init()

    # 初期化処理
    def _logger_init(self):
        if not path.exists("/tmp/logs"):
            os.makedirs("/tmp/logs")
        log_filename = '/tmp/logs/{0}_{1}.log'.format(self.process_name, self.log_date_str)

        logging.basicConfig(
            filename=log_filename,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=logging.INFO
        )

        logger = logging.getLogger()

        self.handler = watchtower.CloudWatchLogHandler(
            log_group=os.environ['ENV_Logger_LogGroupName'],
            stream_name="{0}_{1}".format(self.process_name, self.log_date_str)
        )
        logger.addHandler(self.handler)
        return log_filename

    # INFOレベルでログ出力を行います
    def write_msg(self,*msg):
        print(msg)
        if self.write_to_cloudwatch:
            logger = logging.getLogger()
            logger.info(msg)

    # WARNINGレベルでログ出力を行います
    def write_warning(self,*msg):
        print(msg)
        if self.write_to_cloudwatch:
            logger = logging.getLogger()
            logger.warning(msg)

    # ERRORレベルでログ出力を行います
    def write_error(self,*msg):
        print(msg)
        if self.write_to_cloudwatch:
            logger = logging.getLogger()
            logger.error(msg)
