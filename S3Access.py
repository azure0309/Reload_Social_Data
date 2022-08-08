# -*- coding: utf-8 -*-
import os
import traceback
import boto3
from boto3.session import Session
from Logger import Logger


# ダウンロード関連のユーティリティクラス
class S3Access(object):
    _instance = None
    BUCKET_NAME = os.environ['ENV_S3Accesss_Bucket']

    # 唯一のインスタンスを生成するコンストラクタ
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    # コンストラクタ
    def __init__(self):
        self.logger = Logger()


    # put 処理
    #
    # path_local コピー元のローカルパス
    # path_s3    コピー先のバケット以下のパス
    def put(self, path_local, path_s3):
        try:
            s3_session = Session()
            s3 = s3_session.resource('s3')
            o_bucket = s3.Bucket(self.BUCKET_NAME)
            o_bucket.upload_file(path_local, path_s3)
        except Exception as e:
            self.logger.write_error('S3Access#put 01 Error localpath:{} s3path:{}'.format(path_local, path_s3))
            self.logger.write_error(traceback.format_exc())
            return False
        self.logger.write_msg('S3Access#put 02 localpath:{} s3path:{}'.format(path_local, path_s3))
        return True

    # get 処理
    #
    # path_s3    コピー元のバケット以下のパス
    # path_local コピー先のローカルパス
    def get(self, path_s3, path_local):
        try:
            s3_session = Session()
            s3 = s3_session.resource('s3')
            o_bucket = s3.Bucket(self.BUCKET_NAME)
            o_bucket.download_file(path_s3, path_local)
        except Exception as e:
            self.logger.write_error('S3Access#get 01 Error localpath:{} s3path:{}'.format(path_local, path_s3))
            self.logger.write_error(traceback.format_exc())
            return False
        self.logger.write_msg('S3Access#get 02 localpath:{} s3path:{}'.format(path_local, path_s3))
        return True


    # ディレクトリの一覧取得処理
    #
    # prefix  ディレクトリ指定
    # 返値 ディレクトリ内のファイルリスト
    def list(self,prefix=""):
        try:
            s3_session = Session()
            s3 = s3_session.resource('s3')
            o_bucket = s3.Bucket(self.BUCKET_NAME)
            return o_bucket.objects.filter(Prefix=prefix)
        except Exception as e:
            self.logger.write_error('S3Access#list 01 Error prefix:{}'.format(prefix))
            self.logger.write_error(traceback.format_exc())
            return False
        self.logger.write_msg('S3Access#list 02 Error prefix:{}'.format(prefix))
        return True


