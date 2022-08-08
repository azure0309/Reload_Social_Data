# -*- coding: utf-8 -*-
from boto3.session import Session
from botocore.exceptions import ClientError
import ast
import os

# Secret Manager アクセス管理クラス
class SecretManager(object):
    _instance = None

    # 唯一のインスタンスを生成するコンストラクタ
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    # コンストラクタ
    def __init__(self):
        session = Session()
        self.client = session.client(
            service_name = 'secretsmanager',
        )

    # シークレット情報の取得
    #
    # secret_name 取得するシークレット名
    # 返り値：シークレット値
    def getSecret(self,secret_name):
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + secret_name + " was not found")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", e)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", e)
            print(e)
        else:
            # Secrets Manager decrypts the secret value using the associated KMS CMK
            # Depending on whether the secret was a string or binary, only one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                text_secret_data = ast.literal_eval(get_secret_value_response['SecretString'])

            return text_secret_data
