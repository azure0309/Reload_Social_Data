# -*- coding: utf-8 -*-
import sqlalchemy as db
from sqlalchemy import MetaData, Table, Column, Integer, String, types

from SecretManager import SecretManager
import pandas as pd
import os


# DB管理クラス
class DBConnect(object):
    _instance = None

    # 唯一のインスタンスを生成するコンストラクタ
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    # コンストラクタ
    def __init__(self):
        os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'
        try:
            SECRETS = SecretManager().getSecret(os.environ['ENV_DBConnect_SECRET'])
            self.db_hostname = SECRETS['host']
            self.db_name = SECRETS['dbname']
            self.db_port = SECRETS['port']
            self.db_username = SECRETS['username']
            self.db_password = SECRETS['password']
        except KeyError as err:
            self.db_hostname = 'dentsu-casting-stg-211113.c7yvenrstrjg.ap-northeast-1.rds.amazonaws.com'
            self.db_name = 'dentsu_casting_stg'
            self.db_port = 3306
            self.db_username = 'dc_stg'
            self.db_password = 'dD34dmQMOycbQuyzl7bF'
        CONN_STR = 'mysql+pymysql://{2}:{3}@{0}:{4}/{1}'.format(
            self.db_hostname,
            self.db_name,
            self.db_username,
            self.db_password,
            self.db_port
        )
        self.engine = db.create_engine(CONN_STR)

    # データベース名を返します
    def getDBName(self):
        return self.db_name

    # engine オブジェクトを返す
    def getEngine(self):
        return self.engine

    # クエリ実行
    def executeQuery(self, sql):
        connection = self.engine.connect()
        ret = connection.execute(sql)
        result = ret.fetchone()
        return result

    # Dataframe をテーブルに反映します
    #
    # dataframe_to_table(df, 'tablename', ['id', 'talent_id'])  -- for example
    def dataframe_to_table(self, datas, table_name, database_name=None, index=None, if_exists='append'):
        def sqlcol(df):
            dtypedict = {}
            for i, j in zip(df.columns, df.dtypes):
                if "object" in str(j):
                    dtypedict.update({i: types.NVARCHAR(length=255)})
            return dtypedict

        with self.engine.connect() as connection:
            df = pd.DataFrame(datas)
            outputdict = sqlcol(df)
            if index is not None:
                df.set_index(index, inplace=True)
                df.to_sql(table_name, con=self.engine, if_exists=if_exists, dtype=outputdict, schema=database_name)
            else:
                df.to_sql(table_name, con=self.engine, if_exists='append', dtype=outputdict, index=False,
                          schema=database_name)
