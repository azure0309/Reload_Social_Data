# -*- coding: utf-8 -*-
from pytz import timezone
from datetime import datetime
import dateutil.parser


# 日付関連のユーティリティクラス
class DateUtil(object):
    _instance = None
    TZ      = timezone('Asia/Tokyo')

    # 唯一のインスタンスを生成するコンストラクタ
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance


    # 現在の日時を返します
    def getNow(self):
        time = datetime.now(DateUtil.TZ)
        return time


    # フォーマット変換
    #
    # time パース対象の文字列 '%Y-%m-%d %H:%M:%S' の書式
    # type 1: '%Y-%m-%d %H:%M:%S' のフォーマット
    # type 2: '%Y-%m-%d' のフォーマット
    # 解析結果の datetime
    def toFormat(self, time, type=1):
        try:
            if type == 1:
                return dateutil.parser.parse(time).strftime('%Y-%m-%d %H:%M:%S')
            elif type == 2:
                return dateutil.parser.parse(time).strftime('%Y-%m-%d')
        except Exception as e:
            return ""


    # フォーマット変換
    #
    # datestr パース対象の文字列 '%Y年%m月%d日' の書式
    # type 1: '%Y年%m月%d日' のフォーマット
    # type 2: 'YYYYMMDD' のフォーマット
    # type 3: 'YYYY-MM-DD' のフォーマット
    # 解析結果の datetime YYYY-MM-DD の書式
    def parse_date(self, datestr, type=1):
        try:
            if type == 1:
                dt = datetime.strptime(datestr, '%Y年%m月%d日')
            elif type == 2:
                dt = datetime.strptime(datestr, '%Y%m%d')
            else:
                dt = datetime.strptime(datestr, '%Y-%m-%d')
            dt2 = "{:0=4}-{:0=2}-{:0=2}".format(dt.year, dt.month, dt.day)
            return dt2
        except Exception as e:
            return ""

