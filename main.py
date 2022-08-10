import os
# from S3Access import S3Access
from Logger import Logger
from DBConnect import DBConnect
from DateUtil import DateUtil
import pandas as pd
import sqlalchemy as db
from sqlalchemy import desc, MetaData, Table, Column, Integer, String, DateTime, text
import traceback


class Reload_Social_Datas():
    CONV_COLUMN_MAP = {
        'タレントコード': 'talent_code',
        'influencer_name': 'talent_name_org',
        '性別': 'gender',
        '生年月日': 'birthday',
        '職業ジャンル': 'occupation_genre',
        '出身地': 'birth_place',
        '趣味特技': 'hobbies_skills',
        '更新日': "update_date",
        'social_subscriber_count': 'youtube_subscriber_count',
        'social_male': 'youtube_male_ratio',
        'social_female': 'youtube_female_ratio',
        'social_age13_17': 'youtube_age13_17',
        'social_age18_24': 'youtube_age18_24',
        'social_age25_34': 'youtube_age25_34',
        'social_age35_44': 'youtube_age35_44',
        'social_age45_54': 'youtube_age45_54',
        'social_age55_64': 'youtube_age55_64',
        'social_age65_': 'youtube_age65_',
        'social_followers': 'twitter_follower_count',
        'social_followed_by': 'instagram_follower_count',
        'social_posts_count': 'instagram_posts_count',
        '知名度': 'popularity_score'
    }

    # gender カラム変換マップ
    CONV_GENDER_MAP = {
        '': -1,
        'None': -1,
        '故人': 0,
        'キャラクター': 1,
        '男性': 2,
        '女性': 3,
        '男性グループ': 4,
        '女性グループ': 5,
        '男女グループ': 6
    }

    # 職業カラム変換マップ
    CONV_OCCUPATION_MAP = {
        'タレント／俳優': 1,
        'タレント／バラエティ': 2,
        'タレント／モデル': 3,
        'タレント／グラビアモデル': 4,
        'タレント／お笑い': 5,
        'タレント／キャスター（アナウンサー）': 6,
        'タレント／落語': 7,
        'タレント／歌舞伎俳優': 8,
        'タレント／その他古典芸能': 9,
        'タレント／声優': 10,
        'タレント／ダンサー': 11,
        'タレント／その他': 12,
        '音楽家／ミュージシャン': 13,
        '音楽家／クラシック': 14,
        '音楽家／ジャズ': 15,
        '音楽家／作詞作曲家': 16,
        '音楽家／その他': 17,
        'インフルエンサー／Youtuber': 18,
        'インフルエンサー／Instagram': 19,
        'インフルエンサー／Twitter': 20,
        'インフルエンサー／TikTok': 21,
        'インフルエンサー／Facebook': 22,
        'インフルエンサー／その他': 23,
        'アスリート／野球': 24,
        'アスリート／サッカー': 25,
        'アスリート／テニス': 26,
        'アスリート／ゴルフ': 27,
        'アスリート／バスケットボール': 28,
        'アスリート／バレーボール': 29,
        'アスリート／ラグビー': 30,
        'アスリート／卓球': 31,
        'アスリート／バドミントン': 32,
        'アスリート／その他球技': 33,
        'アスリート／陸上競技': 34,
        'アスリート／水泳': 35,
        'アスリート／体操': 36,
        'アスリート／柔道': 37,
        'アスリート／プロレス': 38,
        'アスリート／レスリング': 39,
        'アスリート／ボクシング': 40,
        'アスリート／相撲': 41,
        'アスリート／その他格闘技': 42,
        'アスリート／アイススケート': 43,
        'アスリート／スキー競技': 44,
        'アスリート／スノボ競技': 45,
        'アスリート／サーフィン': 46,
        'アスリート／Xスポーツ（クライミング・BMX・スケボーなど）': 47,
        'アスリート／モータースポーツ': 48,
        'アスリート／公営競技': 49,
        'アスリート／eスポーツ': 50,
        'アスリート／その他': 51,
        '元アスリート／野球': 52,
        '元アスリート／サッカー': 53,
        '元アスリート／テニス': 54,
        '元アスリート／ゴルフ': 55,
        '元アスリート／バスケットボール': 56,
        '元アスリート／バレーボール': 57,
        '元アスリート／ラグビー': 58,
        '元アスリート／卓球': 59,
        '元アスリート／バドミントン': 60,
        '元アスリート／その他球技': 61,
        '元アスリート／陸上競技': 62,
        '元アスリート／水泳': 63,
        '元アスリート／体操': 64,
        '元アスリート／柔道': 65,
        '元アスリート／プロレス': 66,
        '元アスリート／レスリング': 67,
        '元アスリート／ボクシング': 68,
        '元アスリート／相撲': 69,
        '元アスリート／その他格闘技': 70,
        '元アスリート／アイススケート': 71,
        '元アスリート／スキー競技': 72,
        '元アスリート／スノボ競技': 73,
        '元アスリート／サーフィン': 74,
        '元アスリート／Xスポーツ（クライミング・BMX・スケボーなど）': 75,
        '元アスリート／モータースポーツ': 76,
        '元アスリート／公営競技': 77,
        '元アスリート／eスポーツ': 78,
        '元アスリート／その他': 79,
        '文化人／学者': 80,
        '文化人／評論家': 81,
        '文化人／作家': 82,
        '文化人／イラストレーター': 83,
        '文化人／カメラマン': 84,
        '文化人／映画監督': 85,
        '文化人／演出家': 86,
        '文化人／芸術家': 87,
        '文化人／建築家': 88,
        '文化人／料理関係': 89,
        '文化人／書家': 90,
        '文化人／漫画家': 91,
        '文化人／医療関係': 92,
        '文化人／美容関係': 93,
        '文化人／棋士': 94,
        '文化人／服飾関係': 95,
        '文化人／その他': 96,
        '海外タレント／海外タレント': 97
    }

    # 更新日を表すカラム
    COLUMN_UPDATE_DATE = "update_date"
    # タレントコードカラム
    COLUMN_TALENT_CODE = "talent_code"
    # 性別カラム
    COLUMN_GENDER = "gender"
    # 生年月日カラム
    COLUMN_BDAY = "birthday"
    # 取得対象フラグ
    COLUMN_IS_ACTIVE = "is_active"
    # 取得対象フラグ
    COLUMN_OCCUPATION = "occupation_genre"

    # m_talent に登録するカラム定義
    COLUMNS_TALENT_MASTER = ["talent_code", "influencer_id", "talent_name_org", "gender", "birthday",
                             "occupation_genre", "birth_place", "hobbies_skills", "update_date", "is_active"]

    # t_talent_social に登録するカラム定義
    COLUMNS_TALENT_SOCIAL = [
        "talent_code", "influencer_id", "social_platform", "social_link",
        "youtube_subscriber_count", "youtube_male_ratio", "youtube_female_ratio",
        "youtube_age13_17", "youtube_age18_24", "youtube_age25_34",
        "youtube_age35_44", "youtube_age45_54", "youtube_age55_64", "youtube_age65_",
        "twitter_follower_count", "instagram_follower_count", "instagram_posts_count", "popularity_score"
    ]

    def __init__(self, file_path):
        self.file_path = file_path
        self.obj_fetch_talentlist = FetchTalentList(self.file_path)
        self.obj_date = DateUtil()
        self.batchStatus = BatchStatus()

    def execute(self):
        df, file_name = self.obj_fetch_talentlist.get_talentlist_from_api()
        self.batchStatus.update_batch_status(file_name, 'start')
        self.obj_talent_social = TalentSocial()
        df = self.transform_columns(df)
        self.obj_talent_social.delete()
        self.obj_talent_social.insert(df[self.COLUMNS_TALENT_SOCIAL])
        self.obj_talent_combined = TalentCombined()
        self.obj_talent_combined.update_follower_counts()
        self.batchStatus.update_batch_status(file_name, 'finish')

    def transform_columns(self, df):
        # カラム名を一括変換
        df.columns = df.columns.str.lower()
        df.rename(columns=self.CONV_COLUMN_MAP, inplace=True)

        # タレントコードが数値以外の不正行を削除
        # df = DataFrameUtil().validate_numeric(df, self.COLUMN_TALENT_CODE)
        df['talent_code'] = df['talent_code'].astype('int')

        # '%Y年%m月%d日' のフォーマット -> "YYYY-MM-DD" に変換
        def format_date1(datestr):
            if datestr:
                return self.obj_date.parse_date(datestr, type=1)
            else:
                return None

        # 'YYYYMMDD' のフォーマット' -> 'YYYY-MM-DD' に変換
        def format_date2(datestr):
            return self.obj_date.parse_date(datestr, type=2)

        def map_occupation_genre(text):
            result = []
            if text:
                text = text.split('、')
                for row in text:
                    if self.CONV_OCCUPATION_MAP.get(row):
                        result.append(self.CONV_OCCUPATION_MAP.get(row))
            else:
                None
            return ','.join(str(x) for x in result) if result else None

        # birthday のフォーマット変更 YYYY-MM-DD
        df[self.COLUMN_BDAY] = df[self.COLUMN_BDAY].apply(format_date1)

        # 更新日のフォーマット変更 YYYY-MM-DD
        # df[self.COLUMN_UPDATE_DATE] = df[self.COLUMN_UPDATE_DATE].astype(str).apply(format_date2)
        df[self.COLUMN_UPDATE_DATE] = pd.to_datetime(df[self.COLUMN_UPDATE_DATE], format='%Y%m%d').dt.date

        # gender カラム値の変換
        df[self.COLUMN_GENDER].replace(self.CONV_GENDER_MAP, inplace=True)

        df[self.COLUMN_OCCUPATION] = df[self.COLUMN_OCCUPATION].astype(str).apply(map_occupation_genre)

        # is_active カラムを付与
        df = self._append_column_is_active(df)
        return df

    def _append_column_is_active(self, df):
        def check_gender(x):
            if x == 0 or x == 1:
                return 0
            else:
                return 1

        # df = DataFrameUtil().add_column(df, self.COLUMN_GENDER, self.COLUMN_IS_ACTIVE)
        df[self.COLUMN_IS_ACTIVE] = df[self.COLUMN_GENDER].apply(check_gender)
        return df


class FetchTalentList():
    S3_DIR = "talent_list"

    def __init__(self, file_path):
        # self.s3access = S3Access()
        self.file_path = file_path

    def get_talentlist_from_api(self):
        # 最新データを取得して保存したパスを取得する
        path_local = f"json_files/{self.file_path}"
        file_name = os.path.basename(path_local).split('.')[0]
        # S3にバックアップ
        # self._backup_data(path_local)

        df = pd.read_json(path_local)
        #        df.to_csv("/tmp/org.csv", sep="\t")

        # 削除
        # os.remove(path_local)
        return df, file_name

    def _backup_data(self, path_local):
        filename = os.path.basename(path_local)

        # S3にバックアップ
        path_s3 = "{}/{}".format(self.S3_DIR, filename)
        self.s3access.put(path_local, path_s3)


# バッチ処理のステータステーブルを管理するクラス
class BatchStatus(object):
    _instance = None

    # 管理テーブル名
    TABLE_NAME = "t_batch_status"

    # コンストラクタ
    def __init__(self):

        self.dbconnect = DBConnect()
        self.engine = self.dbconnect.getEngine()

    # 実行対象日付の情報を返します
    # batch_status=1 の条件で t_batch_status テーブルからデータを取得します
    def update_batch_status(self, batch_end_date, mode):
        if mode == 'start':
            with self.engine.connect() as conn:
                query = f"update {self.TABLE_NAME} set batch_status = 1 where batch_end_date = '{batch_end_date}'"
                conn.execute(query)
                print('Changed status to 1 of running batch_id')
        if mode == 'finish':
            with self.engine.connect() as conn:
                query = f"update {self.TABLE_NAME} set batch_status = 2 where batch_end_date = '{batch_end_date}'"
                conn.execute(query)
                print('Changed status to 2 of running batch_id')

    def getRunningDates(self):
        with self.engine.connect() as conn:
            metadata = db.MetaData(schema=self.dbconnect.db_name)
            table = db.Table(self.TABLE_NAME, metadata, autoload=True, autoload_with=conn)
            query = db.select([
                table.columns.batch_id,
                table.columns.batch_status,
                table.columns.batch_message,
                table.columns.batch_start_date,
                table.columns.batch_end_date,
                table.columns.created_at,
                table.columns.updated_at,
            ]).where(
                table.columns.batch_status == 1
            ).order_by(desc(table.columns.batch_start_date))

            df_fetch = pd.DataFrame(conn.execute(query),
                                    columns=["batch_id", "batch_status", "batch_message", "batch_start_date",
                                             "batch_end_date", "created_at", "updated_at"])

            for i, row in df_fetch.iterrows():
                if row["batch_status"] == 1:
                    return row

                return None


class TalentSocial(object):
    _instance = None

    # 管理テーブル名
    TABLE_NAME = "t_talent_social"
    TABLE_TALENT_MASTER = "m_talent"

    # コンストラクタ
    def __init__(self):
        self.dbconnect = DBConnect()
        self.engine = self.dbconnect.getEngine()
        self.batchStatus = BatchStatus()
        self.running_batch = self.batchStatus.getRunningDates()

    def insert(self, df):
        try:
            # get running batch and talent list from db (to get talent_id)
            print(self.running_batch['batch_id'])
            with self.engine.connect() as conn:
                df_talent_db = pd.read_sql('SELECT talent_id, talent_code FROM {};'.format(self.TABLE_TALENT_MASTER),
                                           con=conn)

                # in order to add talent_id, we have to merge 2 dfs
                df.dropna(subset=["social_link"], inplace=True)
                df['batch_id'] = self.running_batch['batch_id']
                df = pd.merge(df, df_talent_db, on=['talent_code'], how='inner')

                df.to_sql(self.TABLE_NAME, con=conn, if_exists="append", index=False)
                print('Inserted {} talent sns data to t_talent_social'.format(len(df_talent_db)))
        except Exception as err:
            print('Error occurred during insert_sns_data: {}'.format(str(err)))

    def delete(self):
        batch_id = self.running_batch['batch_id']
        with self.engine.connect() as conn:
            query = f"delete from {self.TABLE_NAME} where batch_id = {batch_id}"
            conn.execute(query)
            print(f'Deleted all records of {batch_id}')

class TalentCombined:
    TABLE_NAME = 't_talent_combined'
    TABLE_SOCIAL = 't_talent_social'
    def __init__(self):
        self.dbconnect = DBConnect()
        self.engine = self.dbconnect.getEngine()
        self.batchStatus = BatchStatus()
        self.running_batch = self.batchStatus.getRunningDates()

    def update_follower_counts(self):
        with self.engine.connect() as conn:
            query = text(f'''update {self.TABLE_NAME} ttc
                            join (select talent_id,
                                         batch_id,
                                         max(twitter_follower_count)   as twitter_follower_count,
                                         max(youtube_subscriber_count) as youtube_subscriber_count,
                                         max(instagram_follower_count) as instagram_follower_count
                                    from {self.TABLE_SOCIAL}
                                   where batch_id = :batch_id
                                   group by talent_id, batch_id) tts on ttc.talent_id = tts.talent_id and ttc.batch_id = tts.batch_id
                           set ttc.twitter_follower_count   = tts.twitter_follower_count,
                               ttc.youtube_subscriber_count = tts.youtube_subscriber_count,
                               ttc.instagram_follower_count = tts.instagram_follower_count
                         where ttc.batch_id = :batch_id
                           and tts.batch_id = :batch_id;''')
            conn.execute(query, batch_id=self.running_batch['batch_id'])
            print(f"Updated follower counts of {self.TABLE_NAME} table")


for json_file in os.listdir('json_files'):
#     print(json_file)
    obj_execute = Reload_Social_Datas(json_file)
    obj_execute.execute()
