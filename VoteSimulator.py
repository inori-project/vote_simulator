import pandas as pd
import os
import sqlalchemy

import numpy as np
import tempfile
from enum import IntEnum


class VoteType(IntEnum):
    # 単勝
    Win = 1
    # 複勝
    Place = 2
    # 馬連
    Quinella = 3
    # 馬単
    Exacta = 4
    # ワイド
    Wide = 5
    # 3連複
    Trio = 6
    # 3連単
    Trifecta = 7
    # undefined
    Undefined = 9


class VoteSimulator(object):

    def __init__(self, db=os.environ.get("DB_STRING"), src='jrdb', nocache=False):
        self.inited = False
        self.db = sqlalchemy.create_engine(db)

        self.temp_load_df = os.path.join(tempfile.gettempdir(), ".votesimulator")
        self.df = self.load(src, nocache)

    def load_from_jrdb(self):
        hjc_df = pd.read_sql(f"SELECT * FROM d_jrdb_HJC",
                             index_col=['race_key'], con=self.db)
        print('hjc:', len(hjc_df))
        for c in [_c for _c in hjc_df.columns if "day" not in _c]:
            hjc_df[c] = hjc_df[c].fillna(0).astype(np.int64)
        _buf = []
        # 組み合わせは事前に昇順ソートしておく
        for row in hjc_df.to_records():
            # win
            for i in range(1, 4):
                if row[f"win_payout_{i}"] > 0:
                    _buf.append(pd.Series(
                        [row['race_key'], VoteType.Win, str(row[f"win_{i}"]).zfill(2), row[f"win_payout_{i}"]]))
            # place
            for i in range(1, 6):
                if row[f"place_payout_{i}"] > 0:
                    _buf.append(pd.Series(
                        [row['race_key'], VoteType.Place, str(row[f"place_{i}"]).zfill(2), row[f"place_payout_{i}"]]))
            # quinella
            for i in range(1, 4):
                if row[f"quinella_payout_{i}"] > 0:
                    a = sorted([row[f"quinella_{i}_1"], row[f"quinella_{i}_2"]])
                    _buf.append(pd.Series([row['race_key'], VoteType.Quinella, str(a[0]).zfill(2) + str(a[1]).zfill(2),
                                           row[f"quinella_payout_{i}"]]))
            # wide
            for i in range(1, 8):
                if row[f"wide_payout_{i}"] > 0:
                    a = sorted([row[f"wide_{i}_1"], row[f"wide_{i}_2"]])
                    _buf.append(pd.Series([row['race_key'], VoteType.Wide, str(a[0]).zfill(2) + str(a[1]).zfill(2),
                                           row[f"wide_payout_{i}"]]))
            # exacta
            for i in range(1, 7):
                if row[f"exacta_payout_{i}"] > 0:
                    _buf.append(pd.Series([row['race_key'], VoteType.Exacta,
                                           str(row[f"exacta_{i}_1"]).zfill(2) + str(row[f"exacta_{i}_2"]).zfill(2),
                                           row[f"exacta_payout_{i}"]]))
            # trio
            for i in range(1, 4):
                if row[f"trio_payout_{i}"] > 0:
                    a = sorted([row[f"trio_{i}_1"], row[f"trio_{i}_2"], row[f"trio_{i}_3"]])
                    _buf.append(pd.Series(
                        [row['race_key'], VoteType.Trio, str(a[0]).zfill(2) + str(a[1]).zfill(2) + str(a[2]).zfill(2),
                         row[f"trio_payout_{i}"]]))
            # trifecta
            for i in range(1, 7):
                if row[f"trifecta_payout_{i}"] > 0:
                    _buf.append(pd.Series([row['race_key'], VoteType.Trifecta,
                                           str(row[f"trifecta_{i}_1"]).zfill(2) + str(row[f"trifecta_{i}_2"]).zfill(
                                               2) + str(row[f"trifecta_{i}_3"]).zfill(2), row[f"trifecta_payout_{i}"]]))

        a = pd.DataFrame(_buf)
        a.columns = ['race_key', 'vote_type', 'joined_horse_no', 'payout']
        a = a.set_index(['race_key', 'vote_type', 'joined_horse_no']).sort_index()
        return a

    def load(self, src='jrdb', nocache=False):
        # load済みのキャッシュがある場合はそれを使用する
        if not nocache and os.path.exists(self.temp_load_df):
            print('load from cache..', self.temp_load_df)
            df = pd.read_pickle(self.temp_load_df)
        else:
            # JRDBの払い戻しデータを使用する
            if src == 'jrdb':
                print('load from jrdb data..')
                df = self.load_from_jrdb()
            elif src == 'jvlink':
                print('load from jvlink data..')
                # TODO: jvlink
                df = pd.DataFrame()
        self._save(df)
        return df

    def _save(self, df):
        df.to_pickle(self.temp_load_df)

    def run(self, votes):
        _buf = []
        for o in votes:
            for v in o['votes']:
                if v['type'] in [VoteType.Quinella, VoteType.Wide, VoteType.Trio]:
                    v['comb'] = sorted(v['comb'])

                c = "".join(list(map(lambda s: str(s).zfill(2), v['comb'])))
                _buf.append([o['race_key'], v['type'], c, v['amount']])

        vdf = pd.DataFrame(_buf)
        vdf.columns = ['race_key', 'vote_type', 'joined_horse_no', 'amount']
        vdf = vdf.set_index(['race_key', 'vote_type', 'joined_horse_no'])
        vdf["payout"] = self.df['payout']
        vdf['payout'] = vdf['payout'].fillna(0) * (vdf['amount'] / 100)
        vdf['pnl'] = vdf['payout'] - vdf['amount']
        return vdf
