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
