# TODO: postproces.ipynb 정리
from common.paths import *
import pandas as pd

def concat_results(paths, col_name) -> pd.DataFrame:
    translation_df_list = []
    for path in paths:
        df = pd.read_csv(
            path,
            header=None,
            names=["review"],
            sep=r'\n',
            engine='python',
            quotechar='"',
            doublequote=True,
        )
        translation_df_list.append(df)
    concat_df = pd.concat(translation_df_list, axis=0)
    concat_df.columns = [col_name]
    return concat_df.reset_index(drop=True)

def remove_special_chars(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        # 1. 이중 따옴표 먼저 처리
        df[col] = df[col].str.replace('""', '"', regex=False)       
        # 2. 시작과 끝의 따옴표만 제거 (정규표현식 사용)
        df[col] = df[col].str.replace(r'^[“"](.*?)[”"]$', r'\1', regex=True)

    return df

def resolve_conflict_rows(translation_df: pd.DataFrame):
    label_conflicts = (
        translation_df
        .groupby('eng')['max_sentimental']
        .nunique()
        .gt(1)  # 그룹 내 감성 레이블이 2개 이상이면 True
    )

    # 2. 레이블이 다른 p2e 목록 추출
    conflicting_p2e = label_conflicts[label_conflicts].index

    # 3. 해당 eng를 가진 모든 행 추출
    conflict_rows = translation_df[translation_df['eng'].isin(conflicting_p2e)]
    print("Before resolving conflicts:")
    print(conflict_rows)
    print()
    
    translation_df.loc[[35148, 35451], ['negative','neutral','positive','max_sentimental']] = \
    translation_df.loc[36160, ['negative','neutral','positive','max_sentimental']].values

    print("After resolving conflicts:")
    print(translation_df.loc[conflict_rows.index])
        
    return translation_df