# TODO: postproces.ipynb 정리
from common.paths import *
import pandas as pd
import re

def concat_results(paths, col_name) -> pd.DataFrame:
    translation_df_list = []
    for path in paths:
        df = pd.read_csv(
            path,
            header=None,
            names=[col_name],
            sep=r'\n',
            engine='python',
            quotechar='"',
            doublequote=True,
        )

        # df = pd.read_csv(
        #     path,
        #     header=None,
        #     names=[col_name],
        #     sep=r'\n',
        #     engine='python',
        #     quotechar=None  # ✅ 따옴표 무시하고 그대로 읽기
        # )
        
        df[col_name] = df[col_name].str.strip('"“”')

        translation_df_list.append(df)
    concat_df = pd.concat(translation_df_list, axis=0)
    # concat_df.columns = [col_name]
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


# 문자열 정리 함수 (더 정확한 버전)
def clean_text(text):
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    # 1. 맨 앞 따옴표 제거
    if text.startswith('"'):
        text = text[1:]
    
    # 2. 연속된 따옴표들을 하나로 변경 (2개 이상의 연속 따옴표를 1개로)
    text = re.sub(r'"{2,}', '"', text)
    
    # 3. 맨 마지막 따옴표 제거
    if text.endswith('"'):
        text = text[:-1]
    
    return text

# 데이터프레임 전체 문자열 정리 함수 (추천)
def clean_dataframe_strings(df, columns=None):
    """
    데이터프레임의 문자열 열들을 정리하는 함수
    
    Parameters:
    df: pandas DataFrame
    columns: 처리할 열 목록 (None이면 모든 문자열 열 자동 처리)
    """
    df_cleaned = df.copy()
    
    # columns가 지정되지 않으면 모든 문자열 열을 자동 선택
    if columns is None:
        columns = df_cleaned.select_dtypes(include=['object']).columns
    
    # 각 열에 대해 문자열 정리 적용
    for col in columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_text)
    
    return df_cleaned
