# TODO: postproces.ipynb 정리
from common.paths import *
import pandas as pd

def merge_results(paths):
    translated_result_df = []
    # for path in [preprocessed_texts_path, p2e_path, e2k_path]:
    for path in paths:
        df = pd.read_csv(
            path,
            header=None,
            names=["review"],
            sep=r'\n',
            engine='python',
            quotechar='"',
            doublequote=True
        )
        print(df.shape)
        translated_result_df.append(df)

    merged_translation = pd.concat(translated_result_df, axis=1)
    merged_translation.columns = ['preprocessed', 'p2e', 'e2k']

    # 후처리 필요...
    for col in ["preprocessed", "p2e", "e2k"]:
        # 1. 이중 따옴표 먼저 처리
        merged_translation[col] = merged_translation[col].str.replace('""', '"', regex=False)
        # 2. 시작과 끝의 따옴표만 제거 (정규표현식 사용)
        merged_translation[col] = merged_translation[col].str.replace(r'^"(.*)"$', r'\1', regex=True)

    merged_translation.shape