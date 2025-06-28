from utils import get_dataset, BronzeDataName, SilverDataName
from utils.paths import SILVER_DIR
import pandas as pd
import os

# Load datasets
products_df, _ = get_dataset(BronzeDataName.PRODUCTS)
products_df = products_df.drop_duplicates()
category_df, _ = get_dataset(BronzeDataName.CATEGORY)
category_df = category_df.drop_duplicates()


def translate_categories(products_df: pd.DataFrame, category_df: pd.DataFrame) -> pd.DataFrame:
    # 1. 고유 카테고리 추출
    category_origin_df = products_df[['product_category_name']].dropna().drop_duplicates()
    category_origin_df.columns = ['origin']

    # 2. 카테고리 번역 테이블 생성 (left join)
    category_df.columns = ['origin', 'eng']  # 컬럼 이름 통일
    category_merged = pd.merge(category_origin_df, category_df, on='origin', how='left')

    # 3. 수동 보정
    category_merged.loc[category_merged['origin'] == 'pc_gamer', 'eng'] = 'gaming_pc'
    category_merged.loc[
        category_merged['origin'] == 'portateis_cozinha_e_preparadores_de_alimentos',
        'eng'
    ] = 'portable_kitchen_and_food_preparators'

    # 4. 저장
    category_merged.to_csv(
        os.path.join(SILVER_DIR, SilverDataName.CATEGORY.value),
        sep='\t',
        index=False
    )

    return category_merged

def translate_products(products_df: pd.DataFrame, category_mapping_df: pd.DataFrame) -> None:
    # 1. 제품에 번역된 카테고리 병합
    merged = pd.merge(
        products_df,
        category_mapping_df,
        how='left',
        left_on='product_category_name',
        right_on='origin'
    )

    # 2. 번역된 카테고리로 대체
    products_df['product_category_name'] = merged['eng']

    # 3. 저장
    products_df.to_csv(
        os.path.join(SILVER_DIR, SilverDataName.PRODUCTS.value),
        sep='\t',
        index=False
    )

if __name__ == "__main__":
    category_mapping_df = translate_categories(products_df, category_df)
    translate_products(products_df, category_mapping_df)

