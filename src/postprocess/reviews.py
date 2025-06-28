from utils import *
from pathlib import Path

def is_conflict(df:pd.DataFrame):
    # p2e별로 max_sentimental 값이 몇 종류 있는지 계산
    label_conflicts = (
        df
        .groupby('por2eng')['sentimentality']
        .nunique()
        .gt(1)  # 그룹 내 감성 레이블이 2개 이상이면 True
    )
    conflict_count = len(label_conflicts[label_conflicts == True])
    if conflict_count > 0:
        print("Conflict exists!")
        return True
    return False

def gather_inference(sent_config_file_name: str, trans_config_file_name: str) -> pd.DataFrame:
    senti_config_path = Path(INFERENCE_CONFIGS_DIR) / sent_config_file_name
    senticonfig = PipelineConfig.load(senti_config_path)
    senti_df, _ = get_dataset(senticonfig.dst_path)

    trans_config_path = Path(INFERENCE_CONFIGS_DIR) / trans_config_file_name
    trans_config = PostProcessConfig.load(trans_config_path)
    trans_df, _ = get_dataset(trans_config.dst_path)

    gather_df = trans_df.copy()
    gather_df['sentimentality'] = senti_df.drop(columns=['por2eng']).idxmax(axis=1)
    return gather_df