from utils import *
import pandas as pd
import re
from pathlib import Path
from typing import List

def merge_results(src_paths: List[str], dst_prefix: str) -> None:
    gather_config = GatherConfig(
        src_paths=src_paths,
        dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, f'{dst_prefix}.tsv'),
        inplace=True
    )

    gather_config.save()
    
    df_list = []
    for src_path in gather_config.src_paths:
        df, _ = get_dataset(src_path)
        df_list.append(df)
    
    merged_df = pd.concat(df_list, axis=0, ignore_index=True)
    merged_df.to_csv(gather_config.dst_path, sep='\t', index=False)