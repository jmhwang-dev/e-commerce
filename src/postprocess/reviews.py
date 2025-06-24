from utils import *
import pandas as pd
import re
from pathlib import Path
from typing import List

def merge_results(src_paths: List[Path], dst_prefix: str) -> None:
    gather_config = GatherConfig(
        src_paths=src_paths,
        dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, f'{dst_prefix}.tsv'),
        inplace=True
    )

    gather_config.save()
    
    series_list = []
    for src_path in gather_config.src_paths:
        ds = get_dataset(src_path).iloc[:, 0]
        series_list.append(ds)
    
    merged_series = pd.concat(series_list, ignore_index=True)
    merged_df = merged_series.to_frame(name=dst_prefix)
    merged_df.to_csv(gather_config.dst_path, sep='\t', index=False)