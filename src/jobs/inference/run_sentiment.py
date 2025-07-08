import os
import multiprocessing as mp
from pathlib import Path
from datetime import datetime

from ecommerce.utils import (
    ensure_directories,
    get_dataset,
    BaseConfig,
    PipelineConfig,
    PostProcessConfig,
    INFERENCE_ARTIFACTS_DIR,
    INFERENCE_CONFIGS_DIR,
)
from ecommerce.inference import run_sentiment
from ecommerce.postprocess.gather import merge_results

def get_workers(config: BaseConfig, dst_prefix, worker_cnt=2) -> dict[str, mp.Process]:
    dataset, _ = get_dataset(config.dst_path)
    dataset = dataset[['por2eng']]
    chunk_size = len(dataset) // worker_cnt

    worker_dict = {}
    for i in range(worker_cnt):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < worker_cnt - 1 else len(dataset)
        dst_file_name = f'{dst_prefix}_{i+1}.tsv' if worker_cnt > 1 else f'{dst_prefix}.tsv'

        device = 'cuda' if i == 0 else 'cpu'
        initial_batch_size = 2000 if i == 0 else 4000
    
        config_senti = PipelineConfig(
            src_path=config.dst_path,
            dataset_start_index=start_index,
            dataset_end_index=end_index,

            dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, dst_file_name),
            checkpoint="j-hartmann/sentiment-roberta-large-english-3-classes",
            device=device,
            initial_batch_size=initial_batch_size,
            inplace=True
        )

        config_senti.save()
        worker = mp.Process(
            target=run_sentiment,
            args=(config_senti, dataset[start_index:end_index])
        )
        worker_dict[config_senti.dst_path] = worker
    return worker_dict

if __name__ == "__main__":
    ensure_directories()
    translation_config_path = Path(INFERENCE_CONFIGS_DIR) / "por2eng_20250626_172543.yml"
    translation_config = PostProcessConfig.load(translation_config_path)
    dst_prefix = f'sentiment_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

    worker_dict = get_workers(translation_config, dst_prefix, 1)

    for worker in worker_dict.values():
        worker.start()

    for worker in worker_dict.values():
        worker.join()

    src_paths = list(worker_dict.keys())
    merge_results(src_paths, dst_prefix)

