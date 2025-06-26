from utils import *
from preprocess import *
from postprocess import *

import multiprocessing as mp
from pathlib import Path
from datetime import datetime

def get_workers(config: PreprocessConfig, dst_prefix, worker_cnt=2) -> dict[str, mp.Process]:
    dataset, _ = get_dataset(config.dst_path)
    chunk_size = len(dataset) // worker_cnt

    worker_dict = {}
    for i in range(worker_cnt):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < worker_cnt - 1 else len(dataset)
        dst_file_name = f'{dst_prefix}_{i+1}.tsv'

        device = 'auto' if i == 0 else 'cpu'
        initial_batch_size = 30 if i == 0 else 120

        config_p2e = TranslatePipelineConfig(
            src_path=config.dst_path,
            dataset_start_index=start_index,
            dataset_end_index=end_index,

            dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, dst_file_name),
            checkpoint="Unbabel/TowerInstruct-7B-v0.2",
            device=device,
            initial_batch_size=initial_batch_size,
            language_from='Portuguese',
            language_into='English',
            inplace=True
        )

        config_p2e.save()

        worker = mp.Process(
            target=run_translator,
            args=(config_p2e, dataset[start_index:end_index])
        )
        worker_dict[config_p2e.dst_path] = worker

    return worker_dict


if __name__ == "__main__":
    preprocess_config_path = Path(PREPROCESS_CONFIGS_DIR) / "clean_comments_text_only.yml"
    preprocess_config = PreprocessConfig.load(preprocess_config_path)
    dst_prefix = f'por2eng_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    worker_dict = get_workers(preprocess_config, dst_prefix, 2)

    for worker in worker_dict.values():
        worker.start()

    for worker in worker_dict.values():
        worker.join()

    src_paths = list(worker_dict.keys())
    merge_results(src_paths, dst_prefix)