from utils.config import *
from utils.paths import *
from utils.loader import *

from preprocess import *

import multiprocessing as mp
from pathlib import Path

def get_workers(config: PreprocessConfig, dst_prefix, worker_cnt=2) -> dict[TranslatePipelineConfig, mp.Process]:
    dataset = load_texts(config.dst_path)
    chunk_size = len(dataset) // worker_cnt

    worker_dict = {}
    for i in range(worker_cnt):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < worker_cnt - 1 else len(dataset)
        dst_file_name = f'{dst_prefix}_{i+1}.txt'

        device = 'auto' if i == 0 else 'cpu'
        initial_batch_size = 9 if i == 0 else 120

        config_translate = TranslatePipelineConfig(
            src_path=config.dst_path,
            dataset_start_index=start_index,
            dataset_end_index=end_index,

            dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, dst_file_name),
            checkpoint="Unbabel/TowerInstruct-7B-v0.2",
            device=device,
            initial_batch_size=initial_batch_size,
            language_from='English',
            language_into='Korean',
            inplace=True
        )

        config_translate.save()

        worker = mp.Process(
            target=run_translator,
            args=(config_translate, dataset[start_index:end_index])
        )
        worker_dict[config_translate.config_save_path] = worker

    return worker_dict


if __name__ == "__main__":
    por2eng_gather_config_path = Path(INFERENCE_CONFIGS_DIR) / "por2eng_gather.yml"
    por2eng_gather_config = GatherConfig.load(por2eng_gather_config_path)
    dst_prefix = 'eng2kor'
    worker_dict = get_workers(por2eng_gather_config, dst_prefix, 2)

    for worker in worker_dict.values():
        worker.start()
        worker.join()

    src_paths = list(map(lambda x: TranslatePipelineConfig.load(x).dst_path, worker_dict.keys()))
    merge_results(src_paths, dst_prefix)