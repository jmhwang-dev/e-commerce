from common.config import *
from common.paths import *
from common.loader import *

from inference.pipeline.translate import Translator

import multiprocessing as mp
from pathlib import Path

def translate_p2e(src_path, dataset, dataset_start_index_, dataset_end_index_, dst_file_name, device_, initial_batch_size_, ):
    config_p2e = TranslatePipelineConfig(
        src_path=src_path,
        dataset_start_index=dataset_start_index_,
        dataset_end_index=dataset_end_index_,

        dst_path=dst_file_name,
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device=device_,
        initial_batch_size=initial_batch_size_,
        language_from='Portuguese',
        language_into='English',
        inplace=True
    )
    config_p2e.save()
    translator_p2e = Translator(config_p2e)
    translator_p2e.set_input(dataset[dataset_start_index_:dataset_end_index_])
    translator_p2e.run()

if __name__ == "__main__":
    config_path = Path(ARTIFACT_INFERENCE_PREPROCESS_DIR) / "config_all_portuguese.yml"
    dataset_config = PreprocessConfig.load(config_path)

    dataset = load_dataset(dataset_config.dst_path)
    worker_cnt = 2
    chunk_size = len(dataset) // worker_cnt

    output_path_worker1 = os.path.join(ARTIFACT_INFERENCE_RESULT_DIR, 'trans_p2e_auto.txt')
    worker_trans_p2e_auto = mp.Process(
        target=translate_p2e,
        args=(dataset_config.dst_path, dataset, 0, chunk_size, output_path_worker1, 'auto', 2,)
    )

    output_path_worker2 = os.path.join(ARTIFACT_INFERENCE_RESULT_DIR, 'trans_p2e_cpu.txt')
    worker_trans_p2e_cpu = mp.Process(
        target=translate_p2e,
        args=(dataset_config.dst_path, dataset, chunk_size, len(dataset), output_path_worker2, 'cpu', 2,)
    )


    worker_trans_p2e_auto.start()
    worker_trans_p2e_cpu.start()

    worker_trans_p2e_auto.join()
    worker_trans_p2e_cpu.join()
