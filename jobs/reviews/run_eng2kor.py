from common.config import *
from common.paths import *
from common.loader import *

from pipelines import Translator

import multiprocessing as mp
from pathlib import Path

def translate_e2k(src_path, dataset, dataset_start_index_, dataset_end_index_, dst_file_name, device_, initial_batch_size_, ):
    config_e2k = TranslatePipelineConfig(
        src_path=src_path,
        dataset_start_index=dataset_start_index_,
        dataset_end_index=dataset_end_index_,

        dst_path=dst_file_name,
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device=device_,
        initial_batch_size=initial_batch_size_,
        language_from='English',
        language_into='Korean',
        inplace=True
    )
    config_e2k.save()
    translator_p2e = Translator(config_e2k)
    translator_p2e.set_input(dataset[dataset_start_index_:dataset_end_index_])
    translator_p2e.run()

if __name__ == "__main__":
    p2e_config_path = Path(CONFIGS_INFERENCE_DIR) / "1_por2eng.yml"
    p2e_config_atuo = TranslatePipelineConfig.load(p2e_config_path)

    e2k_dataset_auto = load_dataset(p2e_config_atuo.dst_path)
    output_path_worker1 = os.path.join(ARTIFACTS_INFERENCE_DIR, '1_eng2kor.txt')
    worker_trans_p2e_auto = mp.Process(
        target=translate_e2k,
        args=(p2e_config_atuo.dst_path, e2k_dataset_auto, 0, len(e2k_dataset_auto), output_path_worker1, 'auto', 9,)
    )

    p2e_config_path = Path(CONFIGS_INFERENCE_DIR) / "2_por2eng.yml"
    p2e_config_cpu = TranslatePipelineConfig.load(p2e_config_path)
    e2k_dataset_cpu = load_dataset(p2e_config_cpu.dst_path)
    output_path_worker2 = os.path.join(ARTIFACTS_INFERENCE_DIR, f'2_eng2kor.txt')
    worker_trans_p2e_cpu = mp.Process(
        target=translate_e2k,
        args=(p2e_config_cpu.dst_path, e2k_dataset_cpu, 0, len(e2k_dataset_cpu), output_path_worker2, 'cpu', 120,)
    )

    worker_trans_p2e_auto.start()
    worker_trans_p2e_cpu.start()

    worker_trans_p2e_auto.join()
    worker_trans_p2e_cpu.join()
