from utils.config import *
from utils.paths import *
from utils.loader import *

from pipelines.sentiment import SentimentAnalyzer

import multiprocessing as mp
from pathlib import Path

def analyze_sentiment(src_path, dataset, dataset_start_index_, dataset_end_index_, dst_file_name, device_, initial_batch_size_, ):
    config_senti = PipelineConfig(
        src_path=src_path,
        dataset_start_index=dataset_start_index_,
        dataset_end_index=dataset_end_index_,

        dst_path=dst_file_name,
        checkpoint="j-hartmann/sentiment-roberta-large-english-3-classes",
        device=device_,
        initial_batch_size=initial_batch_size_,
        inplace=True
    )
    config_senti.save()
    translator_p2e = SentimentAnalyzer(config_senti)
    translator_p2e.set_input(dataset[dataset_start_index_:dataset_end_index_])
    translator_p2e.run()

if __name__ == "__main__":
    e2k_config_path = Path(INFERENCE_CONFIGS_DIR) / "por2eng_1.yml"
    e2k_config_atuo = TranslatePipelineConfig.load(e2k_config_path)

    senti_dataset_auto = load_dataset(e2k_config_atuo.dst_path)
    output_path_worker1 = os.path.join(INFERENCE_ARTIFACTS_DIR, 'eng_senti_1.csv')
    worker_trans_p2e_auto = mp.Process(
        target=analyze_sentiment,
        args=(e2k_config_atuo.dst_path, senti_dataset_auto, 0, len(senti_dataset_auto), output_path_worker1, 'cuda', 600,)
    )

    e2k_config_path = Path(INFERENCE_CONFIGS_DIR) / "por2eng_2.yml"
    e2k_config_cpu = TranslatePipelineConfig.load(e2k_config_path)

    senti_dataset_cpu = load_dataset(e2k_config_cpu.dst_path)
    output_path_worker2 = os.path.join(INFERENCE_ARTIFACTS_DIR, 'eng_senti_2.csv')
    worker_trans_p2e_cpu = mp.Process(
        target=analyze_sentiment,
        args=(e2k_config_cpu.dst_path, senti_dataset_cpu, 0, len(senti_dataset_cpu), output_path_worker2, 'cpu', 300,)
    )

    worker_trans_p2e_auto.start()
    worker_trans_p2e_cpu.start()

    worker_trans_p2e_auto.join()
    worker_trans_p2e_cpu.join()
