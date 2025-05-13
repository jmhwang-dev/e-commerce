from setup import *
from config import *
from loader import *

import multiprocessing as mp

def analyze_sentiment(dataset, device_, initial_batch_size_, dst_file_name_):
    analyzor = get_sentiment_analyzer(device_, initial_batch_size_, dst_file_name_)
    analyzor.set_input(dataset)
    analyzor.run()

if __name__ == "__main__":
    # TODO: Needs abstraction
    dataset_path = os.path.join(ARTIFACT_DIR, 'inference', "trans_p2e_cpu.txt")
    dataset = load_dataset(dataset_path)

    chunk_size = len(dataset) // 2

    worker_senti_gpu = mp.Process(target=analyze_sentiment, args=(dataset[:chunk_size], 'cuda', 300, 'senti_eng_cpu3_but_gpu.csv'))
    worker_senti_gpu.start()

    worker_senti_cpu = mp.Process(target=analyze_sentiment, args=(dataset[chunk_size:], 'cpu', 300, 'senti_eng_cpu3.csv'))
    worker_senti_cpu.start()

    worker_senti_cpu.join()
    worker_senti_gpu.join()