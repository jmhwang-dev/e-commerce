from setup import *
from config import *
from loader import *

import multiprocessing as mp

def translate_p2e(dataset, device_, initial_batch_size_, dst_file_name_):
    translator_p2e = get_translator_p2e(device_, initial_batch_size_, dst_file_name_)
    translator_p2e.set_input(dataset)
    translator_p2e.run()

if __name__ == "__main__":
    # TODO: Needs abstraction
    dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
    dataset = load_dataset(dataset_config['dst_path'])

    chunk_size = len(dataset) // 5

    worker_trans_p2e_auto = mp.Process(target=translate_p2e, args=(dataset[:chunk_size*4], 'auto', 10, 'trans_p2e_auto.txt'))
    worker_trans_p2e_auto.start()

    worker_trans_p2e_cpu = mp.Process(target=translate_p2e, args=(dataset[chunk_size*4:], 'cpu', 100, 'trans_p2e_cpu.txt'))
    worker_trans_p2e_cpu.start()

    worker_trans_p2e_auto.join()
    worker_trans_p2e_cpu.join()