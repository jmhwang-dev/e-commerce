from setup import *
from config import *
from loader import *

import multiprocessing as mp

def translate_e2k(dataset, device_, initial_batch_size_, dst_file_name_):
    translator_p2e = get_translator_e2k(device_, initial_batch_size_, dst_file_name_)
    translator_p2e.set_input(dataset)
    translator_p2e.run()

if __name__ == "__main__":
    # TODO: Needs abstraction
    dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
    dataset = load_dataset("./artifact/inference/trans_p2e.txt")

    chunk_size = len(dataset) // 5

    worker_trans_p2e_auto = mp.Process(target=translate_e2k, args=(dataset[:chunk_size*3], 'auto', 9, 'trans_e2k_auto.txt'))
    worker_trans_p2e_auto.start()

    worker_trans_p2e_cpu = mp.Process(target=translate_e2k, args=(dataset[chunk_size*3:], 'cpu', 120, 'trans_e2k_cpu.txt'))
    worker_trans_p2e_cpu.start()

    worker_trans_p2e_auto.join()
    worker_trans_p2e_cpu.join()