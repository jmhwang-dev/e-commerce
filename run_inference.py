import multiprocessing

from inference import *
from config import *

def translate_p2e(worker: Translator, dataset, output_queue:multiprocessing.Queue):
    i = 0
    while i < len(dataset):
        worker.set_input(dataset[i:i+worker.current_batch_size])
        worker.run()
        results = worker.get_results()
        for result in results:
            output_queue.put(result)
        i += worker.current_batch_size

def translate_e2k(worker: Translator, input_queue:multiprocessing.Queue, output_queue:multiprocessing.Queue):
    # TODO: if p2e has done, load e2k model one more
    while True:
        # if input_queue.empty() and
        worker.set_input(dataset[i:i+worker.current_batch_size])
        worker.run()
        results = worker.get_results()
        for result in results:
            output_queue.put(result)
        i += worker.current_batch_size
    pass

def senti_ananlyze_eng(worker: SentimentAnalyzer, input_queue):
    pass

if __name__ == "__main__":
    dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
    dataset = load_dataset(dataset_config['dst_path'])

    queue_trans_e2k = multiprocessing.Queue()
    queue_senti_eng = multiprocessing.Queue()

    translator_p2e = get_translator_p2e()
    translator_e2k = get_translator_e2k()
    analyzer_eng = get_sentiment_analyzer()

    worekr_trans_p2e = multiprocessing.Process(target=translate_p2e, args=(translator_p2e, dataset, queue_trans_e2k,))
    worekr_trans_e2k = multiprocessing.Process(target=translate_e2k, args=(translator_e2k, queue_trans_e2k, queue_senti_eng, ))
    worekr_senti_eng = multiprocessing.Process(target=senti_ananlyze_eng, args=(analyzer_eng, queue_senti_eng, ))

    workers = [worekr_trans_p2e, worekr_trans_e2k, worekr_senti_eng]
    for worker in workers:
        worker.start()
    
    for worker in workers:
        worker.join()