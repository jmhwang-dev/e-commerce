import multiprocessing as mp
import threading

from inference import *
from config import *

def translate_p2e(dataset, queue_trans_e2k:mp.Queue, queue_senti_eng:mp.Queue, p2e_ready_event:threading.Event, p2e_done_event:threading.Event):
    translator_p2e = get_translator_p2e()
    p2e_ready_event.set()

    start_index = 0
    end_index = translator_p2e.current_batch_size
    while start_index < len(dataset):
        
        if end_index > len(dataset):
            end_index = len(dataset)

        chunk = dataset[start_index:end_index]

        translator_p2e.set_input(chunk)
        translator_p2e.run()
        results = translator_p2e.get_results()
        for result in results:
            queue_trans_e2k.put(result)
            queue_senti_eng.put(result)

        save_translation(translator_p2e.config.dst_path, chunk, results)
        translator_p2e.increase_batch_size()

        start_index = end_index
        end_index += translator_p2e.current_batch_size
    
    p2e_done_event.set()

def translate_e2k(queue_trans_e2k:mp.Queue, p2e_ready_event:threading.Event, p2e_done_event:threading.Event):
    p2e_ready_event.wait()
    translator_e2k = get_translator_e2k()

    while True:
        if p2e_done_event.is_set() and queue_trans_e2k.empty():
            return
        dataset = []

        for _ in range(translator_e2k.current_batch_size):
            if not queue_trans_e2k.empty():
                text = queue_trans_e2k.get()
                dataset.append(text)

        if dataset:
            translator_e2k.set_input(dataset)
            translator_e2k.run()
            results = translator_e2k.get_results()
            save_translation(translator_e2k.config.dst_path, dataset, results)
            translator_e2k.increase_batch_size()

def senti_ananlyze_eng(queue_senti_eng:mp.Queue, p2e_ready_event:threading.Event, p2e_done_event:threading.Event):
    p2e_ready_event.wait()
    analyzer_eng = get_sentiment_analyzer()
    while True:
        if p2e_done_event.is_set() and queue_senti_eng.empty():
            return
        
        dataset = []
        while not queue_senti_eng.empty():
            dataset.append(queue_senti_eng.get())

        if dataset:
            analyzer_eng.set_input(dataset)
            analyzer_eng.run()
            results = analyzer_eng.get_results()
            save_sentiment(analyzer_eng.config.dst_path, results)

if __name__ == "__main__":
    dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
    dataset = load_dataset(dataset_config['dst_path'])

    manager = mp.Manager()
    p2e_ready_event = manager.Event()
    p2e_done_event = manager.Event()

    queue_trans_e2k = mp.Queue()
    queue_senti_eng = mp.Queue()

    worker_trans_p2e = mp.Process(target=translate_p2e, args=(dataset, queue_trans_e2k, queue_senti_eng, p2e_ready_event, p2e_done_event))
    worker_trans_p2e.start()

    worker_trans_e2k = mp.Process(target=translate_e2k, args=(queue_trans_e2k, p2e_ready_event, p2e_done_event))
    worker_trans_e2k.start()

    # worker_senti_eng = mp.Process(target=senti_ananlyze_eng, args=(queue_senti_eng, p2e_ready_event, p2e_done_event))
    # worker_senti_eng.start()


    # print("✅ P2E 프로세스 종료됨, 추가 E2K 프로세스 실행")
    worker_trans_e2k_2 = mp.Process(target=translate_e2k, args=(queue_trans_e2k, p2e_ready_event, p2e_done_event))
    worker_trans_e2k_2.start()
    worker_trans_e2k_2.join()

    worker_trans_p2e.join()
    worker_trans_e2k.join()
    # worker_senti_eng.join()