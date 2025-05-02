import multiprocessing as mp
import threading
import time
import logging
from queue import Empty
import traceback

from inference import *
from config import *

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def translate_p2e(dataset, queue_trans_e2k, p2e_ready_event, p2e_done_event):
    """포르투갈어에서 영어로 번역하는 함수"""
    try:
        translator_p2e = get_translator_p2e()
        p2e_ready_event.set()
        logger.info("P2E 번역기 준비 완료")

        start_index = 0
        end_index = translator_p2e.current_batch_size
        
        while start_index < len(dataset):
            if end_index > len(dataset):
                end_index = len(dataset)
            
            chunk = dataset[start_index:end_index]
            logger.info(f"P2E 번역 중: {start_index}~{end_index} / {len(dataset)}")
            
            translator_p2e.set_input(chunk)
            translator_p2e.run()
            results = translator_p2e.get_results()
            
            for result in results:
                queue_trans_e2k.put(result)

            save_translation(translator_p2e.config.dst_path, chunk, results)
            translator_p2e.increase_batch_size()
            
            start_index = end_index
            end_index += translator_p2e.current_batch_size
        
        logger.info("P2E 번역 작업 완료")
        p2e_done_event.set()
    except Exception as e:
        logger.error(f"P2E 번역 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
        p2e_done_event.set()  # 오류 발생해도 다른 프로세스가 종료될 수 있도록 이벤트 설정

def translate_e2k(queue_trans_e2k, p2e_ready_event, p2e_done_event, worker_id=1):
    """영어에서 한국어로 번역하는 함수"""
    try:
        p2e_ready_event.wait()
        translator_e2k = get_translator_e2k()
        logger.info(f"E2K 번역기 {worker_id} 준비 완료")
        
        processed_count = 0
        
        while True:
            # p2e 완료되고 큐가 비었을 때 종료
            if p2e_done_event.is_set() and queue_trans_e2k.empty():
                logger.info(f"E2K 번역기 {worker_id} 작업 완료: {processed_count}개 처리됨")
                return
            
            dataset = []
            
            # 최대 배치 크기만큼 큐에서 데이터 가져오기
            try:
                # 처음 항목은 최대 1초까지 기다림 (CPU 부하 방지)
                if not dataset:
                    text = queue_trans_e2k.get(timeout=1)
                    dataset.append(text)
                
                # 나머지 배치는 대기 없이 빠르게 채움
                for _ in range(translator_e2k.current_batch_size - 1):
                    if not queue_trans_e2k.empty():
                        text = queue_trans_e2k.get_nowait()
                        dataset.append(text)
                    else:
                        break
                
            except Empty:
                # 타임아웃 발생 시 다시 검사
                if p2e_done_event.is_set():
                    continue
                time.sleep(0.1)  # 짧은 대기 후 재시도
            
            if dataset:
                logger.info(f"E2K 번역기 {worker_id}: {len(dataset)}개 항목 번역 중")
                translator_e2k.set_input(dataset)
                translator_e2k.run()
                results = translator_e2k.get_results()
                save_translation(translator_e2k.config.dst_path, dataset, results)
                translator_e2k.increase_batch_size()
                processed_count += len(dataset)
    
    except Exception as e:
        logger.error(f"E2K 번역기 {worker_id} 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())


def monitor_queue_sizes(queues, stop_event, interval=5):
    """큐 크기를 모니터링하는 함수"""
    while not stop_event.is_set():
        sizes = {name: q.qsize() for name, q in queues.items()}
        logger.info(f"현재 큐 크기: {sizes}")
        time.sleep(interval)

if __name__ == "__main__":
    try:
        logger.info("작업 시작")
        dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
        dataset = load_dataset(dataset_config['dst_path'])
        logger.info(f"데이터셋 로드 완료: {len(dataset)}개 항목")
        
        # 멀티프로세싱 리소스 초기화
        manager = mp.Manager()
        p2e_ready_event = manager.Event()
        p2e_done_event = manager.Event()
        monitor_stop_event = manager.Event()
        
        queue_trans_e2k = manager.Queue()
        
        # 모니터링 스레드 시작
        queues = {
            "E2K": queue_trans_e2k,
        }
        monitor_thread = threading.Thread(
            target=monitor_queue_sizes, 
            args=(queues, monitor_stop_event, 10)
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # P2E 번역 프로세스 시작
        worker_trans_p2e = mp.Process(
            target=translate_p2e, 
            args=(dataset, queue_trans_e2k, p2e_ready_event, p2e_done_event)
        )
        worker_trans_p2e.start()
        
        # E2K 번역 프로세스 시작
        worker_trans_e2k = mp.Process(
            target=translate_e2k, 
            args=(queue_trans_e2k, p2e_ready_event, p2e_done_event, 1)
        )
        worker_trans_e2k.start()
        
        # P2E 프로세스가 일정량 작업 후 두 번째 E2K 작업자 시작
        # time.sleep(10)  # 일정 시간 후 또는 큐 크기에 따라 추가 작업자 시작할 수도 있음
        logger.info("추가 E2K 번역 프로세스 시작")
        worker_trans_e2k_2 = mp.Process(
            target=translate_e2k, 
            args=(queue_trans_e2k, p2e_ready_event, p2e_done_event, 2)
        )
        worker_trans_e2k_2.start()
        
        # 모든 작업자 프로세스 완료 대기
        worker_trans_p2e.join()
        logger.info("P2E 번역 프로세스 종료됨")
        
        worker_trans_e2k.join()
        logger.info("E2K 번역 프로세스 1 종료됨")
        
        worker_trans_e2k_2.join()
        logger.info("E2K 번역 프로세스 2 종료됨")
        
        # 모니터링 중지
        monitor_stop_event.set()
        monitor_thread.join()
        
        logger.info("모든 작업 완료")
    
    except Exception as e:
        logger.error(f"메인 프로세스 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        # 리소스 정리
        logger.info("리소스 정리 중...")