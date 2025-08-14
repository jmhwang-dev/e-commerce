import json
from pprint import pformat
import logging
import time
from service.init.inference import *

os.makedirs("/app/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/p2e.log'),  # 로그를 저장할 파일
        logging.StreamHandler()  # 콘솔 출력 (선택적)
])

if __name__=="__main__":
    try:
        translator = get_translator()
        analyzer = get_sentiment_analyzer()
        logging.info("모델 로드 완료")

        consumer = wait_for_partition_assignment()
        logging.info(f'할당된 파티션: {consumer.assignments()}')

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.info(f'컨슈머 오류: {msg.error()}')
                break

            value = json.loads(msg.value().decode('utf-8'))
            with open("/app/logs/output.json", 'w') as f:
                json.dump(value, f)
            # logging.info(f"\n{pformat(value, indent=2)}")
            logging.info("메시지 수신 완료")

            dataset = [value['prompt']]
            
            start = time.time()
            por2eng_outputs = translator(
                dataset,
                max_new_tokens=512,
                do_sample=False,
                batch_size=len(dataset)
            )
            end = time.time()
            logging.info(f"완료 - por2eng: {end-start}")

            por2eng_results = [
                out[0]['generated_text'][-1]['content'] for out in por2eng_outputs
            ]
            por2eng_results = list(map(lambda x: x.strip(), por2eng_results))
            logging.info(por2eng_results)

            start = time.time()
            senti_outputs = analyzer(por2eng_results, batch_size=len(por2eng_results))
            end = time.time()
            logging.info(f"완료 - senti: {end-start}")
            senti_results = [{item['label']: item['score'] for item in row} for row in senti_outputs]
            logging.info(senti_results)
    except TimeoutError as e:
        print(str(e))
        exit()

    except KeyboardInterrupt:
        consumer.close()
    finally:
        consumer.close()