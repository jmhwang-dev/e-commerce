# Install transformers from source - only needed for versions <= v4.34
# pip install git+https://github.com/huggingface/transformers.git
# pip install accelerate
import torch
from transformers import pipeline
import pandas as pd
import socket
from tqdm import tqdm
import os
import sys
import logging
import time

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("translation_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchTranslator():
    def __init__(self, device='auto', batch_size=8, model_name="Unbabel/TowerInstruct-7B-v0.2"):
        # CPU/GPU 환경에 따른 설정
        self.device = device
        if device == 'cpu':
            # CPU 최적화
            torch.set_num_threads(os.cpu_count() - 4)  # 사용 가능한 모든 CPU 코어 사용
            self.torch_dtype = torch.float32  # CPU에서는 float32가 더 안정적
        else:
            # GPU 최적화
            self.torch_dtype = torch.bfloat16  # GPU에서는 bfloat16으로 메모리 절약
        
        logger.info(f"초기화: device={device}, batch_size={batch_size}, dtype={self.torch_dtype}")
        
        try:
            self.pipe = pipeline(
                "text-generation", 
                model=model_name,
                torch_dtype=self.torch_dtype, 
                device_map=device
            )
            # ★ 토크나이저 패딩 방향 및 패드 토큰 설정
            self.pipe.tokenizer.padding_side = 'left'
            self.pipe.tokenizer.pad_token = self.pipe.tokenizer.eos_token
            self.batch_size = batch_size
            logger.info("모델 로딩 완료")
        except Exception as e:
            logger.error(f"모델 로딩 실패: {e}")
            raise
    
    def process_batch(self, texts):
        """배치 단위로 텍스트 번역"""
        print(f"zzzzzzzzz:     {texts}")
        if not len(texts):
            return []
        
        start_time = time.time()
        messages_list = []
        
        # 메시지 템플릿 적용
        for text in texts:
            messages = [
                {
                    "role": "user",
                    "content": rf"Translate the following text from Portuguese into Korean.\nPortuguese: {text}\nKorean:"
                }
            ]
            prompt = self.pipe.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            messages_list.append(prompt)
        
        # 예외 처리와 함께 배치 처리
        try:
            outputs = self.pipe(
                messages_list, 
                max_new_tokens=512, 
                do_sample=False,
                batch_size=len(messages_list)  # 명시적으로 배치 크기 지정
            )
            
            results = []
            print('================================')
            print(outputs)
            for output in outputs:
                result = output[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()
                results.append(result)
            
            elapsed_time = time.time() - start_time
            logger.info(f"배치 처리 완료: {len(texts)}개 텍스트, 소요 시간: {elapsed_time:.2f}초")
            
            return results
        except RuntimeError as e:
            # CUDA 메모리 부족 등의 에러 처리
            if "CUDA out of memory" in str(e):
                logger.error(f"GPU 메모리 부족 에러: {e}")
                # 배치 크기를 줄여서 재시도
                if len(texts) > 1:
                    logger.info(f"배치 크기를 {len(texts)//2}로 줄여서 재시도합니다.")
                    half = len(texts) // 2
                    first_half = self.process_batch(texts[:half])
                    second_half = self.process_batch(texts[half:])
                    return first_half + second_half
                else:
                    logger.error("단일 항목 처리 중 메모리 부족. 건너뜁니다.")
                    return ["[번역 실패: 메모리 부족]"]
            else:
                logger.error(f"예상치 못한 에러: {e}")
                return ["[번역 실패]"] * len(texts)

def save_batch(dst, origins, translations):
    """번역 결과를 파일에 저장"""
    try:
        # 디렉토리 확인 및 생성
        os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
        
        with open(dst, 'a', encoding='utf-8') as f:
            for origin, trans in zip(origins, translations):
                f.write(f"{origin} ===> {trans}\n")
        return True
    except Exception as e:
        logger.error(f"저장 중 에러 발생: {e}")
        return False

def get_optimal_batch_size(device):
    """디바이스에 따른 최적의 배치 크기 반환"""
    if device == 'cpu':
        return 2  # CPU에서는 작은 배치 크기 사용
    else:
        try:
            # GPU 메모리 크기에 따라 배치 크기 조정
            gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)  # GB 단위
            if gpu_mem > 16:
                return 8
            elif gpu_mem > 8:
                return 4
            else:
                return 2
        except:
            return 4  # 기본값

def main():
    # 진행 상태 파일 설정
    progress_file = "translation_progress.txt"
    
    try:
        # 환경 설정
        if socket.gethostname() == 'desktop':
            device = 'cpu'
            csv_path = "./eda/review/artifact/reviews_preprocessed.csv"
            target_column = 'preprocessed_comment'
            dst_path = "./translate_desktop_cpu_batch/translated_comment.txt"
        else:
            device = 'cpu'  # GPU가 있으면 CPU 사용
            csv_path = "./eda/review/artifact/reviews_preprocessed.csv"
            target_column = 'preprocessed_title'
            # dst_path = "./translate/translated_title.txt"
            dst_path = "./translate_desktop_cpu_batch/translated_comment.txt"

        
        # 최적의 배치 크기 결정
        BATCH_SIZE = get_optimal_batch_size(device)
        logger.info(f"사용 디바이스: {device}, 배치 크기: {BATCH_SIZE}")
        
        # 데이터 로드
        try:
            df = pd.read_csv(csv_path, index_col=0)
            target_list = df[target_column].dropna().unique()
            logger.info(f"데이터 로딩 완료: {len(target_list)}개 항목")
        except Exception as e:
            logger.error(f"데이터 로딩 실패: {e}")
            return
        
        # 이전 진행 상태 확인
        last_processed = 0
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                try:
                    last_processed = int(f.read().strip())
                    logger.info(f"이전 진행 상태 발견: {last_processed}/{len(target_list)}")
                except:
                    pass
        
        # 번역기 초기화
        translator = BatchTranslator(device=device, batch_size=BATCH_SIZE)
        
        # 배치 처리
        for i in tqdm(range(last_processed, len(target_list), BATCH_SIZE), 
                      desc="번역 진행률", ncols=70, initial=last_processed//BATCH_SIZE, 
                      total=len(target_list)//BATCH_SIZE):
            batch_texts = target_list[i:i+BATCH_SIZE]
            print(f"zzzzzzzz {batch_texts}")
            translations = translator.process_batch(batch_texts)
            
            if save_batch(dst_path, batch_texts, translations):
                # 진행 상태 저장
                with open(progress_file, 'w') as f:
                    f.write(str(i + len(batch_texts)))
            
            # 메모리 관리
            if i % (BATCH_SIZE * 10) == 0 and i > 0:
                if device != 'cpu':
                    torch.cuda.empty_cache()
                    logger.info("GPU 캐시 정리")
    
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"예상치 못한 에러: {e}")
        raise

if __name__ == "__main__":
    main()