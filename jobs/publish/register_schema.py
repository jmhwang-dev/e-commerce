import json
import logging
from pathlib import Path
from service.init.confluent import SchemaRegistryManager

# 이 스크립트의 로거 인스턴스를 가져옵니다.
logger = logging.getLogger(__name__)

# 프로젝트 루트를 기준으로 스키마 디렉토리 경로 설정
SCHEMA_DIR = Path('infra/confluent/schemas/')

def setup_logging():
    """스크립트의 로깅 설정을 구성합니다."""
    # 모든 로거의 기본 레벨을 DEBUG로 설정하여 모든 메시지를 핸들러로 전달
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # 1. 콘솔 핸들러: 간결한 포맷으로 INFO 레벨 이상의 로그만 출력
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)

    # 2. 파일 핸들러: 상세한 포맷으로 DEBUG 레벨 이상의 모든 로그를 파일에 기록
    Path("logs").mkdir(exist_ok=True)
    file_handler = logging.FileHandler("logs/schema_registration.log", mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # 설정된 핸들러들을 루트 로거에 추가
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

def process_schema_file(schema_path: Path):
    """단일 스키마 파일을 읽고 Schema Registry에 등록합니다."""
    logger.info(f"--- 처리 시작: {schema_path.relative_to(SCHEMA_DIR)} ---")
    try:
        schema_str = schema_path.read_text(encoding="utf-8")
        schema_json = json.loads(schema_str)
        subject_name = schema_json.get("name")

        # SchemaRegistryManager.delete_subject(subject_name)

        if not subject_name:
            logger.warning(f"'{schema_path.name}' 파일에 'name' 필드가 없어 건너뜁니다.")
            return

        logger.info(f"주제: '{subject_name}' 확인")
        SchemaRegistryManager.set_compatibility(subject_name)
        SchemaRegistryManager.register_schema(subject_name, schema_str)

    except FileNotFoundError:
        logger.error(f"파일을 찾을 수 없습니다: {schema_path}")
    except json.JSONDecodeError:
        logger.error(f"'{schema_path.name}' 파일의 JSON 형식이 올바르지 않습니다.")
    except Exception as e:
        logger.error(f"'{schema_path.name}' 처리 중 예상치 못한 오류 발생", exc_info=True)

def main():
    """메인 실행 함수: 스키마 디렉토리를 순회하며 모든 .avsc 파일을 등록합니다."""
    setup_logging()  # 로깅 설정 실행

    logger.info("=" * 40)
    logger.info("Schema Registry 스크립트 시작")
    logger.info(f"대상 디렉토리: {SCHEMA_DIR.absolute()}")
    logger.info("=" * 40)

    if not SCHEMA_DIR.is_dir():
        logger.critical(f"스키마 디렉토리 '{SCHEMA_DIR}'를 찾을 수 없어 스크립트를 종료합니다.")
        return

    for tier_dir in sorted(SCHEMA_DIR.iterdir()):
        if not tier_dir.is_dir():
            continue

        logger.info(f"[{tier_dir.name.upper()} 계층 스키마 처리 시작]")
        schema_files = sorted(tier_dir.glob('*.avsc'))
        
        if not schema_files:
            logger.info(f"'{tier_dir.name}'에 등록할 스키마 파일(.avsc)이 없습니다.")
            continue
            
        for schema_path in schema_files:
            process_schema_file(schema_path)

    logger.info("=" * 40)
    logger.info("모든 스키마 처리가 완료되었습니다.")
    logger.info("=" * 40)

if __name__ == "__main__":
    main()