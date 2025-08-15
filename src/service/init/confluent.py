import os
import logging
import json
from typing import Tuple
from functools import wraps
from dotenv import load_dotenv
from typing import Optional, Callable, Any
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

# 로거 인스턴스만 가져옵니다.
# 설정(basicConfig)은 이 파일을 import하는 실행 스크립트에서 담당합니다.
logger: logging.Logger = logging.getLogger(__name__)

def _handle_sr_errors(func: Callable) -> Callable:
    """SchemaRegistryError를 처리하고 클라이언트를 리셋하는 데코레이터."""
    @wraps(func)
    def wrapper(cls: 'SchemaRegistryManager', *args: Any, **kwargs: Any) -> Any:
        try:
            return func(cls, *args, **kwargs)
        except SchemaRegistryError as e:
            logger.error(f"Schema Registry 작업 실패 (함수: {func.__name__}): {e}")
            cls._reset_client()
            raise
    return wrapper

class SchemaRegistryManager:
    """Confluent Schema Registry와의 모든 상호작용을 관리하는 유틸리티 클래스."""

    load_dotenv('./configs/kafka/.env')
    SCHEMA_REGISTRY_URL: str = os.environ.get("SCHEMA_REGISTRY_INTERNAL_URL", "http://schema-registry:8081")
    _client: Optional[SchemaRegistryClient] = None

    @classmethod
    def _get_client(cls, use_internal=True) -> SchemaRegistryClient:
        """SchemaRegistryClient 인스턴스를 지연 초기화하여 반환합니다 (싱글턴 패턴)."""
        if not use_internal:
            cls.SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_EXTERNAL_URL", "http://localhost:8082")

        if cls._client is None:
            cls._client = SchemaRegistryClient({"url": cls.SCHEMA_REGISTRY_URL})
            logger.debug(f"SchemaRegistryClient 초기화 완료: {cls.SCHEMA_REGISTRY_URL}")
        return cls._client

    @classmethod
    def _reset_client(cls) -> None:
        """네트워크 문제 등 발생 시 클라이언트 연결을 리셋합니다."""
        cls._client = None
        logger.warning("SchemaRegistryClient가 리셋되었습니다. 다음 호출 시 재연결됩니다.")

    @classmethod
    @_handle_sr_errors
    def set_compatibility(cls, subject_name: str, level: str = "BACKWARD") -> None:
        """주제(Subject)의 호환성 레벨을 설정합니다."""
        client = cls._get_client(use_internal=False)
        client.set_compatibility(subject_name, level)
        logger.info(f"'{subject_name}' 주제의 호환성을 '{level}'로 설정했습니다.")

    @classmethod
    @_handle_sr_errors
    def register_schema(cls, subject_name: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """
        새로운 스키마를 주제에 등록합니다.
        스키마가 이미 존재하면 기존 ID를 반환하고, 없으면 새로 등록합니다.
        """
        client = cls._get_client()
        schema = Schema(schema_str, schema_type=schema_type)

        try:
            registered_schema = client.lookup_schema(subject_name, schema)
            logger.info(
                f"스키마가 '{subject_name}'에 이미 존재합니다. "
                f"(ID: {registered_schema.schema_id}, Version: {registered_schema.version})"
            )
            return registered_schema.schema_id
        except SchemaRegistryError as e:
            if "not found" not in str(e).lower():
                raise

        schema_id = client.register_schema(subject_name=subject_name, schema=schema)
        logger.info(f"새로운 스키마를 '{subject_name}'에 등록했습니다. (ID: {schema_id})")
        return schema_id
    
    @classmethod
    @_handle_sr_errors
    def delete_schema_version(cls, subject_name: str, version: int) -> list[int]:
        """
        주제(Subject)의 특정 버전 스키마를 삭제합니다. (Soft Delete)
        
        :param subject_name: 삭제할 스키마가 속한 주제
        :param version: 삭제할 버전 번호
        :return: 삭제 후 남아있는 버전들의 리스트
        """
        client = cls._get_client()
        logger.warning(f"'{subject_name}' 주제의 '{version}' 버전을 삭제합니다.")
        # Confluent 라이브러리는 삭제된 버전 번호를 반환하지만, 여기서는 남아있는 버전을 조회하는 것이 더 명확할 수 있습니다.
        # 실제 delete_version 호출은 삭제된 버전 번호 하나만 반환합니다.
        deleted_version = client.delete_version(subject_name, version)
        logger.info(f"'{subject_name}' 주제에서 '{deleted_version}' 버전이 삭제되었습니다.")
        # 남아있는 버전을 확인하려면 별도 조회가 필요
        versions = client.get_versions(subject_name)
        return versions


    @classmethod
    @_handle_sr_errors
    def delete_subject(cls, subject_name: str) -> list[int]:
        """
        주제(Subject)와 관련된 모든 버전의 스키마를 영구적으로 삭제합니다. (Hard Delete)
        !!주의!! 이 작업은 되돌릴 수 없으며 관련 데이터를 읽지 못하게 할 수 있습니다.
        
        :param subject_name: 삭제할 주제
        :return: 삭제된 버전들의 리스트
        """
        client = cls._get_client()
        logger.critical(f"'{subject_name}' 주제의 모든 스키마를 영구적으로 삭제합니다. 이 작업은 되돌릴 수 없습니다.")

        # 사용자 확인 절차
        # proceed = input(f"정말로 '{subject_name}' 주제를 삭제하시겠습니까? [y/N]: ")
        # if proceed.lower() != 'y':
        #     logger.info("삭제 작업을 취소했습니다.")
        #     return []
        
        deleted_versions = client.delete_subject(subject_name, permanent=True)
        logger.info(f"'{subject_name}' 주제가 영구적으로 삭제되었습니다. 삭제된 버전: {deleted_versions}")
        return deleted_versions
    
    @staticmethod
    def get_schem_identifier(schema_str) -> Tuple[str, str]:
        schema_dict = json.loads(schema_str)
        namespace = schema_dict.get('namespace')
        table_name = schema_dict.get('name')
        return namespace, table_name