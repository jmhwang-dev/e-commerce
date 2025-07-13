# environment:
#       - SUPERSET_CONFIG_PATH=/app/superset_config.py
#       # - SUPERSET_SECRET_KEY=super_secret_please_change
#     volumes:
#       - "./superset/superset_config.py:/app/superset_config.py"

# superset_config.py
import os

# PostgreSQL 연결 설정
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset_user:superset_pw@postgres:5432/superset'

# 시크릿 키 설정
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'super_secret_please_change')

# # 추가 설정들
# MAPBOX_API_KEY = os.environ.get('MAPBOX_API_KEY', '')
# CSRF_ENABLED = True
# WTF_CSRF_ENABLED = True

# # 개발 모드 설정 (운영환경에서는 False로 설정)
# DEBUG = True

# # 웹 서버 설정
# WEBSERVER_THREADS = 8
# WEBSERVER_TIMEOUT = 60

# # 캐시 설정
# CACHE_CONFIG = {
#     'CACHE_TYPE': 'simple',
# }

# # 기타 설정
# ENABLE_PROXY_FIX = True
# PUBLIC_ROLE_LIKE_GAMMA = True