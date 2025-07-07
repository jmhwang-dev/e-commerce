# 스크립트 실행 권한 부여
chmod +x notebook_to_script.sh

# 기본 사용법
./notebook_to_script.sh test.ipynb

# 출력 파일명 지정
./notebook_to_script.sh test.ipynb my_script.py

# 깔끔한 출력 (주석 정리)
./notebook_to_script.sh test.ipynb --clean

# 상세한 출력과 함께
./notebook_to_script.sh test.ipynb --verbose --clean