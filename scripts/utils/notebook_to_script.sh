#!/bin/bash

# 노트북을 Python 스크립트로 변환하는 스크립트
# 사용법: ./notebook_to_script.sh <notebook_file> [output_file]

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 사용법 출력 함수
usage() {
    echo -e "${BLUE}사용법:${NC}"
    echo "  $0 <notebook_file> [output_file]"
    echo ""
    echo -e "${BLUE}예시:${NC}"
    echo "  $0 test.ipynb"
    echo "  $0 test.ipynb output.py"
    echo ""
    echo -e "${BLUE}옵션:${NC}"
    echo "  -h, --help    이 도움말을 표시합니다"
    echo "  -c, --clean   출력 파일을 깔끔하게 정리합니다 (주석 제거, 빈 줄 정리)"
    echo "  -v, --verbose 상세한 출력을 표시합니다"
    exit 1
}

# 에러 메시지 출력 함수
error_exit() {
    echo -e "${RED}에러:${NC} $1" >&2
    exit 1
}

# 성공 메시지 출력 함수
success_msg() {
    echo -e "${GREEN}성공:${NC} $1"
}

# 경고 메시지 출력 함수
warning_msg() {
    echo -e "${YELLOW}경고:${NC} $1"
}

# 정보 메시지 출력 함수
info_msg() {
    echo -e "${BLUE}정보:${NC} $1"
}

# 변수 초기화
NOTEBOOK_FILE=""
OUTPUT_FILE=""
CLEAN_OUTPUT=false
VERBOSE=false

# 인자 파싱
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -c|--clean)
            CLEAN_OUTPUT=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        *)
            if [[ -z "$NOTEBOOK_FILE" ]]; then
                NOTEBOOK_FILE="$1"
            elif [[ -z "$OUTPUT_FILE" ]]; then
                OUTPUT_FILE="$1"
            else
                error_exit "너무 많은 인자가 제공되었습니다."
            fi
            shift
            ;;
    esac
done

# 필수 인자 검증
if [[ -z "$NOTEBOOK_FILE" ]]; then
    error_exit "노트북 파일을 지정해주세요."
fi

# 파일 존재 확인
if [[ ! -f "$NOTEBOOK_FILE" ]]; then
    error_exit "파일을 찾을 수 없습니다: $NOTEBOOK_FILE"
fi

# 출력 파일명 생성
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="${NOTEBOOK_FILE%.ipynb}.py"
fi

# 필요한 도구 확인
if ! command -v python3 &> /dev/null; then
    error_exit "python3이 설치되어 있지 않습니다."
fi

# nbconvert 확인 및 설치
if ! python3 -c "import nbformat" &> /dev/null; then
    warning_msg "nbformat이 설치되어 있지 않습니다. 설치를 시도합니다..."
    pip3 install nbformat || error_exit "nbformat 설치에 실패했습니다."
fi

# 변환 함수
convert_notebook() {
    local input_file="$1"
    local output_file="$2"
    
    if [[ "$VERBOSE" == true ]]; then
        info_msg "노트북 변환 시작: $input_file -> $output_file"
    fi
    
    # Python 스크립트로 변환
    python3 - << EOF
import json
import nbformat
import sys

def convert_notebook_to_script(notebook_path, output_path):
    try:
        # 노트북 파일 읽기
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = nbformat.read(f, as_version=4)
        
        # Python 스크립트 생성
        script_lines = []
        script_lines.append("#!/usr/bin/env python3")
        script_lines.append("# -*- coding: utf-8 -*-")
        script_lines.append(f"# Converted from {notebook_path}")
        script_lines.append("")
        
        cell_count = 0
        for cell in notebook.cells:
            if cell.cell_type == 'code':
                cell_count += 1
                script_lines.append(f"# Cell {cell_count}")
                
                # 소스 코드 추가
                source = cell.source.strip()
                if source:
                    script_lines.append(source)
                    script_lines.append("")
            elif cell.cell_type == 'markdown':
                # 마크다운 셀을 주석으로 변환
                markdown_lines = cell.source.split('\n')
                script_lines.append("# Markdown Cell:")
                for line in markdown_lines:
                    script_lines.append(f"# {line}")
                script_lines.append("")
        
        # 파일 쓰기
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(script_lines))
        
        print(f"변환 완료: {cell_count}개 코드 셀 처리됨")
        return True
        
    except Exception as e:
        print(f"변환 실패: {e}")
        return False

# 변환 실행
success = convert_notebook_to_script('$input_file', '$output_file')
sys.exit(0 if success else 1)
EOF
    
    return $?
}

# 출력 파일 정리 함수
clean_output() {
    local file="$1"
    
    if [[ "$VERBOSE" == true ]]; then
        info_msg "출력 파일 정리 중..."
    fi
    
    # 임시 파일 생성
    local temp_file=$(mktemp)
    
    # 파일 정리
    python3 - << EOF
import re

def clean_script(input_path, output_path):
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        lines = content.split('\n')
        cleaned_lines = []
        
        prev_empty = False
        for line in lines:
            # 빈 줄 처리 (연속된 빈 줄 제거)
            if line.strip() == '':
                if not prev_empty:
                    cleaned_lines.append('')
                prev_empty = True
            else:
                # Cell 번호 주석 제거 (선택사항)
                if not line.startswith('# Cell '):
                    cleaned_lines.append(line)
                prev_empty = False
        
        # 파일 끝 정리
        while cleaned_lines and cleaned_lines[-1] == '':
            cleaned_lines.pop()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(cleaned_lines))
            f.write('\n')  # 마지막 개행 추가
        
        return True
        
    except Exception as e:
        print(f"정리 실패: {e}")
        return False

# 정리 실행
success = clean_script('$file', '$temp_file')
if success:
    import shutil
    shutil.move('$temp_file', '$file')
else:
    import os
    os.remove('$temp_file')
EOF
}

# 메인 실행
main() {
    info_msg "노트북 변환 시작..."
    
    # 변환 실행
    if convert_notebook "$NOTEBOOK_FILE" "$OUTPUT_FILE"; then
        success_msg "변환이 완료되었습니다: $OUTPUT_FILE"
        
        # 정리 옵션이 활성화된 경우
        if [[ "$CLEAN_OUTPUT" == true ]]; then
            clean_output "$OUTPUT_FILE"
            success_msg "출력 파일이 정리되었습니다."
        fi
        
        # 파일 권한 설정
        chmod +x "$OUTPUT_FILE"
        
        # 결과 정보 표시
        if [[ "$VERBOSE" == true ]]; then
            info_msg "출력 파일 정보:"
            echo "  - 경로: $OUTPUT_FILE"
            echo "  - 크기: $(wc -l < "$OUTPUT_FILE") 줄"
            echo "  - 권한: $(ls -l "$OUTPUT_FILE" | cut -d' ' -f1)"
        fi
        
        info_msg "변환된 스크립트 실행 방법:"
        echo "  python3 $OUTPUT_FILE"
        echo "  또는"
        echo "  ./$OUTPUT_FILE"
        
    else
        error_exit "변환에 실패했습니다."
    fi
}

# 스크립트 실행
main