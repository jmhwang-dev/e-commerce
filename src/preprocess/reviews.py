import pandas as pd
from utils.config import *
from utils.paths import *
from utils.loader import *
from pipelines import *
from pathlib import Path
import re
import emoji
import pandas as pd
import os
import csv

def remove_emoji(text):
    if not isinstance(text, str):
        return text
    text = emoji.replace_emoji(text, replace=' ')
    return text

def strip_edge_quotes(text):
    if not isinstance(text, str):
        return text
    return text.strip('"\'' + '"' + '"')  # 양쪽 끝에 있는 일반/홑/쌍따옴표 모두 제거

def clean_quotes(text):
    if not isinstance(text, str):
        return text
    
    # 먼저 양끝 큰따옴표 제거
    text = text.strip('"')
    
    # 연속된 큰따옴표를 하나로 변환 (공백 포함/미포함 모두 처리)
    text = re.sub(r'"\s*"+', '"', text)  # "" 또는 " " 등을 "로 변환
    text = re.sub(r'"+', '"', text)      # 남은 연속 따옴표 처리
    
    return text

def convert_all_quotes_to_single(text):
    if not isinstance(text, str):
        return text
    # 모든 종류의 따옴표를 작은따옴표(')로 치환
    return re.sub(r'[\"“”\']', "'", text)



def reduce_repeated_spaces(text):
    if not isinstance(text, str):
        return text
    # 공백이 여러 개일 경우 하나로 줄임
    text = re.sub(r'\s+', ' ', text)
    return text.strip()  # 양쪽 여백 제거

def remove_leading_special_except_quote_and_portuguese(text):
    if not isinstance(text, str):
        return text
    # 포르투갈어 특수문자(라틴1 보충) 포함해서 허용 문자 지정
    allowed_chars = r"a-zA-Z0-9áàâãéèêíïóôõöúçñÁÀÂÃÉÈÍÏÓÔÕÖÚÇÑ\s'"
    pattern = rf"^[^{allowed_chars}]+"
    return re.sub(pattern, "", text)

def replace_exclamation_with_dot(text):
    if not isinstance(text, str):
        return text
    return text.replace('!', '.')

def remove_trailing_special_except_question_and_quote(text):
    if not isinstance(text, str):
        return text
    
    # 문장 끝에서 괄호 ')', 물음표(?)와 따옴표(", ')를 제외한 특수문자 1개 이상 연속 제거
    return re.sub(r'[^a-zA-Z0-9\s\?"\')"]+$', '', text)

def reduce_repeated_special_chars(text):
    if not isinstance(text, str):
        return text
    # 특수문자가 연속될 경우 하나로 줄임
    return re.sub(r'([^\w\s])\1+', r'\1', text)

def remove_space_before_punctuation(text):
    if not isinstance(text, str):
        return text
    # 느낌표, 물음표, 마침표, 쉼표 앞의 공백 제거
    return re.sub(r'\s+([!?.,])', r'\1', text)

def remove_single_quote(text):
    if not isinstance(text, str):
        return text
    
    # 홑따옴표 개수 세기
    count = text.count("'")
    if count == 1:
        # 홑따옴표 1개면 모두 제거
        return text.replace("'", "")
    else:
        # 그 외는 그대로 반환
        return text
    
def reduce_repeated_chars(text):
    if not isinstance(text, str):
        return text
    # 같은 문자가 2번 이상 반복되는 부분을 1개로 치환
    return re.sub(r'(.)\1+', r'\1', text)
    
##################
def remove_special_chars_except_selected(text):
    if not isinstance(text, str):
        return text
    # 허용된 특수문자와 알파벳, 숫자, 공백을 제외한 나머지 특수문자 제거
    return re.sub(r"[^\w\s\?\!\'.,]", "", text)


def strip_spaces_inside_quotes(text):
    if not isinstance(text, str):
        return text
    
    def replacer(m):
        return f"'{m.group(1).strip()}'"
    
    return re.sub(r"'(.*?)'", replacer, text, flags=re.DOTALL)

def clean_portuguese_text(text):
    if not isinstance(text, str):
        return text
    # 1) 허용 문자 외 모두 제거: 알파벳, 숫자, 공백, 특수문자("?, !, ', ., ,")
    text = re.sub(r"[^\w\s\?\!\'.,]", "", text, flags=re.UNICODE)
    # 2) 언더스코어는 제거 (또는 공백으로 대체)
    text = re.sub(r"_", "", text)
    return text.strip()
##################

def remove_if_single_char(text):
    if not isinstance(text, str):
        return text
    if len(text) == 1:
        return ''
    return text


def clean_text(df:pd.DataFrame, target_col) -> None:
    df[target_col] = df[target_col].str.replace(r'[\r\n]', ' ', regex=True)
    df[target_col] = df[target_col].str.lower()

    df[target_col] = df[target_col].apply(remove_emoji)
    df[target_col] = df[target_col].apply(convert_all_quotes_to_single)
    df[target_col] = df[target_col].apply(remove_single_quote)
    df[target_col] = df[target_col].apply(reduce_repeated_special_chars)
    df[target_col] = df[target_col].apply(reduce_repeated_chars)
    
    df[target_col] = df[target_col].apply(remove_space_before_punctuation)
    df[target_col] = df[target_col].apply(replace_exclamation_with_dot)

    df[target_col] = df[target_col].apply(remove_leading_special_except_quote_and_portuguese)
    df[target_col] = df[target_col].apply(reduce_repeated_spaces)
    df[target_col] = df[target_col].apply(remove_trailing_special_except_question_and_quote)
    df[target_col] = df[target_col].apply(remove_if_single_char)


    df[target_col] = df[target_col].str.strip()

    df = df[df[target_col].notna() & (df[target_col].str.strip() != '')]

    # assign()을 사용하여 임시 컬럼 추가 후 정렬
    df = df.assign(length=df[target_col].str.len()).sort_values(by='length', ascending=False).drop(columns='length')

    save_path = os.path.join(SILVER_DIR, f'{target_col}.tsv')
    # df.to_csv(save_path, sep='\t', index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
    df.to_csv(save_path, sep='\t', index=False)


def extract_text(config: PreprocessConfig):
    reviews_with_content_df = pd.read_csv(config.src_path)

    unique_title = reviews_with_content_df['review_comment_title'].dropna().drop_duplicates()
    unique_message = reviews_with_content_df['review_comment_message'].dropna().drop_duplicates()

    all_portuguese = pd.concat([unique_title, unique_message])
    all_portuguese = all_portuguese.dropna()
    all_portuguese = all_portuguese.sort_values(key=lambda x: x.str.len(), ascending=False)

    # ✅ 결과 저장 경로: 전처리 artifact 디렉토리
    dst_path = Path(PREPROCESS_ARTIFACTS_DIR) / Path(config.dst_path).name
    all_portuguese.to_csv(dst_path, index=False, header=False)

    print(f"Portuguese to translate: {dst_path}")


def gather_results(src_paths: List[Path], dst_prefix: str) -> None:
    gather_config = GatherConfig(
        src_paths=src_paths,
        dst_path=os.path.join(INFERENCE_ARTIFACTS_DIR, f'{dst_prefix}_gather.txt'),
        inplace=True
    )

    gather_config.save()
    
    results = []
    for src_path in gather_config.src_paths:
        texts = load_texts(src_path)
        results += texts
    
    # 저장은 수동으로 (이스케이프 방지)
    with open(gather_config.dst_path, 'w', encoding='utf-8') as f:
        for text_value in results:
            f.write(str(text_value) + '\n')

def run_translator(config: TranslatePipelineConfig, dataset):
    translatore = Translator(config)
    translatore.set_input(dataset)
    translatore.run()