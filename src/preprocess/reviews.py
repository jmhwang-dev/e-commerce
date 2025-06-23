import pandas as pd
from utils import *
from pipelines import *
from pathlib import Path
import re
import emoji
import pandas as pd
import os

def remove_emoji(text):
    if not isinstance(text, str):
        return text
    text = emoji.replace_emoji(text, replace=' ')
    return text

def convert_emphasis_to_single_quote(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[`´"“”‘’\'*^]', "'", text)

def replace_exclamation_with_dot(text):
    if not isinstance(text, str):
        return text
    return text.replace('!', '.')

def remove_trailing_punctuation(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[.,]+$', '', text)

def reduce_repeated_special_chars(text):
    if not isinstance(text, str):
        return text
    # 특수문자가 연속될 경우 하나로 줄임
    return re.sub(r'([^\w\s])\1+', r'\1', text)

def reduce_repeated_spaces(text):
    if not isinstance(text, str):
        return text
    # 공백이 여러 개일 경우 하나로 줄임
    text = re.sub(r'\s+', ' ', text)
    return text.strip()  # 양쪽 여백 제거

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
    # 같은 문자가 3번 이상 반복되면 하나로 치환
    return re.sub(r'(.)\1{2,}', r'\1', text)


def strip_spaces_inside_quotes(text):
    if not isinstance(text, str):
        return text
    
    def replacer(m):
        return f"'{m.group(1).strip()}'"
    
    return re.sub(r"'(.*?)'", replacer, text, flags=re.DOTALL)

def remove_if_single_char(text):
    if not isinstance(text, str):
        return text
    if len(text) == 1:
        return ''
    return text

def remove_empty_or_whitespace_single_quotes(text):
    if not isinstance(text, str):
        return text
    return re.sub(r"'\s*'", '', text)

def clean_review_comment(df:pd.DataFrame, target_col) -> pd.DataFrame:
    # 668fcfc39b15a8751fd9f9c3721c6533
    df[target_col] = df[target_col].str.lower()
    df[target_col] = df[target_col].str.replace(r'[\r\n]', ' ', regex=True)
    df[target_col] = df[target_col].apply(remove_emoji)

    df[target_col] = df[target_col].apply(convert_emphasis_to_single_quote)
    df[target_col] = df[target_col].apply(remove_empty_or_whitespace_single_quotes)
    df[target_col] = df[target_col].apply(remove_single_quote)

    df[target_col] = df[target_col].apply(reduce_repeated_special_chars)
    df[target_col] = df[target_col].apply(replace_exclamation_with_dot)
    df[target_col] = df[target_col].apply(remove_space_before_punctuation)

    df[target_col] = df[target_col].apply(reduce_repeated_chars)
    df[target_col] = df[target_col].apply(reduce_repeated_spaces)
    df[target_col] = df[target_col].apply(remove_trailing_punctuation)
    
    df[target_col] = df[target_col].apply(remove_if_single_char)
    df[target_col] = df[target_col].apply(strip_spaces_inside_quotes)
    
    df[target_col] = df[target_col].str.strip()
    df = df[df[target_col].notna() & (df[target_col].str.strip() != '')]
    df = df.reset_index(drop=True)
    df.loc[:, 'type'] = target_col
    df.rename(columns={target_col: 'clean_review_comment'}, inplace=True)

    return df
    

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