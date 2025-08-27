from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re
import emoji

# --------------------
# 텍스트 처리 함수들
# --------------------

def replace_emoji_with_dot(text):
    if text is None:
        return text
    return emoji.replace_emoji(text, replace='.')

def replace_emoticon_with_dot(text):
    if text is None:
        return text
    pattern = r'(:\)|;\)|:-\)|:\(|:\\|:/|\^_+\^|\*__*\*)'
    def replacer(match):
        matched = match.group(0)
        if matched == ':/':
            start = match.start()
            context = text[max(0, start - 8):start].lower()
            if 'http' in context and context.endswith('http'):
                return matched
            else:
                return '.'
        else:
            return '.'
    return re.sub(pattern, replacer, text)

def convert_emphasis_to_single_quote(text):
    if text is None:
        return text
    return re.sub(r'[`´"“”‘’\'*^]', "'", text)

def replace_exclamation_with_dot(text):
    if text is None:
        return text
    return text.replace('!', '.')

def remove_trailing_punctuation(text):
    if text is None:
        return text
    return re.sub(r'[.,+]+$', '', text)

def reduce_repeated_special_chars(text):
    if text is None:
        return text
    def replacer(match):
        s = match.group(0)
        if s.startswith('//'):
            start = match.start()
            if start >= 6 and text[start-6:start].endswith('https:'):
                return s
            if start >= 5 and text[start-5:start].endswith('http:'):
                return s
        return s[0]
    return re.sub(r'([^\w\s])\1+', replacer, text)

def reduce_repeated_spaces(text):
    if text is None:
        return text
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def remove_space_before_punctuation(text):
    if text is None:
        return text
    return re.sub(r'\s+([!?.,])', r'\1', text)

def remove_single_quote(text):
    if text is None:
        return text
    if text.count("'") == 1:
        return text.replace("'", "")
    return text

def reduce_repeated_chars(text):
    if text is None:
        return text
    return re.sub(r'(.)\1{2,}', r'\1', text)

def strip_spaces_inside_quotes(text):
    if text is None:
        return text
    def replacer(m):
        return f"'{m.group(1).strip()}'"
    return re.sub(r"'(.*?)'", replacer, text, flags=re.DOTALL)

def remove_if_short(text):
    if text is None:
        return text
    if len(text) <= 2:
        return ''
    return text

def remove_empty_or_whitespace_single_quotes(text):
    if text is None:
        return text
    return re.sub(r"'\s*'", '', text)


class PortuguessPreprocessor:

    @staticmethod
    def melt_reviews(df: DataFrame) -> DataFrame:
        # Spark 3.5+ melt API
        if 'review_id' not in df.columns:
            raise ValueError("Column 'review_id' must exist in dataset")
        return df.melt(
            ids=["review_id"],
            values=["review_comment_title", "review_comment_message"],
            variableColumnName="message_type",   # 여기는 실제 컬럼명 의미에 맞게 수정
            valueColumnName="portuguess"
        ).na.drop()

    @staticmethod
    def clean_review_comment(df: DataFrame, value_column_name: str = 'portuguess') -> DataFrame:
        # 문자열 처리용 UDF 등록
        udf_replace_emoji = F.udf(replace_emoji_with_dot)
        udf_replace_emoticon = F.udf(replace_emoticon_with_dot)
        udf_convert_emphasis = F.udf(convert_emphasis_to_single_quote)
        udf_remove_empty_quotes = F.udf(remove_empty_or_whitespace_single_quotes)
        udf_remove_single_quote = F.udf(remove_single_quote)
        udf_reduce_special = F.udf(reduce_repeated_special_chars)
        udf_replace_exclamation = F.udf(replace_exclamation_with_dot)
        udf_remove_space_before_punct = F.udf(remove_space_before_punctuation)
        udf_reduce_chars = F.udf(reduce_repeated_chars)
        udf_reduce_spaces = F.udf(reduce_repeated_spaces)
        udf_remove_trailing_punct = F.udf(remove_trailing_punctuation)
        udf_remove_if_short = F.udf(remove_if_short)
        udf_strip_spaces_inside_quotes = F.udf(strip_spaces_inside_quotes)

        col_expr = F.col(value_column_name)
        col_expr = F.lower(col_expr)
        col_expr = F.regexp_replace(col_expr, r'[\r\n]', ' ')
        col_expr = udf_replace_emoji(col_expr)
        col_expr = udf_replace_emoticon(col_expr)
        col_expr = udf_convert_emphasis(col_expr)
        col_expr = udf_remove_empty_quotes(col_expr)
        col_expr = udf_remove_single_quote(col_expr)
        col_expr = udf_reduce_special(col_expr)
        col_expr = udf_replace_exclamation(col_expr)
        col_expr = udf_remove_space_before_punct(col_expr)
        col_expr = udf_reduce_chars(col_expr)
        col_expr = udf_reduce_spaces(col_expr)
        col_expr = udf_remove_trailing_punct(col_expr)
        col_expr = udf_remove_if_short(col_expr)
        col_expr = udf_strip_spaces_inside_quotes(col_expr)

        df = df.withColumn(value_column_name, col_expr)
        df = df.filter(
            (F.col(value_column_name).isNotNull()) &
            (F.trim(F.col(value_column_name)) != "")
        ).dropDuplicates()
        return df
    
def get_review_metadata(decoded_stream_df: DataFrame):
    return decoded_stream_df.select(
        'review_id',
        'order_id',
        'review_score',
        'review_creation_date',
        'review_answer_timestamp',
    )