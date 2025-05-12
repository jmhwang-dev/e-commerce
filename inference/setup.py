from config import *
from .pipeline.translate import Translator
from .pipeline.sentiment import SentimentAnalyzer

def get_translator_p2e(device_, initial_batch_size, dst_file_name) -> Translator:
    config_p2e = TranslatePipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name=dst_file_name,
        
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device=device_,
        initial_batch_size=initial_batch_size,

        language_from='Portuguese',
        language_into="English",
        inplace=True
    )
    config_p2e.save()
    return Translator(config_p2e)

def get_translator_e2k(device_, initial_batch_size, dst_file_name) -> Translator:
    config_e2k = TranslatePipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name=dst_file_name,
        
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device=device_,
        initial_batch_size=initial_batch_size,

        language_from='English',
        language_into="Korean",
        inplace=True
    )
    config_e2k.save()
    return Translator(config_e2k)

def get_sentiment_analyzer(device_, initial_batch_size_, dst_file_name_) -> SentimentAnalyzer:
    config_senti = PipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name=dst_file_name_,
        
        checkpoint="j-hartmann/sentiment-roberta-large-english-3-classes",

        device=device_,
        initial_batch_size=initial_batch_size_,

        inplace=True
    )
    config_senti.save()
    return SentimentAnalyzer(config_senti)