from .pipeline import *

def get_translator_p2e() -> Translator:
    config_p2e = TranslatePipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name="trans_p2e.txt",
        
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device='auto',
        batch_size=8,
        dynamic_batch_size_increment=1,
        dynamic_batch_size_decrement=1,

        language_from='Portuguese',
        language_into="English"
    )
    config_p2e.save()
    return Translator(config_p2e)

def get_translator_e2k() -> Translator:
    config_e2p = TranslatePipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name="trans_e2p.txt",
        
        checkpoint="Unbabel/TowerInstruct-7B-v0.2",
        device='cpu',
        batch_size=8,
        dynamic_batch_size_increment=1,
        dynamic_batch_size_decrement=1,

        language_from='English',
        language_into="Korean"
    )
    config_e2p.save()
    return Translator(config_e2p)

def get_sentiment_analyzer():
    config_senti = PipelineConfig(
        src_path=None,
        dst_dir_name='inference',
        dst_file_name="senti_eng.txt",
        
        checkpoint="j-hartmann/sentiment-roberta-large-english-3-classes",
        device='cpu',
    )
    config_senti.save()
    return  SentimentAnalyzer(config_senti)