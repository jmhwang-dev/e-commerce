from inference import *
from config import *

if __name__ == "__main__":
    translator_p2e = get_translator_p2e()
    translator_e2k = get_translator_e2k()
    analyzer_eng = get_sentiment_analyzer()

    dataset_config = load_config('preprocess', 'config_all_portuguess.yml')
    dataset = load_dataset(dataset_config['dst_path'])