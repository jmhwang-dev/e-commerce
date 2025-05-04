from inference import *
from io import *

# # padding과 truncation을 직접 설정
# inputs = self.pipeline.tokenizer(self.prompts, return_tensors='pt', padding=True, truncation=False)
# input_ids = inputs['input_ids']
# bytes_per_token = input_ids.element_size()  # 각 요소의 바이트 크기
# total_bytes = input_ids.numel() * bytes_per_token  # 전체 바이트 수

# print(self.vram_available_for_use)
# print(total_bytes)
# exit()

dataset = load_dataset("./artifact/preprocess/all_portuguess.txt")
translator = get_translator_p2e()
translator.set_input(dataset)
translator.run()
results = translator.get_results()
save_translation(translator.config.dst_path, results)