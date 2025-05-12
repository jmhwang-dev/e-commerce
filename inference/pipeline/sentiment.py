from inference.pipeline.base import *
from config import *

class SentimentAnalyzer(BasePipeline):
    def __init__(self, config:TranslatePipelineConfig):
        self.config = config
        self.batch_size = self.config.initial_batch_size

        self.pipeline = pipeline(
            "text-classification",
            model=self.config.checkpoint,
            device=self.config.device,
            top_k=3,

            max_length=512,
            truncation=True,
            )

    def set_input(self, dataset:List[str]):
        self.prompts = dataset

    def run(self,):
        start_index = 0
        total = len(self.prompts)

        while start_index < total:
            end_index = min(start_index + self.batch_size, total)
            chunk = self.prompts[start_index:end_index]
            try:
                start_time = time.time()
                outputs = self.pipeline(chunk, batch_size=len(chunk))
                duration = time.time() - start_time
                print(f"[{self.config.device}] Processing batch: {end_index}/{total} - Time: {duration:.2f}s")
                self.save_results(outputs)
                start_index = end_index
                self.adjust_batch_size(+1)

            except torch.cuda.OutOfMemoryError:
                print(f"[{self.config.device}] ⚠️ Out of Memory. Reducing batch size.")
                self.adjust_batch_size(-1)
                torch.cuda.empty_cache()
                gc.collect()

    def adjust_batch_size(self, delta: int):
        if self.config.device == 'auto':
            new_size = max(1, self.batch_size + delta)
        else:
            # Limit maximum batch size for CPU inference
            new_size = min(1000, self.batch_size + delta)
        print(f"[{self.config.device}] Batch size {'increased' if delta > 0 else 'decreased'}: {self.batch_size} → {new_size}")
        self.batch_size = new_size

    def save_results(self, outputs):
        rows = [{item['label']: item['score'] for item in row} for row in outputs]
        df_new = pd.DataFrame(rows)

        try:
            existing_df = pd.read_csv(self.config.dst_path)
            df = pd.concat([existing_df, df_new], ignore_index=True)
        except FileNotFoundError:
            df = df_new
        finally:
            df.to_csv(self.config.dst_path, index=False)