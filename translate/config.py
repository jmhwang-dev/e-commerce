from dataclasses import dataclass
import os

@dataclass
class Config:
    checkpoint: str = "Unbabel/TowerInstruct-7B-v0.2"
    artifact_path: str = "./translate/artifact"
    batch_size: int = 8
    dynamic_batch_size_increment: int = 1
    dynamic_batch_size_decrement: int = 1
    device: str = "auto"
    dst_path: str = ""
    src_path: str = ""

    def __post_init__(self):
        if self.dynamic_batch_size_decrement < 0:
            raise ValueError("dynamic_batch_size_decrement should be positive number.")
        
        if self.device not in ('auto', 'cpu'):
            raise ValueError("device should be one of 'auto' or 'cpu'.")
        
        if not self.dst_path:
            self.dst_path = os.path.join(self.artifact_path, f"translated_reviews_{self.device}.txt")

        if not self.src_path:
            self.src_path = os.path.join(self.artifact_path, "preprocessed_reviews.csv")
