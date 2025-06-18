from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Type, TypeVar
import yaml
import os

from common.paths import ARTIFACT_INFERENCE_PREPROCESS_DIR, ARTIFACT_INFERENCE_RESULT_DIR

T = TypeVar("T", bound="BaseConfig")

@dataclass(kw_only=True)
class BaseConfig(ABC):
    src: str
    dst: str
    inplace: bool = False

    @property
    @abstractmethod
    def save_path(self) -> Path:
        pass

    @property
    @abstractmethod
    def config_dict(self) -> dict:
        pass

    def save(self):
        if self.save_path.exists():
            with open(self.save_path, 'r') as f:
                existing_config = yaml.safe_load(f)

            if existing_config == self.config_dict:
                print(f"[SKIP] Identical config already exists at {self.save_path}")
                return

            if not self.inplace:
                raise FileExistsError(
                    f"[ABORT] A different config already exists at {self.save_path}. "
                    f"Use `inplace=True` to overwrite."
                )

        self.save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.save_path, 'w') as f:
            yaml.dump(self.config_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print(f"[OK] Configuration saved at {self.save_path}")

    def __post_init__(self):
        if not self.inplace and Path(self.dst).exists():
            raise FileExistsError(f"{self.dst} already exists.")

    @classmethod
    def load(cls: Type[T], path: Union[str, Path]) -> T:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"{path} not found")

        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)

        return cls(**config_dict)

@dataclass(kw_only=True)
class PreprocessConfig(BaseConfig):
    _save_path: Path = field(init=False)
    _config_dict: dict = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        stem = Path(self.dst).stem
        self._save_path = Path(ARTIFACT_INFERENCE_PREPROCESS_DIR) / f"config_{stem}.yml"
        self._config_dict = {
            "src": self.src,
            "dst": self.dst
        }

    @property
    def save_path(self) -> Path:
        return self._save_path

    @property
    def config_dict(self) -> dict:
        return self._config_dict

@dataclass(kw_only=True)
class PipelineConfig(BaseConfig):
    checkpoint: str
    device: str = "cpu"
    initial_batch_size: int

    _save_path: Path = field(init=False)
    _config_dict: dict = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        if self.device not in ("auto", "cpu", "cuda"):
            raise ValueError("device should be one of 'auto', 'cpu', or 'cuda'.")

        stem = Path(self.src).stem
        self._save_path = Path(ARTIFACT_INFERENCE_RESULT_DIR) / f"config_{stem}.yml"

        self._config_dict = {
            "src": self.src,
            "dst": self.dst,
            "checkpoint": self.checkpoint,
            "device": self.device,
            "initial_batch_size": self.initial_batch_size
        }

    @property
    def save_path(self) -> Path:
        return self._save_path

    @property
    def config_dict(self) -> dict:
        return self._config_dict

@dataclass(kw_only=True)
class TranslatePipelineConfig(PipelineConfig):
    language_from: str
    language_into: str

    def __post_init__(self):
        super().__post_init__()
        self._config_dict.update({
            "language_from": self.language_from,
            "language_into": self.language_into
        })
