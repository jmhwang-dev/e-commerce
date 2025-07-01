"""
`poetry run pytest`
"""
import pytest
from pathlib import Path
import yaml
import shutil

from ecommerce.utils.config import PreprocessConfig, TranslatePipelineConfig
from ecommerce.utils.paths import ensure_directories

ARTIFACT_PATH = Path("./tests/tmp_config_test/")
ARTIFACT_PATH.mkdir(parents=True, exist_ok=True)

def test_preprocess_config_save_and_load():
    dst = ARTIFACT_PATH / "sample_output.csv"

    config = PreprocessConfig(
        src_path="dummy/input.csv",
        dst_path=str(dst),
        inplace=True
    )
    config.save()

    assert config.config_save_path.exists()

    # Load back
    loaded = PreprocessConfig.load(config.config_save_path)
    assert loaded.src_path == "dummy/input.csv"
    assert loaded.dst_path == str(dst)

def test_translate_config_save_and_load():
    dst = ARTIFACT_PATH / "translated.csv"

    config = TranslatePipelineConfig(
        src_path="data.csv",
        dst_path=str(dst),
        dataset_start_index=0,
        dataset_end_index=10,
        checkpoint="test/model",
        device="cpu",
        initial_batch_size=2,
        language_from="English",
        language_into="Korean",
        inplace=True
    )
    config.save()

    assert config.config_save_path.exists()

    with open(config.config_save_path) as f:
        content = yaml.safe_load(f)
        assert content["language_from"] == "English"
        assert content["language_into"] == "Korean"

    loaded = TranslatePipelineConfig.load(config.config_save_path)
    assert loaded.language_from == "English"
    assert loaded.language_into == "Korean"

@pytest.fixture(scope="session", autouse=True)
def setup_dirs():
    ensure_directories()

@pytest.fixture(scope="session", autouse=True)
def cleanup_tmp_config_dir():
    yield
    if ARTIFACT_PATH.exists():
        shutil.rmtree(ARTIFACT_PATH)
