import os

HOME_DIR = os.path.expanduser('~')
REPOSITORY_DIR = os.path.join(HOME_DIR, 'Documents', 'github', 'jmhwang-dev' ,'e-commerce')

ARTIFACTS_ROOT_DIR = os.path.join(REPOSITORY_DIR, "artifacts")
METADATA_ARTIFACT_DIR = os.path.join(ARTIFACTS_ROOT_DIR, "metadata")
PREPROCESS_ARTIFACTS_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'preprocess')
POSTPROCESS_ARTIFACTS_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'postprocess')
INFERENCE_ARTIFACTS_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'inference')

CONFIGS_ROOT_DIR = os.path.join(REPOSITORY_DIR, "configs")
PREPROCESS_CONFIGS_DIR = os.path.join(CONFIGS_ROOT_DIR, 'preprocess')
POSTPROCESS_CONFIGS_DIR = os.path.join(CONFIGS_ROOT_DIR, 'postprocess')
INFERENCE_CONFIGS_DIR = os.path.join(CONFIGS_ROOT_DIR, 'inference')

MEDALLION_ROOT_DIR = os.path.join(REPOSITORY_DIR, "data")
BRONZE_DIR = os.path.join(MEDALLION_ROOT_DIR, 'bronze')
SILVER_DIR = os.path.join(MEDALLION_ROOT_DIR, 'silver')

os.makedirs(ARTIFACTS_ROOT_DIR, exist_ok=True)
os.makedirs(METADATA_ARTIFACT_DIR, exist_ok=True)

os.makedirs(PREPROCESS_ARTIFACTS_DIR, exist_ok=True)
os.makedirs(POSTPROCESS_ARTIFACTS_DIR, exist_ok=True)
os.makedirs(INFERENCE_ARTIFACTS_DIR, exist_ok=True)

os.makedirs(CONFIGS_ROOT_DIR, exist_ok=True)
os.makedirs(PREPROCESS_CONFIGS_DIR, exist_ok=True)
os.makedirs(POSTPROCESS_CONFIGS_DIR, exist_ok=True)
os.makedirs(INFERENCE_CONFIGS_DIR, exist_ok=True)

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)