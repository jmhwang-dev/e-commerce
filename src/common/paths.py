import os

ARTIFACTS_ROOT_DIR = "./artifacts"
ARTIFACTS_PREPROCESS_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'preprocess')
ARTIFACTS_POSTPROCESS_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'postprocess')
ARTIFACTS_INFERENCE_DIR = os.path.join(ARTIFACTS_ROOT_DIR, 'inference')

CONFIGS_ROOT_DIR = "./configs"
CONFIGS_PREPROCESS_DIR = os.path.join(CONFIGS_ROOT_DIR, 'preprocess')
CONFIGS_POSTPROCESS_DIR = os.path.join(CONFIGS_ROOT_DIR, 'postprocess')
CONFIGS_INFERENCE_DIR = os.path.join(CONFIGS_ROOT_DIR, 'inference')

MEDALLION_ROOT_DIR = "./data"
BRONZE_DIR = os.path.join(MEDALLION_ROOT_DIR, 'bronze')
SILVER_DIR = os.path.join(MEDALLION_ROOT_DIR, 'silver')

os.makedirs(ARTIFACTS_ROOT_DIR, exist_ok=True)
os.makedirs(ARTIFACTS_PREPROCESS_DIR, exist_ok=True)
os.makedirs(ARTIFACTS_POSTPROCESS_DIR, exist_ok=True)
os.makedirs(ARTIFACTS_INFERENCE_DIR, exist_ok=True)

os.makedirs(CONFIGS_ROOT_DIR, exist_ok=True)
os.makedirs(CONFIGS_PREPROCESS_DIR, exist_ok=True)
os.makedirs(CONFIGS_POSTPROCESS_DIR, exist_ok=True)
os.makedirs(CONFIGS_INFERENCE_DIR, exist_ok=True)

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)