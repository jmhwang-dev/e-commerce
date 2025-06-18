import os

ARTIFACT_ROOT_DIR = "./artifact"
ARTIFACT_INFERENCE_PREPROCESS_DIR = os.path.join(ARTIFACT_ROOT_DIR, 'inference', 'preprocess')
ARTIFACT_INFERENCE_RESULT_DIR = os.path.join(ARTIFACT_ROOT_DIR, 'inference', 'result')

MEDALLION_ROOT_DIR = "./medallion"
BRONZE_DIR = os.path.join(MEDALLION_ROOT_DIR, 'bronze')
SILVER_DIR = os.path.join(MEDALLION_ROOT_DIR, 'silver')