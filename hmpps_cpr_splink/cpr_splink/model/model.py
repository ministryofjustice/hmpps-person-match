from pathlib import Path

MODEL_VERSION = "model_2025_03_17_1e09"

MODEL_PATH = Path(__file__).parent / f"model_files/{MODEL_VERSION}.json"
# TODO: other metadata of use to library client?

MATCH_WEIGHT_THRESHOLD = 18
