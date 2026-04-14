import os
from dotenv import load_dotenv

from custom_logger import log
from constants import GEMINI_PROMPT_TEMPLATE, DEFAULT_MODEL, VIDEO_MODEL

from parse_with_gemini import process_bundle, DEFAULT_MODEL

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
output_filename = os.path.join("crawl_json", "raw_bundle_crawl_20260412_032947_8c149e.json")
if api_key: process_bundle(output_filename, api_key, DEFAULT_MODEL)