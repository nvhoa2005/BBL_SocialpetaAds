from export_excel import json_to_excel
from custom_logger import log
import os

final_json_path = os.path.join("crawl_json", "final_result_crawl_20260412_032947_8c149e.json")
            
if final_json_path:
    log.info("CHUYỂN GIAO SANG EXCEL EXPORTER...")
    json_to_excel(final_json_path)
else:
    log.error("Không có file final JSON để chuyển sang Excel.")