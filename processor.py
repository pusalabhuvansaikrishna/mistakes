# processor.py
import pandas as pd
import requests
from typing import List, Dict, Any, Optional
import time


def fetch_json_data(api_url: str) -> Optional[Dict[str, Any]]:
    """Fetch JSON from the /api/ endpoint"""
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching {api_url}: {e}")
        return None


def process_single_file(uploaded_file, url_column: str = "URL") -> tuple[str, bytes]:
    """
    Process one uploaded CSV file and return (output_filename, csv_bytes)
    """
    start_time = time.time()
    original_name = uploaded_file.name
    base_name = original_name.rsplit('.', 1)[0] if '.' in original_name else original_name
    output_name = f"{base_name}_output.csv"

    try:
        df = pd.read_csv(uploaded_file)
        print(f"📂 Processing: {original_name} ({len(df)} rows)")

        if url_column not in df.columns:
            raise ValueError(f"Column '{url_column}' not found. Available columns: {list(df.columns)}")

        unique_urls = df[url_column].dropna().unique()
        output_rows = []

        for idx, original_url in enumerate(unique_urls, 1):
            print(f"   🌐 [{idx}/{len(unique_urls)}] {original_url}")
            api_url = original_url.rstrip('/') + '/api/'

            json_data = fetch_json_data(api_url)
            if not json_data:
                continue

            item_id = json_data.get("id")
            gt = json_data.get("gt")
            ocr_list: List[Dict] = json_data.get("ocr_list", [])

            for item in ocr_list:
                output_rows.append({
                    "id": item_id,
                    "URL": original_url,
                    "GT": gt,
                    "Layout Model": item.get("layout_model", ""),
                    "ocr_model": item.get("ocr_model", ""),
                    "predicted_text": item.get("text", "")
                })

        output_df = pd.DataFrame(output_rows) if output_rows else pd.DataFrame(
            columns=["id", "URL", "GT", "Layout Model", "ocr_model", "predicted_text"]
        )

        csv_bytes = output_df.to_csv(index=False).encode('utf-8')
        duration = time.time() - start_time
        print(f"✅ Finished {original_name} → {output_name} in {duration:.1f}s\n")

        return output_name, csv_bytes

    except Exception as e:
        print(f"❌ Failed processing {original_name}: {e}")
        raise