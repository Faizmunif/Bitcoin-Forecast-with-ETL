import logging
import pandas as pd
from extract_task import extract_data
from transform_task import preprocess_data, forecast_data
from load_task import load_to_db
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    csv_path = r"D:\Kuliah Semester 4\Data Warehouse\Proyek\merge_data.csv"

    logging.info("Step 1: Membaca file CSV...")
    try:
        df = pd.read_csv(csv_path)
        data_records = df.to_dict(orient='records')
        logger.info(f"Total record: {len(data_records)}")
    except Exception as e:
        logger.error(f"Gagal membaca file CSV: {e}")
        exit(1)

    try:
        logger.info("🔄 Menjalankan pipeline satu kali untuk seluruh data...")

        # Extract (opsional kalau sudah dari CSV)
        result = extract_data.delay(data_records)
        data = result.get(timeout=30)
        logger.info("✅ Extract selesai.")

        # Preprocess
        result = preprocess_data.delay(data)
        preprocessed = result.get(timeout=30)
        logger.info("✅ Preprocess selesai.")

        # Forecast
        result = forecast_data.delay(preprocessed)
        forecasted = result.get(timeout=30)
        logger.info("✅ Forecast selesai.")

        # Load data historis
        result = load_to_db.delay(forecasted["history"])
        logger.info(f"✅ Load history selesai: {result.get(timeout=30)}")

        # Load data forecast
        result = load_to_db.delay(forecasted["forecast"])
        logger.info(f"✅ Load forecast selesai: {result.get(timeout=30)}")

    except Exception as e:
        logger.error(f"❌ Gagal memproses data: {e}")
