from celery_app import app
import psycopg2
import pandas as pd

@app.task(name='load_task.load_to_db')
def load_to_db(data):
    print("🚨 [LOAD TASK] Masuk ke fungsi load_to_db")
    df = pd.DataFrame(data)

    if df.empty:
        print("⚠️ Data kosong, tidak ada yang disimpan.")
        return "Data kosong."

    print("Kolom-kolom yang diterima:", df.columns.tolist())
    print("Jumlah baris:", len(df))

    conn = psycopg2.connect(
        host="localhost",
        dbname="bitcoin_db",
        user="postgres",
        password="23022006",
        port=5432
    )
    cur = conn.cursor()

    if 'close' in df.columns and 'forecast' not in df.columns:
        print("📥 Menyimpan ke bitcoin_history...")
        for _, row in df.iterrows():
            try:
                date_value = pd.to_datetime(row["date"]).date()
                close_value = float(row["close"])
                cur.execute("""
                    INSERT INTO bitcoin_history (date, close)
                    VALUES (%s, %s)
                    ON CONFLICT (date) DO UPDATE SET close = EXCLUDED.close
                """, (date_value, close_value))
            except Exception as e:
                print(f"⚠️ Gagal menyimpan baris: {row} | Error: {e}")

    elif 'forecast' in df.columns and 'close' not in df.columns:
        print("📥 Menyimpan ke bitcoin_forecast...")
        for _, row in df.iterrows():
            try:
                date_value = pd.to_datetime(row["date"]).date()
                forecast_value = float(row["forecast"])
                cur.execute("""
                    INSERT INTO bitcoin_forecast (date, forecast)
                    VALUES (%s, %s)
                    ON CONFLICT (date) DO UPDATE SET forecast = EXCLUDED.forecast
                """, (date_value, forecast_value))
            except Exception as e:
                print(f"⚠️ Gagal menyimpan baris: {row} | Error: {e}")

    else:
        print("❌ Format data tidak cocok atau kolom tidak ditemukan.")

    conn.commit()
    cur.close()
    conn.close()
    return "✅ Data berhasil dimasukkan ke tabel yang sesuai"
