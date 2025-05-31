from celery_app import app
import pandas as pd
from datetime import timedelta
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.arima.model import ARIMA
import numpy as np

@app.task(name='transform_task.preprocess_data')
def preprocess_data(data):
    df = pd.DataFrame(data)
    df["Date"] = pd.to_datetime(df["Date"])
    df.sort_values("Date", inplace=True)
    return df.to_dict(orient='records')

@app.task(name='transform_task.forecast_data')
def forecast_data(data):
    df = pd.DataFrame(data)
    df["Date"] = pd.to_datetime(df["Date"])
    df.sort_values("Date", inplace=True)

    try:
        close_series = df["BTC_Close_BTC"].values.reshape(-1, 1)

        # Skala data agar ARIMA lebih stabil
        scaler = MinMaxScaler()
        scaled_close = scaler.fit_transform(close_series).flatten()

        # ARIMA model
        model = ARIMA(scaled_close, order=(5, 1, 0))
        model_fit = model.fit()

        # Prediksi 90 hari ke depan
        steps = 90
        forecast_scaled = model_fit.forecast(steps=steps)
        forecast_orig = scaler.inverse_transform(forecast_scaled.reshape(-1, 1)).flatten()

        # Tanggal prediksi
        last_date = df["Date"].max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, steps + 1)]

        forecast_df = pd.DataFrame({
            "date": future_dates,
            "forecast": forecast_orig
        })

        # Data historis asli
        history_df = df[["Date", "BTC_Close_BTC"]].rename(columns={
            "Date": "date",
            "BTC_Close_BTC": "close"
        })

        # Ganti NaN jadi None
        forecast_df = forecast_df.where(pd.notnull(forecast_df), None)
        history_df = history_df.where(pd.notnull(history_df), None)

        return {
            "history": history_df.to_dict(orient="records"),
            "forecast": forecast_df.to_dict(orient="records")
        }

    except Exception as e:
        print(f"❌ Error di forecast_data: {e}")
        return {
            "history": [],
            "forecast": []
        }
