from celery_app import app

@app.task(name='extract_task.extract_data')
def extract_data(data_records):
    if not data_records or not isinstance(data_records, list):
        return []
    return data_records
