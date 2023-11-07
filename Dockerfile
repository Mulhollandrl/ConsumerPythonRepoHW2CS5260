FROM python:3.12
COPY consumer.py consumer.py
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 80
CMD ["python", "consumer.py", "--storage_strategy", "${STORAGE_STRATEGY}", "--queue_name", "${QUEUE_NAME}", \
"--access_key", "${ACCESS_KEY}", "--secret_key", "${SECRET_KEY}", "--session_token", "${SESSION_TOKEN}", "--bucket2_name", "${BUCKET2_NAME}", \
"--bucket3_name", "${BUCKET3_NAME}", "--table_name", "${TABLE_NAME}"]
