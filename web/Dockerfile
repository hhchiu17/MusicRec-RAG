# 使用 Python 3.9 作為基礎映像
FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

# 使用 Gunicorn 啟動 Flask 應用
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "app:app"]