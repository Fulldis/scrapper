FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
COPY article_tag_service.py .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "article_tag_service.py"] 