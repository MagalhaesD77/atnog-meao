FROM python:3.9-slim

WORKDIR /app

COPY src .

RUN pip install -r requirements.txt

CMD ["python", "-u", "main.py"]