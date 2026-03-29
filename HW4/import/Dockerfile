FROM python:slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
CMD ["sh", "-c", "python task_3.py && python task_4.py && python task_5.py"]