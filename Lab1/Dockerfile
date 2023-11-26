FROM python:3.11

WORKDIR /app
COPY pipeline.py pipeline.py
RUN pip install pandas 

ENTRYPOINT [ "python", "pipeline.py" ]