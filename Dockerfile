
# syntax=docker/dockerfile:1

FROM python:3.9

WORKDIR /code

RUN apt install libpq-dev python3-dev

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

EXPOSE 80
