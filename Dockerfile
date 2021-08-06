FROM python:3.8

WORKDIR /code
COPY . /code/
RUN
RUN pip install --no-cache-dir flake8 pytest
RUN pip install -r requirements.txt
ENTRYPOINT ['/bin/bash']
