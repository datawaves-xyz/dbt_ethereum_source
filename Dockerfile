FROM fishtownanalytics/dbt:1.0.0
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
  python-dev \
  libsasl2-dev \
  gcc \
  unixodbc-dev \
  && apt-get clean \
  && rm -rf \
  /var/lib/apt/lists/* \
  /tmp/* \
  /var/tmp/*
RUN pip install "dbt-spark[PyHive]"==1.0.0
WORKDIR /support
RUN mkdir /root/ethereum
WORKDIR /ethereum
COPY . .
RUN dbt deps
CMD ["run", "--profiles-dir", "profiles"]
