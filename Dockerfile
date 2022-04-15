FROM fishtownanalytics/dbt:1.0.0

# Install dependencies 
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

# Copu files to workspace
WORKDIR /support
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/ethereum
WORKDIR /ethereum
COPY . .


RUN dbt deps
CMD ["run"]
