# Unweaver at its last commit (2022-11-02), as upstream's own Dockerfile builds it.
FROM python:3.8-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libsqlite3-mod-spatialite libgdal-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/nbolten/unweaver /unweaver && cd /unweaver \
    && git checkout 66352c181582d08a032a2af12dc14fd2415a7294 \
    && pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir .
ENTRYPOINT ["unweaver"]
