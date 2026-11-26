FROM ghcr.io/astral-sh/uv:0.9.11@sha256:5aa820129de0a600924f166aec9cb51613b15b68f1dcd2a02f31a500d2ede568 AS uv
FROM python:3.11.14-slim-bookworm@sha256:65a93d69fa75478d554f4ad27c85c1e69fa184956261b4301ebaf6dbb0a3543d
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/map_reduce/ ./src/map_reduce/
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --require-hashes --no-cache -r /tmp/requirements.txt \
    && uv pip install --system --no-cache setuptools==80.9.0 wheel==0.45.1 \
    && uv pip install --system --no-deps --no-build-isolation --no-cache . \
    && rm /tmp/requirements.txt
ARG SOURCE_ID
LABEL org.opencontainers.image.revision=$SOURCE_ID
ENV SOURCE_ID=$SOURCE_ID PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
ENTRYPOINT ["mapreduce-lab"]
CMD ["--help"]
