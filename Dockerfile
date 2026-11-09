FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# AWS CLI v2 for Import data (S3 download + SSO sign-in). Same version as the tested Mac CLI.
ARG AWS_CLI_VERSION=2.36.43

WORKDIR /app
COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata curl unzip \
    && case "$(uname -m)" in aarch64|arm64) arch=aarch64 ;; x86_64|amd64) arch=x86_64 ;; *) echo "Unsupported CPU: $(uname -m)" >&2; exit 1 ;; esac \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${AWS_CLI_VERSION}.zip" -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip \
    && apt-get purge -y curl unzip && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* \
    && aws --version \
    && pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 lineage \
    && useradd --uid 10001 --gid lineage --create-home lineage \
    && mkdir -p /app/data /home/lineage/.aws \
    && chown lineage:lineage /app/data /home/lineage/.aws && chmod 700 /home/lineage/.aws

COPY --chown=lineage:lineage app.py ./
COPY --chown=lineage:lineage lineage_app/ ./lineage_app/

USER lineage
# The container cannot open the Mac's browser: SSO sign-in shows a link + code on the progress page.
ENV LINEAGE_AWS_DEVICE_CODE=1
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8765/health', timeout=2)" || exit 1

CMD ["python", "-m", "lineage_app", "--host", "0.0.0.0", "--port", "8765", "--data-dir", "/app/data"]
