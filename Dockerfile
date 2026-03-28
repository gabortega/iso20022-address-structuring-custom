FROM redhat/ubi10-minimal:latest

# Install necessary utilities
RUN microdnf update && \
    microdnf -y install gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget tar make shadow-utils && \
    microdnf clean all

# Download and install Python 3.13.4
RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/3.13.4/Python-3.13.4.tgz && \
    tar xzf Python-3.13.4.tgz && \
    cd Python-3.13.4 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    ln -s /usr/local/bin/python3.13 /usr/bin/python3.13 && \
    ln -s /usr/local/bin/pip3.13 /usr/bin/pip3.13 && \
    cd /usr/src && \
    rm -rf Python-3.13.4.tgz Python-3.13.4

# Verify Python installation
RUN python3.13 --version

# Add appuser (RH OpenShift compatible)
RUN useradd -r -u 1001 -g 0 -d /app -s /sbin/nologin appuser

WORKDIR /app

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN python3.13 -m pip install --no-cache-dir --upgrade pip  && \
    python3.13 -m pip install --no-cache-dir -r requirements.txt

# Copy project source folders
COPY data_structuring/ ./data_structuring/
COPY grpc_api/ ./grpc_api/
COPY resources/ ./resources/

# Remove standalone runner
RUN rm -f ./data_structuring/run.py
# Remove raw dependencies
RUN rm -rf ./resources/raw

RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

# Python-related variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# gRPC server params
ENV ds_grpc_hostname=0.0.0.0
ENV ds_grpc_port=8080
ENV ds_grpc_pipeline_max_instances=1

EXPOSE 8080

ENTRYPOINT ["python3.13", "grpc_api/run_server.py"]