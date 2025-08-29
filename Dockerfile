#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.11.11-slim-bullseye AS base
LABEL maintainer=support@fast.bi

# Create directories and set working directory
RUN mkdir -p /usr/src/app /etc/supervisor/conf.d
WORKDIR /usr/src/app

# System setup - install dependencies
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    git \
    supervisor \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Upgrade pip
RUN python -m pip install --no-cache-dir --upgrade pip

# Copy and install requirements first (will only rebuild if requirements.txt changes)
COPY requirements.txt /usr/src/app/
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy supervisor configuration (less likely to change than app code)
COPY supervisord.conf /etc/supervisor/supervisord.conf

# Copy application code last (most likely to change)
COPY . /usr/src/app

EXPOSE 8080

# Run supervisord with explicit configuration file
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]