# ---- BuildStage ----
FROM python:3.11-alpine AS BuildStage

WORKDIR /app

# Installs build-essential, which includes compilers and tools needed to build Python packages with native extensions (like NumPy, pandas, etc.).
# --no-install-recommends avoids installing unnecessary packages.
# Cleans up package lists to keep the image small.

# Build dependencies only
RUN apk add --no-cache \
    build-base \
    gcc \
    musl-dev \
    libffi-dev

# Upgrades pip to the latest version.
# Uses pip wheel to build wheels (precompiled Python packages) for everything listed in requirements.txt.
# Stores the wheels in /wheels.
# --no-cache-dir avoids caching downloads.
# --no-deps ensures only the packages listed are built, not their dependencies (dependencies will be resolved later when installing from wheels).
COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip wheel --no-cache-dir -r requirements.txt -w /wheels

# ---- RuntimeStage ----
FROM python:3.11-alpine AS RuntimeStage

# create a non-root user
RUN addgroup -S booktrackgrp && adduser -S booktrackuser -G booktrackgrp

WORKDIR /app    

# Upgrade critical OS packages to pick up security fixes (glibc, libc6)
# Keep this minimal and explicit to avoid unexpected package installs. 

# Runtime dependencies only
RUN apk add --no-cache libpq

# copy only build wheels
COPY --from=BuildStage /wheels /wheels
COPY --from=BuildStage  /app/requirements.txt .

# Offline Install
RUN pip install --no-cache-dir --no-index --find-links /wheels/ -r requirements.txt \
    && rm -rf /wheels

# copy application code
COPY main.py .

USER booktrackuser

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Proudction grade server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]