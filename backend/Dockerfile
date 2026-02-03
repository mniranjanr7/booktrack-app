# ---- BuildStage ----
FROM python:3.11-slim AS BuildStage

WORKDIR /app

# Installs build-essential, which includes compilers and tools needed to build Python packages with native extensions (like NumPy, pandas, etc.).
# --no-install-recommends avoids installing unnecessary packages.
# Cleans up package lists to keep the image small.

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrades pip to the latest version.
# Uses pip wheel to build wheels (precompiled Python packages) for everything listed in requirements.txt.
# Stores the wheels in /wheels.
# --no-cache-dir avoids caching downloads.
# --no-deps ensures only the packages listed are built, not their dependencies (dependencies will be resolved later when installing from wheels).
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip wheel --no-cache-dir -r requirements.txt -w /wheels

# ---- RuntimeStage ----
FROM python:3.11-slim AS RuntimeStage

# create a non-root user
RUN groupadd -r booktrackgrp && useradd -r -g booktrackgrp booktrackuser

WORKDIR /app    

# Upgrade critical OS packages to pick up security fixes (glibc, libc6)
# Keep this minimal and explicit to avoid unexpected package installs. 

RUN apt-get update \
    && apt-get install -y --no-install-recommends libc6 \ 
    && rm -rf /var/lib/apt/lists/*

# copy only build wheels
COPY --from=BuildStage /wheels /wheels
COPY --from=BuildStage  /app/requirements.txt .

RUN pip install --no-cache-dir --no-index --find-links /wheels/ -r requirements.txt \
    && rm -rf /wheels

# copy application code
COPY main.py .

USER booktrackuser

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Proudction grade server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]