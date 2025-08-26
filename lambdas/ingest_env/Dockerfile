FROM public.ecr.aws/lambda/python:3.11

# Keep things lean & avoid accidental source builds
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
# Force wheels only; if a package has no wheel, fail fast (better signal)
RUN pip install --only-binary=:all: -r requirements.txt

# App code
COPY app.py ${LAMBDA_TASK_ROOT}

CMD ["app.handler"]
