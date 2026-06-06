from amazon/aws-lambda-python:latest-x86_64

ARG lambda_handler_func_name

ARG lambda_func_code_dir

ARG main_handler_file_name

ARG lambda_func_name
ENV LAMBDA_FUNC_NAME=${lambda_func_name}
ENV LAMBDA_HANDLER_FUNC_NAME=${lambda_handler_func_name}
ENV MAIN_HANDLER_FILE_NAME=${main_handler_file_name}

# Install build tools in case packages need to be compiled from source.
# Amazon Linux 2023 uses 'dnf' instead of 'yum' or 'apt-get'.
RUN dnf install -y gcc python3-devel && dnf clean all

COPY ${lambda_func_code_dir}/ ${LAMBDA_TASK_ROOT}/

RUN chmod +x ${LAMBDA_TASK_ROOT}/bootstrap.sh

WORKDIR ${LAMBDA_TASK_ROOT}

RUN if [ -f requirements.txt ]; then \
    pip install --no-cache-dir -r requirements.txt -t . ; \
    else \
    echo "no requirements.txt file found"; \
    fi

# This is the key! We hijack the start-up sequence
ENTRYPOINT ["/bin/bash", "/var/task/bootstrap.sh"]

# The handler is resolved by bootstrap.sh using ENV vars, so we pass an empty CMD
CMD []