from amazon/aws-lambda-python:latest-x86_64

ARG lambda_handler_func_name

ARG lambda_func_code_dir

ARG main_handler_file_name

COPY ${lambda_func_code_dir}/ ${LAMBDA_TASK_ROOT}/

WORKDIR LAMBDA_TASK_ROOT


RUN if [ -f requirements.txt]: then /
        pip install -r requirements.txt -t .
    else \
        echo "no requirements.txt file found"
    fi


CMD ([ -f .env ] \
    export $(grep -v '^#' .env | xargs) ||  true) && \
    ${main_handler_file_name}.${lambda_handler_func_name}
 