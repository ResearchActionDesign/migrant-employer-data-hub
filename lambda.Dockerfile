FROM public.ecr.aws/lambda/python:3.9
RUN yum -y update
RUN yum -y group install "Development Tools"
RUN yum install -y python3-dev postgresql postgresql-libs postgresql-devel openssl11 openssl11-devel

COPY requirements.txt ./
RUN python3.9 -m pip install -r requirements.txt
COPY app/ ./app

ARG HANDLER_PACKAGE
ENV HANDLER_PACKAGE $HANDLER_PACKAGE
RUN cp ./app/lambda_handlers/"$HANDLER_PACKAGE".py ./app/lambda_handler.py
CMD ["app.lambda_handler.lambda_handler"]
