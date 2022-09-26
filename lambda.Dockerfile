FROM public.ecr.aws/lambda/python:3.9
RUN yum -y update
RUN yum -y group install "Development Tools"
RUN yum install -y python3-dev postgresql postgresql-libs postgresql-devel openssl11 openssl11-dev

COPY requirements.txt ./
RUN python3.9 -m pip install -r requirements.txt
CMD ["app.lambda_handler.lambda_handler"]
