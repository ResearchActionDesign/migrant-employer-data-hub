## Creating migrations

Create a migration using alembic by running
```shell
python -m alembic revision --autogenerate -m "<MESSAGE>"
```

Migrate the db by running
```shell
python -m alembic upgrade head
```

## Deploy to AWS Sam

The lambda function is built within the container specified by `lambda.Dockerfile`.


### Initial deploy
Build the lambda function with `sam build`.

On initial deploy you will need to do the following:
1. Play around with approaches to fixing the circular dependency problem, see https://aws.amazon.com/blogs/mt/resolving-circular-dependency-in-provisioning-of-amazon-s3-buckets-with-aws-lambda-event-notifications/
2. If deployment does not succeed initially, you will be stuck in a ROLLBACK state on initial deployment and need to run
   `sam delete` before re-trying the initial deployment.

### Subsequent deployments
1. Build the lambda function with `sam build`.
2. Run `sam deploy --profile cdm`

Note: The CloudFormation template is not reliably setting up S3 bucket triggers. You may find it easier to just add those via the AWS
console UI.

### Running the dedupe UI locally
1. Start up postgres by running `docker-compose up`
2. Run `pipenv run python interactive_dedupe_session.py`
