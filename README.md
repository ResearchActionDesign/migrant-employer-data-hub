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

To build lambda function run `sam build`.

To deploy run:
`sam deploy --profile cdm`
