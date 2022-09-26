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
`sam deploy --profile cdm`
