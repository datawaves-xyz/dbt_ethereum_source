# dbt_ethereum_source

This package models data loaded from [Ethereum-ETL](https://github.com/blockchain-etl/ethereum-etl).

## Models

This package contains staging models, designed to work simultaneously with [dbt_ethereum](https://github.com/datawaves-xyz/dbt_ethereum).

## Installation Instructions

Include in your `packages.yml`:

```yml
packages:
  - git: "https://github.com/datawaves-xyz/dbt_ethereum_source"
```

## Configuration

By default, this package will looks for your data in the `ethereum` schema of your target database. If this is not where your Ethereum data is, add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml
---
config-version: 2

vars:
  ethereum_schema: your_schema_name
  ethereum_database: your_database_name
```

## Codegen

```
$ pip install blockchain-dbt
$ bdbt ethereum_codegen \
  -w /Users/songv/Code/jike/web3/dbt_ethereum_source/models \
  -rw s3a://ifcrypto/dist/java \
  -p /Users/songv/Code/jike/web3/dbt_ethereum_source/opensea \
  -d spark
```
