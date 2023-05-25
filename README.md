# execd

[![Tag](https://img.shields.io/github/tag/wealdtech/execd.svg)](https://github.com/wealdtech/execd/releases/)
[![License](https://img.shields.io/github/license/wealdtech/execd.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/wealdtech/execd?status.svg)](https://godoc.org/github.com/wealdtech/execd)
![Lint](https://github.com/wealdtech/execd/workflows/golangci-lint/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/wealdtech/execd)](https://goreportcard.com/report/github.com/wealdtech/execd)

`execd` is a process that reads information from an Ethereum execution node and stores it in a database for reporting and analysis.

## Table of Contents

- [Install](#install)
  - [Binaries](#binaries)
  - [Docker](#docker)
  - [Source](#source)
- [Usage](#usage)
- [Maintainers](#maintainers)
- [Contribute](#contribute)
- [License](#license)

## Install

### Binaries

Binaries for the latest version of `execd` can be obtained from [the releases page](https://github.com/wealdtech/execd/releases/latest).

### Docker

You can obtain the latest version of `execd` using docker with:

```
docker pull wealdtech/execd
```

### Source

`execd` is a standard Go binary which can be installed with:

```sh
go get github.com/wealdtech/execd
```

## Requirements to run `execd`
### Database
At current the only supported backend is PostgreSQL.  Once you have a  PostgreSQL instance you will need to create a user and database that `execd` can use, for example run the following commands as the PostgreSQL superuser (`postgres` on most linux installations):

```
# This command creates a user named 'exec' and will prompt for a password.
createuser exec -P
# This command creates a database named 'exec' owned by the 'exec' user.
createdb -E UTF8 --owner=exec exec
```

### Execution node
`execd` supports Erigon execution nodes.  The current state of obtaining data from execution nodes is as follows:

- Erigon:
  - must be run without pruning to allow `execd` to obtain historical data
  - must run the RPC daemon

## Upgrading `execd`
`execd` should upgrade automatically from earlier versions.  Note that the upgrade process can take a long time to complete, especially where data needs to be refetched or recalculated.  `execd` should be left to complete the upgrade, to avoid the situation where additional fields are not fully populated.  If this does occur then `execd` can be run with the options `--blocks.start-height=0` to force `execd` to refetch all blocks, although note that it will commonly be faster to restart `execd` with a clean database in this situation.

## Querying `execd`
`execd` attempts to lay its data out in a standard fashion for a SQL database, mirroring the data structures that are present in Ethereum.  There are some places where the structure or data deviates from the specification, commonly to provide additional information or to make the data easier to query with SQL.  The [database schema](https://github.com/wealdtech/execd/blob/master/services/execdb/postgresql/upgrader.go) can be found in the database module.

## Configuring `execd`
The minimal requirements for `execd` are references to the database and execution node, for example:

```
execd --execdb.url=postgres://chain:secret@localhost:5432 --execclient.address=http://localhost:8545/
```

Here, `execdb.url` is the URL of a local PostgreSQL database with pasword 'secret' and 'execdb.address' is the address of a supported execution node.

`execd` allows additional configuration for itself and its modules.  It takes configuration from the command line, environment variables or a configuration file, but for the purposes of explaining the configuration options the configuration file is used.  This should be in the home directory and called `.execd.yml`.  Alternatively, the configuration file can be placed in a different directory and referenced by `--base-dir`, for example `--base-dir=/home/user/config/execd`; in this case the file should be called `execd.yml` (without the leading period).

```
# log-level is the base log level of the process.
# 'info' should be a suitable log level, unless detailed information is
# required in which case 'debug' or 'trace' can be used.
log-level: info
# log-file specifies that log output should go to a file.  If this is not
# present log output will be to stderr.
log-file: /var/log/execd.log
execdb:
  # server is the server of the PostgreSQL database.
  server: localhost
  # port is the port of the PostgreSQL database.
  port: 5432
  # user is a user in the PostgreSQL database that has access to the 'exec' database.
  user: exec
  # password is the password of the user specified above.
  password: secret
# execclient contains configuration for the Ethereum client.
execclient:
  # log-level is the log level of the specific module.  If not present the base log
  # level will be used.
  log-level: debug
  # address is the address of the execution node.
  address: http://localhost:8545/
# blocks contains configuration for obtaining block-related information.
blocks:
  # enable states if this module will be operational.
  enable: true
  # address is a separate connection for this module.  If not present then
  # execd will use the global connection.
  address: http://otherhost:8544/
  # transactions handles transactions in blocks.
  transactions:
    # enable states that transaction data should be stored
    enable: true

```

## Maintainers

Jim McDonald: [@mcdee](https://github.com/mcdee).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/wealdtech/execd/issues).

## License

[Apache-2.0](LICENSE) © 2021 Weald Technology Trading.
