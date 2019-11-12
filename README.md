# infix

[![Build Status](https://github.com/dogmatiq/infix/workflows/CI/badge.svg)](https://github.com/dogmatiq/infix/actions?workflow=CI)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/infix/master.svg)](https://codecov.io/github/dogmatiq/infix)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/infix.svg?label=semver)](https://semver.org)
[![GoDoc](https://godoc.org/github.com/dogmatiq/infix?status.svg)](https://godoc.org/github.com/dogmatiq/infix)
[![Go Report Card](https://goreportcard.com/badge/github.com/dogmatiq/infix)](https://goreportcard.com/report/github.com/dogmatiq/infix)

This repository is a template for Dogmatiq Go modules.

[Click here](https://github.com/dogmatiq/template/generate) to create a new
repository from this template.

After creating a repository from this template, follow these steps:

- Replace the string `infix` in all files with the actual repo name.
- Add a secret named `CODECOV_TOKEN` containing the codecov.io token for the new repository.
  - The secret is available [here](https://codecov.io/gh/dogmatiq/infix/settings).
  - And is configured [here](https://github.com/dogmatiq/infix/settings/secrets)
- Run the commands below to rename `.template` files to their proper names:

    ```
    mv .dependabot/config.yml.template .dependabot/config.yml
    mv .gitignore.template .gitignore
    ```

These renames are necessary because:
- Dependabot doe not seem to inspect the commits that are copied from the
  template, and hence does not find the config for new repositories.
- The GitHub template system does not retain the `.gitignore` file from the
  template in the new repository.
