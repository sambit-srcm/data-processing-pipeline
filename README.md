# data-processing-pipeline

Go service for the data processing pipeline.

## Requirements

- Go 1.27
- golangci-lint v2.13 (`brew install golangci-lint` or the [install script](https://golangci-lint.run/docs/welcome/install/))
- `goimports` (`go install golang.org/x/tools/cmd/goimports@latest`)

## Commands

| Command | What it does |
| --- | --- |
| `make tidy` | Sync `go.mod` and `go.sum` |
| `make fmt` | Format with `gofmt` and `goimports` |
| `make lint` | Run golangci-lint |
| `make test` | Race-enabled tests with coverage |
| `make build` | Build `bin/dpp` |
| `make ci` | tidy, lint, test, and build |

## CI

- `.github/workflows/ci.yml` lints, tests, and builds on push and pull request.
- `.github/workflows/security.yml` runs `govulncheck` on push, pull request, and every Monday.
- `.github/dependabot.yml` opens weekly updates for Go modules and GitHub Actions.

`make build` needs a `main` package. Until that exists, `go test ./...` and `go build ./...` are the commands that succeed on a module with no packages.
