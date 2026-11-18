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
| `make test-integration` | End-to-end API/pipeline suite under `tests/integration` |
| `make build` | Build `bin/dpp` |
| `make ci` | tidy, lint, test, and build |

## Docker (API + Postgres + Prometheus + Grafana)

```bash
docker compose up --build
```

| Service | URL |
| --- | --- |
| API | http://localhost:8080 |
| Metrics | http://localhost:8080/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |

Grafana opens the **DPP Overview** dashboard on login (HTTP rate, latency, jobs by status).  
If you land on the empty home page: **Dashboards → DPP Overview**, or open http://localhost:3000/d/dpp-overview/dpp-overview

## CI

- `.github/workflows/ci.yml` lints, tests, and builds on push and pull request.
- `.github/workflows/security.yml` runs `govulncheck` on push, pull request, and every Monday.
- `.github/dependabot.yml` opens weekly updates for Go modules and GitHub Actions.

`make build` needs a `main` package. Until that exists, `go test ./...` and `go build ./...` are the commands that succeed on a module with no packages.
