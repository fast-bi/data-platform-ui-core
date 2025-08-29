# Data Platform UI Core

Fast.BI user console that unifies access to all platform services in one place, delivering a seamless, product-like experience without context switching.

## Overview

This image serves the Fast.BI web console, integrating Data Catalog (dbt docs), Data Quality (Re-Data), Orchestration (Airflow/dbt), Metadata, and IDP (SSO) into a single interface. It consolidates navigation, embeds apps, and exposes consistent UX patterns for project-centric workflows.

## Architecture

### Core Features

- **Unified Navigation**: Single entry point to platform capabilities and resources
- **Embedded Views**: Securely embeds underlying apps (docs, quality, orchestration) where applicable
- **SSO Integration**: Keycloak-based SSO for seamless authentication across services
- **Metadata-Aware UI**: Surfaces project health, links, and actions from Meta API
- **Responsive Layout**: Optimized for laptops and large screens

## Docker Image

### Base Image
- **Base**: Python (Flask-based app) with Supervisor/Nginx as configured in repo

### Build

```bash
# Build the image
./build.sh

# Or manually
docker build -t data-platform-ui-core .
```

## Configuration

- `IDP_BASE_URL` – Identity provider (Keycloak) base URL
- `META_API_URL` – Data Platform Meta API endpoint
- `CATALOG_BASE_URL` – Data Catalog service base URL
- `QUALITY_BASE_URL` – Data Quality service base URL
- `ORCHESTRATOR_URL` – Airflow/Orchestration endpoint(s)

Configure environment variables or config files as used by `app/` to point the console to your services.

## Health Checks

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' data-platform-ui-core

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' data-platform-ui-core
```

## Troubleshooting

- **Cannot sign in**: Verify IDP settings and client configuration
- **Missing data**: Ensure Meta API and service URLs are reachable
- **Embeds not loading**: Check CORS/headers and service authentication

## Getting Help

- **Documentation**: https://wiki.fast.bi
- **Issues**: https://github.com/fast-bi/data-platform-ui-core/issues
- **Email**: support@fast.bi

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.