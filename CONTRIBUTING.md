# Contributing

## Conventional Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for automated versioning and changelog generation.

| Prefix | Effect | Example |
|--------|--------|---------|
| `feat:` | Minor version bump | `feat: add constellation tab` |
| `fix:` | Patch version bump | `fix: correct franchise completion calc` |
| `feat!:` or `BREAKING CHANGE:` | Major version bump | `feat!: remove xlsx support` |
| `chore:`, `docs:`, `refactor:`, `style:`, `test:` | No version bump | `docs: update README` |

## Pull Request Process

1. Fork the repo and create your branch from `main`
2. Make your changes and ensure `flake8` passes: `flake8 app.py library_runner.py setup.py --max-line-length=120`
3. Update `config.example.py` if you add new config keys
4. Open a PR against `main` with a clear description
5. PRs require passing lint and validate checks before merge

## Reporting Issues

Use the issue templates:
- **Bug Report** — for broken features or unexpected behavior
- **Feature Request** — for new ideas or improvements
