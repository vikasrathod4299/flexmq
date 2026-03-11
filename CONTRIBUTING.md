# Contributing to flexmq

Thanks for your interest in contributing.

## Development setup

1. Fork and clone the repository.
2. Install dependencies:

```bash
npm install
```

3. Build all packages:

```bash
npm run build
```

4. Run tests:

```bash
npm test
```

## Project structure

- [packages/core](packages/core): core queue, worker, and in-memory storage
- [packages/redis](packages/redis): Redis storage adapter
- [tests](tests): unit and integration tests

## Workflow

1. Create a feature branch from `main`.
2. Make focused changes.
3. Add or update tests.
4. Run all checks locally:
   - `npm run build`
   - `npm test`
5. Open a pull request with a clear summary.

## Coding guidelines

- Keep changes small and easy to review.
- Prefer clear names and straightforward logic.
- Avoid unrelated refactors in the same PR.

## Testing expectations

- Add tests for new behavior.
- Update tests when behavior changes.
- Keep existing tests passing.

## Commit and PR guidance

- Write clear commit messages.
- In PR description, include:
  - what changed
  - why it changed
  - how it was tested

## Reporting issues

Please include:

- environment details (Node version, OS)
- reproduction steps
- expected behavior
- actual behavior
- logs/errors if available

## Code of conduct

Be respectful and constructive in all interactions.
