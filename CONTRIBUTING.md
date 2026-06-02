# Contributing to Hodor

Thank you for taking the time to contribute! Please follow the guidelines below to
keep the project history clean and reviews fast.

---

## Workflow

1. **Fork** the main repository (`talhaHavadar/hodor`).
2. Create a **feature branch** in your fork:
   - `feature/<short-description>` — new features
   - `fix/<short-description>` — bug fixes
   - `docs/<short-description>` — documentation-only changes
   - `chore/<short-description>` — maintenance (CI, deps, tooling)
3. Make your changes following the commit convention below.
4. Open a **Pull Request** from your fork branch targeting `main` in the upstream
   repo.
5. Address review feedback with new commits (no force-pushes after PR is open).
6. A maintainer will squash-merge once CI is green and the PR has ≥ 1 approval.

---

## Commit Convention (Zephyr-style)

Every commit message must follow this format:

```
<scope>: summary of change in max 80 chars

Body text — describe *what* changed and *why*. Wrap each line at 80 characters.
Leave a blank line between the subject and the body.
```

### Rules

- **Subject line** — `<scope>: short summary`, max **80 characters** total.
- **Scope** — a lowercase noun identifying the affected area, e.g. `docs`, `ci`,
  `server`, `auth`, `proto`, `build`.
- **Body** — optional but encouraged for non-trivial changes. Explain context and
  motivation. Each line must be **≤ 80 characters**.
- **Atomic commits** — one logical change per commit. The repo must be in a
  **buildable and working state** after every commit (no broken intermediates).
- Use the **imperative mood** in the subject: "add feature" not "added feature".

### Examples

```
docs: add CONTRIBUTING and PR template

Establishes the fork-and-PR workflow, Zephyr-style commit convention,
and branch naming guidelines for the hodor project.
```

```
server: implement gRPC command execution handler

Adds the initial CommandExecutor service that accepts a CommandRequest
and streams back stdout/stderr in real time. Includes unit tests.
```

---

## Pull Request Requirements

- **Small and focused** — one feature, fix, or change per PR. Avoid bundling
  unrelated changes.
- **CI must be green** before requesting review.
- **At least 1 approval** from a maintainer is required before merging.
- Fill in the PR description template completely.

---

## Code Style

Follow the conventions already present in the codebase. Specific style guides will
be documented here as the project matures.
