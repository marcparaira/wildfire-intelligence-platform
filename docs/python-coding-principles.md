
# Python Coding Standard

**Version:** 0.3  
**Python:** 3.12  
**Status:** Draft

**MUST** = Mandatory  
**SHOULD** = Recommended  
**MAY** = Optional

Deviations from MUST require a documented exception.

## 1. Platform & Tooling

- **MUST** target Python 3.12 and declare it in `pyproject.toml`.
- **MUST** use BasedPyright in strict mode.
- **MUST** annotate parameters, return types, and public attributes.
- **MUST** avoid `Any` except in documented boundary cases.
- **MUST** use Ruff for formatting and linting.
- **MUST** use Pytest for testing.
- **MUST** enforce formatting, linting, type checking, and tests in CI.
- **SHOULD** pin development dependencies.
- **SHOULD** use modern Python typing syntax.
- **SHOULD** use a maximum line length of 100 characters.

## 2. Architecture

- **MUST** separate data models, business logic, infrastructure, and entry points.
- **MUST** follow the Single Responsibility Principle.
- **MUST** keep domain logic independent of persistence and transport mechanisms.
- **MUST** make external dependencies explicit through dependency injection.
- **MUST** avoid circular dependencies.
- **SHOULD** prioritize object-oriented design over procedural programming.
- **SHOULD** prefer composition over inheritance.
- **SHOULD** use `Protocol` for interfaces at meaningful architectural boundaries.
- **MAY** use standalone functions for simple, stateless utilities.
- **MUST NOT** introduce unnecessary abstractions, inheritance, or interfaces.

## 3. Data Models & Typing

- **SHOULD** prefer immutable data models.
- **SHOULD** use `@dataclass(frozen=True, slots=True)` by default for value objects.
- **MUST** validate external data at system boundaries.
- **MUST** preserve domain invariants.
- **MUST** use enums for closed sets of domain values.
- **MUST** avoid magic strings and numbers.
- **MUST** prefer `auto()` for `StrEnum` values when they match lowercase member names.
- **MAY** use explicit enum values for external compatibility.
- **SHOULD** use decorator syntax for Pydantic dataclasses.
- **MUST** use `field(default_factory=...)` for mutable dataclass defaults.

### Enum example

```python
from enum import StrEnum, auto


class OrderStatus(StrEnum):
    PENDING = auto()
    APPROVED = auto()
    REJECTED = auto()
```

### Pydantic dataclass example

```python
from pydantic.dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Order:
    id: int
    status: OrderStatus
```

Use standard-library dataclasses when Pydantic validation is unnecessary.

## 4. Code-Level Conventions

- **MUST** use `X | None` instead of `Optional[X]`.
- **MUST** use built-in generics such as `list[X]` and `dict[K, V]`.
- **MUST** use `is None` and `is not None`.
- **SHOULD** prefer keyword arguments for ambiguous function calls.
- **SHOULD** prefer `pathlib.Path` for filesystem operations.
- **SHOULD** prefer f-strings for ordinary string interpolation.
- **SHOULD** prefer `@property` over trivial getter methods.
- **SHOULD** use `super()` when extending parent behavior.
- **SHOULD** prefer guard clauses and early returns.
- **SHOULD** minimize unnecessary `if/else` nesting.
- **SHOULD** use `match`, polymorphism, or lookup tables when they simplify branching.
- **SHOULD** avoid boolean arguments that switch between distinct behaviors.
- **MUST** keep methods small, focused, and understandable.
- **MUST** use descriptive names.
- **MUST** avoid premature abstraction.

## 5. Error Handling & Logging

- **MUST** handle failures explicitly.
- **MUST NOT** silently suppress unexpected exceptions.
- **SHOULD** use domain-specific exceptions.
- **SHOULD** catch exceptions at the layer capable of handling them.
- **MUST** preserve exception causes with `raise ... from ...` when translating exceptions.
- **MUST** use logging instead of `print()` for production diagnostics.
- **MUST NOT** log secrets or sensitive information.
- **SHOULD** use structured logging with contextual fields.

## 6. Documentation

- **MUST** document non-obvious public APIs and contracts.
- **MUST** document important constraints, side effects, and exceptions.
- **SHOULD** follow PEP 257 docstring conventions.
- **SHOULD** explain why code exists rather than restating what it does.
- **MUST** keep documentation synchronized with implementation.

## 7. Testing

- **MUST** use Pytest.
- **MUST** test business behavior, failure cases, and important boundaries.
- **MUST NOT** depend on external services in unit tests.
- **SHOULD** favor deterministic tests.
- **SHOULD** use simple fixtures and in-memory fakes.
- **SHOULD** add integration tests for infrastructure components.
- **MUST** design business logic to support dependency substitution during testing.

## 8. Continuous Integration & Code Review

- **MUST** pass all automated checks before merging.
- **MUST** review architecture and maintainability in addition to automated results.
- **MUST** prevent undocumented permanent bypasses of quality checks.
- **SHOULD** use identical quality commands locally and in CI.

### Required commands

```bash
ruff format --check .
ruff check .
basedpyright
pytest
```

## 9. Guiding Principles

1. Favor clarity over cleverness.
2. Favor explicit dependencies over hidden coupling.
3. Favor composition over inheritance.
4. Favor strong typing over dynamic assumptions.
5. Favor cohesive classes over large multipurpose classes.
6. Favor simple control flow over unnecessary branching.
7. Favor meaningful abstractions over speculative extensibility.
8. Favor maintainability over premature optimization.
9. Keep business logic independent of external systems.
10. Automate everything that can be consistently enforced.
