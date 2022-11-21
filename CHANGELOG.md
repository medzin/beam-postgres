# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2022-11-21

### Fixed

- Fixed missing argument in sleep log.
- Fixed establishing connection per bundle without closing, which resulted in
  connection leakage.

## [0.2.0] - 2022-11-21

### Added

- RetryRowStrategy ABC class to provide an interface for various retry logics used
  in WriteToPostgres transform.
- AlwaysRetryRowStrategy and RetryRowOnTransientErrorStrategy retry strategies.

### Changed

- Renamed ReadFromPostgres to ReadAllFromPostgres, so the name better reflects
  how the transform works.
- WriteToPostgres returns a PCollection of tuples with the failed element and
  error to allow the graceful handling of errors.

## [0.1.0] - 2022-11-14

### Added

- ReadFromPostgres transform for reading from the database.
- WriteToPostgres transform for writing to the database.

[unreleased]: https://github.com/medzin/beam-postgres/compare/0.2.1...HEAD
[0.2.1]: https://github.com/medzin/beam-postgres/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/medzin/beam-postgres/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/medzin/beam-postgres/releases/tag/0.1.0
