# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[keep a changelog]: https://keepachangelog.com/en/1.0.0/
[semantic versioning]: https://semver.org/spec/v2.0.0.html

## [0.1.2] - 2023-01-05

### Fixed

- Bumped `dogmatiq/linger` to v1.0.0

## [0.1.1] - 2022-11-23

This release removes the _pessimistic_ data store lock.

This is an attempt to prevent issues with rolling restarts/deployments of an
application where the new version was prevented from becoming healthy because it
could not obtain the lock.

The data store continues to use an optimistic locking strategy to prevent
double-hanlding of messages or any kind of data corruption due to multiple
concurrent updates. However, this version of Verity is not designed to run
multiple instances of the same application concurrently. If you do run multiple
instances of the same application, the engine may periodically exit with an
optimistic concurrency error.

## [0.1.0]

- Initial release

<!-- references -->

[unreleased]: https://github.com/dogmatiq/verity
[0.1.0]: https://github.com/dogmatiq/verity/releases/tag/v0.1.0
[0.1.1]: https://github.com/dogmatiq/verity/releases/tag/v0.1.1
[0.1.2]: https://github.com/dogmatiq/verity/releases/tag/v0.1.2

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
