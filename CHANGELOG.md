# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[keep a changelog]: https://keepachangelog.com/en/1.0.0/
[semantic versioning]: https://semver.org/spec/v2.0.0.html

## Unreleased

### Fixed

- `Engine.ExecuteCommand()` now panics when passed an unrecognized command type

## [0.3.3] - 2024-10-10

### Fixed

- Remove erroneous use of stubbed marshaler in `Packer` implementation.

## [0.3.2] - 2024-10-10

### Fixed

- Fixed issue that prevented command types from being recognized by the
  `dogma.CommandExecutor` implementation.

## [0.3.1] - 2024-10-05

### Changed

- Bump `configkit` to v0.16.0, and use `enginekit/message` instead of
  `configkit/message`, which has been removed.

## [0.3.0] - 2024-10-03

- Bumped Dogma to v0.15.0, which requires passing validation scopes to each
  message's `Validate()` method. As these interfaces currently contain no
  methods, Verity simply passes `nil`.

## [0.2.4] - 2024-09-30

### Changed

- Bumped Dogma to v0.14.3
- Replaced usage of the deprecated `marshalkit` module with `enginekit/marshaler`.

## [0.2.3] - 2024-08-17

### Changed

- Bumped Dogma to v0.14.0.

## [0.2.2] - 2024-07-16

### Added

- Added support for `Disable()` method in `dogmatiq/dogma` v0.13.1.

## [0.2.1] - 2024-06-21

### Changed

- Bump `dogmatiq/marshalkit` to v0.7.4, which introduces better handling of
  messages with conflicting names.
- Add the new `stateless.DefaultCodec` to the default set of marshaling codecs
  as a special case for `dogma.StatelessProcessRoot` values that does not depend
  on any other specific code, such as Protocol Buffers or JSON.

## [0.2.0] - 2024-03-26

- **[BC]** Bump `dogmatiq/dogma` to v0.13.0.

## [0.1.8] - 2023-05-03

### Fixed

- Fixed `nil is not a message` error in event stream server.

### Removed

- Removed `networkstream.NoopUnmarshaler`. This marshaler served as an
  optimization by skipping message unmarshaling when only the binary message
  data is needed. Unfortunately, this approach is incompatible with
  `dogmatiq/marshalkit` as of v0.7.3. `marshalkit` now explicitly requires
  unmarshaled messages to implement the `dogma.Message` interface, because this
  interface is no longer equivalent to `any`, as of Dogma v0.12.0.

## [0.1.7] - 2023-05-03

### Added

- Enabled gRPC reflection service

## [0.1.6] - 2023-04-11

### Changed

- `WithLogger()` now accepts either a `*zap.Logger` or Dodeca `logging.Logger`

## [0.1.5] - 2023-04-09

This release updates Verity to adhere to Dogma v0.12.0 interfaces. Please note
that Verity cannot support any projection delivery policies other than the
default `UnicastProjectionDeliveryPolicy`.

## [0.1.4] - 2023-03-27

- Bump `dogmatiq/marshalkit` to v0.7.3

## [0.1.3] - 2023-01-18

### Fixed

- The `ExecuteCommand()`, `RecordEvent()` or `ScheduleTimeout()` methods on the
  engine and handler scopes now panic if passed an invalid message.

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
[0.1.3]: https://github.com/dogmatiq/verity/releases/tag/v0.1.3
[0.1.4]: https://github.com/dogmatiq/verity/releases/tag/v0.1.4
[0.1.5]: https://github.com/dogmatiq/verity/releases/tag/v0.1.5
[0.1.6]: https://github.com/dogmatiq/verity/releases/tag/v0.1.6
[0.1.7]: https://github.com/dogmatiq/verity/releases/tag/v0.1.7
[0.2.0]: https://github.com/dogmatiq/verity/releases/tag/v0.2.0
[0.2.1]: https://github.com/dogmatiq/verity/releases/tag/v0.2.1
[0.2.2]: https://github.com/dogmatiq/verity/releases/tag/v0.2.2
[0.2.3]: https://github.com/dogmatiq/verity/releases/tag/v0.2.3
[0.2.4]: https://github.com/dogmatiq/verity/releases/tag/v0.2.4
[0.3.0]: https://github.com/dogmatiq/verity/releases/tag/v0.3.0
[0.3.1]: https://github.com/dogmatiq/verity/releases/tag/v0.3.1
[0.3.2]: https://github.com/dogmatiq/verity/releases/tag/v0.3.2
[0.3.3]: https://github.com/dogmatiq/verity/releases/tag/v0.3.3

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
