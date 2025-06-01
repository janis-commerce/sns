# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### [2.0.0] - 2025-06-01
### Added
- Added support for large payloads (>256KB) in SNS messages using S3 offloading
- Added new IAM permissions required for S3 offloading functionality
- Added `payloadFixedProperties` to improve error tracking and reporting

### Changed
- Updated SNS package to handle large payloads through S3 integration

### [1.1.0] - 2025-02-06
### Added
- Support for message attributes of type `String.Array`

### [1.0.2] - 2024-12-16
### Fixed
- Now `publishEvents()` splits events into batches of up to 10 entries (fixes **TooManyEntriesInBatchRequestException** SNS error)

### [1.0.1] - 2024-11-08
### Fixed
- Documentation now explains that SNS Client dependency must be manually imported

### [1.0.0] - 2024-10-18
### Added
- SNS trigger for individual and batch events
