# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### [1.0.2] - 2024-12-16
### Fixed
- Now `publishEvents()` splits events into batches of up to 10 entries (fixes **TooManyEntriesInBatchRequestException** SNS error)

### [1.0.1] - 2024-11-08
### Fixed
- Documentation now explains that SNS Client dependency must be manually imported

### [1.0.0] - 2024-10-18
### Added
- SNS trigger for individual and batch events
