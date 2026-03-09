# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed
- **Critical: `read_inbox` with `wait_for_new=true` reliability issues** - Fixed multiple race conditions that caused timeouts even when messages existed:
  - Race condition between initial check and `fs.watch` setup (now properly awaited)
  - Silent failure on file lock contention (now retries with backoff)
  - `fs.watch` missing events (added 5-second fallback polling)
  - File vs directory watching (now always watches directory for atomic write support)
  - Missing final check on timeout (now always does final check before returning false)
  - `readInbox` early return after timeout (now always performs final read)

## [1.0.0] - 2025-01-XX

### Added
- Initial release
- Spawn teammates in tmux, Zellij, iTerm2, WezTerm, or Windows Terminal
- Shared task board with status tracking
- Agent messaging with direct and broadcast support
- Plan approval mode for critical changes
- Separate OS windows mode
- Quality gate hooks
- Thinking level control per teammate
- Smart model resolution with OAuth provider priority
