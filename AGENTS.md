# Repository Guidelines

## Project Structure & Module Organization

This C++17 project implements ZeroMQ request/reply messaging, Raft elections and heartbeats, and durable term/vote and log storage. AppendEntries replicates stored logs; majority commit and state-machine execution are not implemented yet.

- `src/common/`: message encoding, socket helpers, and logging.
- `src/server/`: node configuration, election logic, and server lifecycle; `rpc/` contains peer RPCs and `storage/` contains persistence.
- `src/client/`: business request client.
- `tests/`: unit and process-level integration tests.
- `cmake/`: dependency and testing setup; `scripts/start_demo.sh`: tmux cluster demo.
- `doc/`: protocol explanations and implementation notes. Generated artifacts belong in ignored `build/` directories.

## Build, Test, and Development Commands

Use CMake 3.24+, Ninja, and a C++17 compiler. The current storage backend requires Linux/POSIX.

```sh
cmake --preset debug                      # Configure; initially downloads dependencies
cmake --build --preset debug --parallel    # Build applications and tests
ctest --preset debug                      # Run GoogleTest cases through CTest
./build/debug/zmq_server --help            # Inspect server options
./build/debug/zmq_server                   # Start a local server
./build/debug/zmq_client                   # Send a request from another terminal
./scripts/start_demo.sh --no-build         # Launch the default three-node tmux demo
```

Replace `debug` with `release` for optimized builds. Installed dependencies can be selected with `-DZMQ_TEMPLATE_USE_SYSTEM_DEPS=ON` during configuration.

## Coding Style & Naming Conventions

Use four-space indentation, same-line opening braces, and `.hpp`/`.cpp` file pairs. Follow adjacent code: snake_case filenames and functions, PascalCase for most types, and trailing underscores for private members. Keep implementation helpers in anonymous namespaces and project code under `zmq_template`. No formatter or linter configuration is checked in; preserve local style and resolve compiler warnings.

## Testing Guidelines

Use GoogleTest with `TEST(Suite, Behavior)` or `TEST_F(Fixture, Behavior)` in `tests/*_test.cpp`. Register new test sources in `CMakeLists.txt`. Cover changed behavior, especially protocol validation, term transitions, and persistence failures. No numeric coverage threshold is configured. Run focused cases with `ctest --test-dir build/debug -R ConfigParser --output-on-failure`, then the full suite. Integration tests require local socket permissions.

## Commit & Pull Request Guidelines

Follow existing commit subjects: `feat:`, `fix:`, `refactor:`, or `docs:` followed by a concise Chinese description. Split changes into focused commits by feature or concern. PR descriptions should explain the resulting behavior, relevant issues, and validation commands/results. Update documentation when CLI options, protocol behavior, or storage paths change.

## Runtime Data

Default state lives at `<executable-directory>/data/node-<id>`. Relative `--data-dir` paths resolve against the working directory. Give each node its own directory; never commit runtime state or overwrite existing data during tests.
