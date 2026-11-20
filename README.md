# WhatsApp Linux Client

A lightweight, native Linux desktop client concept built with Rust. The
project separates a persistent background daemon from the graphical
desktop application so that the connection and synchronization layer can
continue running when the UI window is closed.

> **Important compatibility and account-risk notice**
>
> This project is intended to explore a native Linux client using an
> unofficial WhatsApp protocol implementation. WhatsApp does not provide
> a general-purpose official API for building an independent personal
> desktop client. Using unofficial clients may violate WhatsApp's terms
> or trigger temporary or permanent account restrictions, even when
> messages are sent manually and no bulk automation is used. There is no
> guarantee that an account or protocol implementation will remain
> supported. Do not develop or test with an account whose loss would be
> unacceptable. Review WhatsApp's current terms and policies before use.

## Project goals

-   Provide a lightweight native Linux desktop experience without
    Electron or a bundled Chromium browser.
-   Keep connection management and synchronization independent of the UI
    window.
-   Cache chats and messages locally for responsive startup and offline
    history access.
-   Integrate with Linux desktop notifications and system services.
-   Keep protocol, application logic, persistence, and UI concerns
    isolated so components can be tested or replaced independently.

## Non-goals

-   This project is not an official WhatsApp client and is not endorsed
    by or affiliated with WhatsApp or Meta.
-   It does not guarantee compatibility with WhatsApp's changing private
    protocol.
-   It is not intended for spam, bulk messaging, unsolicited messaging,
    or evading platform enforcement.
-   It does not promise that the daemon can remain connected through
    logout, suspend, network loss, or every desktop environment state.

## Architecture

The application is split into a headless daemon and a native desktop UI.

``` text
┌────────────────────────────────────────────────────────────┐
│                    Native Desktop UI                       │
│                 GPUI (initial preference)                  │
│                                                            │
│  Chat list · Conversation · Composer · Search · Settings   │
└──────────────────────────────┬─────────────────────────────┘
                               │
                 Versioned IPC over Unix socket
                               │
┌──────────────────────────────┴─────────────────────────────┐
│                    whatsapp-daemon                         │
│                                                            │
│  Application services                                      │
│  ├── Connection manager                                    │
│  ├── Synchronization coordinator                           │
│  ├── Chat and message services                              │
│  ├── Notification manager                                  │
│  └── Media manager                                         │
│                                                            │
│  ┌────────────────────────┐  ┌───────────────────────────┐  │
│  │ Protocol adapter       │  │ Persistence layer        │  │
│  │ Unofficial protocol    │  │ SQLite · migrations      │  │
│  │ implementation         │  │ message/cache metadata   │  │
│  └────────────────────────┘  └───────────────────────────┘  │
└──────────────────────────────┬─────────────────────────────┘
                               │
             Linux desktop integration
       D-Bus · Secret Service · systemd user service
```

### Core design principles

1.  **The UI does not talk directly to the protocol library.** All UI
    operations go through application services exposed by the daemon.
2.  **The daemon owns connection state and synchronization.** Closing
    the UI should not stop the daemon when background mode is enabled.
3.  **The database is the local source of cached application state.**
    Incoming updates are persisted before they are broadcast to
    connected UI clients.
4.  **IPC is versioned and validated.** Requests, responses, and events
    use shared typed data-transfer objects.
5.  **Protocol-specific details stay behind an adapter.** The rest of
    the application should not depend on a particular unofficial
    protocol crate.

## Technology stack

  -----------------------------------------------------------------------
  Area                    Preferred technology    Notes
  ----------------------- ----------------------- -----------------------
  Language                Rust                    Workspace-based project

  Async runtime           Tokio                   Daemon tasks and socket
                                                  server

  Desktop UI              GPUI                    Initial preference;
                                                  benchmark against Iced
                                                  before final commitment

  Serialization           Serde + JSON            Simple, inspectable IPC
                                                  payloads

  IPC                     Unix Domain Socket      Local daemon/UI
                                                  communication

  Database                SQLite + SQLx           Migrations and
                                                  asynchronous access

  Logging                 `tracing`               Structured logs and
                                                  diagnostics

  Error handling          `thiserror` + `anyhow`  Typed library errors
                                                  and application
                                                  boundaries

  Linux notifications     D-Bus / `notify-rust`   Depends on
                                                  desktop-session
                                                  availability

  Credentials             Linux Secret Service /  Do not store raw
                          keyring                 credentials in ordinary
                                                  DB fields

  Service management      systemd user service    Start-on-login and
                                                  restart policy

  Media                   Local filesystem +      Keep large media blobs
                          SQLite metadata         out of the database
  -----------------------------------------------------------------------

These are proposed choices, not a claim that every integration has
already been implemented or validated. GPUI and Iced should be compared
with a small prototype before committing to the UI framework.

## Workspace layout

``` text
whatsapp-linux-client/
├── Cargo.toml
├── README.md
├── crates/
│   ├── whatsapp-daemon/
│   │   └── src/
│   │       ├── main.rs
│   │       ├── ipc/
│   │       ├── application/
│   │       ├── connection/
│   │       ├── notifications/
│   │       └── media/
│   ├── whatsapp-ui/
│   │   └── src/
│   │       ├── main.rs
│   │       ├── screens/
│   │       ├── components/
│   │       └── state/
│   ├── whatsapp-protocol/
│   │   └── src/
│   │       ├── lib.rs
│   │       └── adapter.rs
│   ├── whatsapp-storage/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── models/
│   │       └── migrations/
│   └── whatsapp-ipc/
│       └── src/
│           ├── lib.rs
│           ├── requests.rs
│           ├── responses.rs
│           └── events.rs
├── packaging/
│   └── systemd/
│       └── whatsapp-daemon.service
└── docs/
    ├── architecture.md
    ├── ipc.md
    └── security.md
```

The exact crate boundaries can evolve during implementation. The key
requirement is to keep protocol, storage, IPC contracts, application
services, and UI independently testable.

## Main components

### 1. Background daemon (`whatsapp-daemon`)

Responsibilities:

-   Own the protocol connection lifecycle.
-   Coordinate login, pairing, reconnect attempts, and synchronization.
-   Expose local IPC endpoints to the UI.
-   Persist incoming messages and state changes.
-   Manage media downloads and local metadata.
-   Dispatch desktop notifications when a graphical session is
    available.
-   Provide health and diagnostic information.

The daemon should use bounded queues, cancellation-aware tasks,
structured logging, and explicit shutdown handling. Reconnection should
use capped exponential backoff with jitter rather than tight retry
loops.

### 2. Protocol adapter (`whatsapp-protocol`)

Define an application-facing interface so the rest of the project does
not depend on a specific protocol library.

Illustrative interface:

``` rust
#[async_trait::async_trait]
pub trait MessagingProtocol: Send + Sync {
    async fn connect(&self) -> anyhow::Result<()>;
    async fn disconnect(&self) -> anyhow::Result<()>;
    async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
    ) -> anyhow::Result<()>;
}
```

This is an architectural example, not a complete protocol contract. The
real interface must reflect the selected library's capabilities and
limitations.

Before adopting any crate, verify its current maintenance status,
license, authentication and multi-device support, synchronization
behaviour, media handling, security posture, and compatibility with
current WhatsApp behaviour. Do not assume feature support based on a
crate name or an old example.

### 3. Persistence (`whatsapp-storage`)

Use SQLite for structured local state and the filesystem for media.

Recommended initial data domains:

-   Chats and chat metadata.
-   Contacts and participant metadata.
-   Messages and message status.
-   Synchronization cursors or checkpoints, where supported by the
    protocol adapter.
-   Media metadata and local file references.
-   Application preferences that are not secrets.

Database requirements:

-   Enable WAL mode where appropriate.
-   Use versioned migrations.
-   Add indexes for chat identifiers, message identifiers, and message
    timestamps.
-   Use transactions for related synchronization updates.
-   Enforce unique message identity constraints where reliable
    identifiers are available.
-   Design pagination rather than loading entire conversations into
    memory.
-   Provide backup, restore, and corruption-recovery procedures.

Store authentication secrets in the desktop Secret Service or another
suitable OS credential store. Do not treat SQLite encryption or
filesystem permissions as a substitute for secure credential handling.

### 4. IPC (`whatsapp-ipc`)

Use a Unix Domain Socket for local communication.

The IPC contract should define:

-   Versioned request and response envelopes.
-   Typed request, response, and event payloads.
-   Explicit error codes and human-readable messages.
-   A subscription mechanism for live events.
-   Bounded message sizes and queue limits.
-   Connection lifecycle and cancellation behaviour.
-   Compatibility behaviour when UI and daemon versions differ.

Use restrictive socket and data-directory permissions. Validate every
request, and do not assume that a local socket is automatically safe
from other local processes.

JSON over the socket is a good starting point because it is easy to
inspect and debug. Protobuf can be evaluated later if measured needs
justify it.

### 5. Desktop UI (`whatsapp-ui`)

The UI should be a client of the daemon, not the owner of the WhatsApp
connection.

Initial screens:

-   QR/pairing and connection status.
-   Searchable chat list.
-   Conversation view and message composer.
-   Media preview.
-   Settings and diagnostics.

The UI should request cached state on startup, then subscribe to daemon
events. It should tolerate daemon restarts and reconnect to IPC without
losing unsaved UI state.

### 6. Linux integration

-   **systemd user service:** optional start-on-login and
    restart-on-failure.
-   **D-Bus notifications:** show incoming-message notifications when a
    user session is available.
-   **Secret Service:** store session credentials securely.
-   **System tray:** optional status and quick actions using the
    desktop's supported tray mechanism.
-   **XDG directories:** use standard locations for application data,
    configuration, cache, and runtime sockets.

A systemd user service does not guarantee operation after logout or
during suspend. Behaviour depends on user lingering configuration,
desktop session state, D-Bus availability, and system policy. Document
these conditions rather than promising uninterrupted connectivity.

## Daemon lifecycle

Expected behaviour:

1.  The user logs into the desktop session.
2.  The daemon starts if background mode is enabled.
3.  The daemon loads non-secret configuration and retrieves credentials
    from the OS credential store.
4.  It initializes the database and protocol adapter.
5.  It attempts to connect and reports its state over IPC.
6.  Incoming events are validated and persisted.
7.  Persisted updates are broadcast to connected UI clients.
8.  Notifications are dispatched when supported by the active desktop
    session.
9.  Closing the UI disconnects only the UI from IPC; the daemon
    continues if configured to do so.
10. A full quit action stops the daemon cleanly.

The application should provide explicit modes:

-   **Background mode:** keep the daemon running after the window
    closes.
-   **Quit completely:** stop both UI and daemon.
-   **Start on login:** user-configurable service startup.

## Synchronization and reliability

Synchronization is a critical subsystem, not a finishing feature.

The implementation should account for:

-   Network loss and recovery.
-   Laptop suspend and resume.
-   Daemon restarts.
-   Duplicate event delivery.
-   Partial synchronization and retry.
-   Database transaction failures.
-   Out-of-order events, where possible.
-   UI disconnects while events continue arriving.
-   Media downloads that fail or are interrupted.

Use idempotent persistence operations and stable protocol-provided
identifiers wherever available. Do not claim exactly-once delivery
unless the protocol and implementation can actually guarantee it.

A key acceptance scenario is: disconnect the computer from the network,
allow messages to arrive elsewhere, reconnect the computer, and verify
that supported synchronization recovers the missed state without
creating duplicate local messages.

## Security and privacy

-   Treat protocol credentials, identity keys, and session material as
    highly sensitive.
-   Use the OS credential store for secrets.
-   Restrict permissions on data, cache, and runtime directories.
-   Restrict and validate IPC clients.
-   Avoid logging message contents, credentials, QR payloads, or
    cryptographic material.
-   Keep dependencies updated and review licenses and security
    advisories.
-   Provide a clear logout/session-removal operation.
-   Explain what is stored locally and how to delete it.
-   Protect media files with appropriate user-only filesystem
    permissions.

This project does not provide a guarantee of end-to-end security merely
by being written in Rust. Security depends on the protocol
implementation, key handling, storage, dependencies, and operational
practices.

## Development roadmap

### Milestone 1 --- Protocol feasibility

Before building the full application, create a small isolated proof of
concept to evaluate the selected protocol implementation.

Verify, to the extent supported:

-   Pairing and authentication.
-   Connection and reconnection.
-   Receiving and sending a basic text message.
-   Initial and subsequent synchronization.
-   Session persistence across process restarts.
-   Media and group-chat capability requirements.
-   Error handling and compatibility limitations.

Stop and reassess if the library cannot reliably support the minimum
required use cases. Do not make your primary account the test account.

### Milestone 2 --- Daemon foundation

-   Create the Cargo workspace.
-   Set up Tokio and structured logging.
-   Define application service boundaries.
-   Add a protocol adapter interface.
-   Implement daemon startup, shutdown, and health reporting.

### Milestone 3 --- Storage and synchronization

-   Add SQLite and migrations.
-   Implement chat and message persistence.
-   Add deduplication and transactional updates.
-   Add pagination and offline history.
-   Test restart and recovery scenarios.

### Milestone 4 --- IPC

-   Define shared DTOs and protocol versioning.
-   Implement the Unix socket server and client.
-   Add request/response handling and event subscriptions.
-   Add socket permissions, validation, and error handling.

### Milestone 5 --- Desktop UI

-   Prototype GPUI and compare it with Iced.
-   Build pairing, chat list, conversation, composer, and settings
    screens.
-   Connect UI state to daemon snapshots and events.
-   Handle daemon restarts and IPC reconnection.

### Milestone 6 --- Linux integration and media

-   Add D-Bus notifications.
-   Add optional systemd user-service integration.
-   Add secure credential storage.
-   Implement media download, caching, and preview.
-   Add optional system tray support.

### Milestone 7 --- Reliability, security, and optimization

-   Test network interruptions and suspend/resume.
-   Test database recovery and backup/restore.
-   Review IPC and filesystem permissions.
-   Audit dependency and credential handling.
-   Measure startup time, idle RAM, CPU usage, and responsiveness.
-   Test on target GNOME and KDE environments.

## Testing strategy

At minimum, build automated tests for:

-   Database migrations and transaction rollback.
-   Message deduplication.
-   IPC serialization and version mismatch.
-   IPC client disconnect and reconnect.
-   Daemon graceful shutdown.
-   Retry/backoff behaviour.
-   Synchronization recovery using recorded or mocked protocol events.
-   Media download interruption and cleanup.
-   Credential retrieval failure.
-   Notification behaviour when D-Bus is unavailable.

Keep protocol integration tests separate from ordinary unit tests so
that protocol instability does not make the whole test suite unreliable.

## Definition of done for an initial usable release

An initial release should not be considered usable until it can:

-   Start and stop cleanly.
-   Pair using the selected supported-by-the-adapter login flow.
-   Display cached chats and messages.
-   Receive and send the minimum supported message types.
-   Recover from ordinary network interruptions.
-   Avoid duplicate local message records in tested retry scenarios.
-   Continue daemon operation after the UI closes when background mode
    is enabled.
-   Reconnect the UI to an already-running daemon.
-   Store credentials outside ordinary plaintext application database
    fields.
-   Explain known protocol, account, and desktop-session limitations to
    users.

## Building and running

The repository is an architecture and implementation plan until the
crates and packaging are actually created. Once the workspace has been
implemented, the intended development commands are:

``` bash
# Build the workspace
cargo build --workspace

# Run tests
cargo test --workspace

# Run the daemon (after implementation)
cargo run -p whatsapp-daemon

# Run the UI (after implementation)
cargo run -p whatsapp-ui
```

These commands describe the intended workspace workflow; they will only
work after the corresponding crates and binaries exist.

## Project status

**Status:** Architecture / pre-implementation planning.

The proposed architecture is not a claim that a working client, protocol
integration, or production-ready security model already exists. The
first engineering milestone is to validate the protocol dependency and
its compatibility, then implement the daemon and its storage/IPC
foundations.

## Disclaimer

WhatsApp is a trademark of WhatsApp LLC. This is an independent,
unofficial project and is not affiliated with, endorsed by, or sponsored
by WhatsApp or Meta. Use of unofficial clients may violate applicable
terms and may result in account restrictions. Users are responsible for
reviewing current platform terms, applicable law, and the risks before
connecting an account.
