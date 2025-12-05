# SimDB — High-Performance Simulation Database

**SimDB** is a high-performance module that unifies **concurrent data pipelines** with **SQLite** to power scalable simulation data engines, analysis tooling, and UI backends. It is designed for performance modeling, functional modeling, and fast data collection/processing at scale. It provides a clean, modular foundation suitable for both simple and complex features in large simulation codebases.

SimDB is a header-only C++20 module whose only required dependency is sqlite3.

---

## Why choose SimDB?

### Unified Concurrent Pipelines + SQLite
SimDB creates a single, optimized data flow from simulation threads into a shared SQLite backend using move-only semantics. Its native integration with SQLite yields pipelines without database contention and with automatic batch-processing of database work on a single database thread.

### High Performance
SimDB is engineered for extremely high throughput under concurrency:

- Minimizes transaction overhead
- Eliminates database access contention
- Shares worker threads across multiple running apps for efficient scaling

### A Clean Break for Complex Codebases
Large simulation projects often suffer from tangled data paths, unbounded coupling, or legacy “write everything to files” architectures. SimDB replaces these with a clean, unified, highly optimized pipeline, helping teams reach performant solutions more quickly and with fewer moving parts.

---

## What Makes the Implementation Unique

SimDB abstracts away the pitfalls and complexities of using a filesystem-backed database concurrently:

- **No user-facing transactions required** — Avoid slow, tiny transactions and implicit file locks; SimDB batches and routes operations automatically.
- **Retry-on-fail implicit transactions** — Database transactions are retried until successful in the event of locked tables or other access issues.
- **No manual concurrency management** — No need to coordinate access to SQLite or worry about schema-level lock contention. SimDB ensures safe, high-throughput execution under heavy parallel workloads.
- **Unified output: “one DB to rule them all”** — Consolidates all simulation output into a single, queryable SQLite database: simulation stats, trace data, UI updates, analysis results, etc.
- **Shared threading model for scalability** — Multiple applications can run concurrently with the same database, and threads are shared appropriately for optimal scaling. Thread sharing occurs implicitly, though you can request dedicated threads for an app if need be (except the database thread; there is only one).

---

## What You Can Build with SimDB

SimDB enables a wide range of high-performance simulation and analysis workflows:

- Fast and scalable simulation statistics engines
- UI backends for live dashboards or post-simulation analysis
- Simulation state replayer backends
- Aggregate analysis across hundreds or thousands of simulations
- Pipeline collectors (pipeline viewer backend)

---

## Ideal For

- Simulation frameworks needing scalable, consistent data handling  
- Performance modelers requiring extremely fast, low-overhead pipelines  
- Researchers analyzing large batches of simulation experiments  
- UI/visualization tools with real-time or batch data access  
- Teams that need to refactor or decouple overly complex simulation infrastructure  

---

## Out-of-the-Box Pipeline Viewer

_Coming soon._

---

## SimDB Architecture

_Coming soon._

---

## Performance Benchmarks

_Coming soon._

---

## Code Snippets: Pipeline Creation

_Coming soon._

---

## Code Snippets: SQLite Interface

_Coming soon._

---

## Getting Started

_Coming soon._
