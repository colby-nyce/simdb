# Doxygen documentation burndown list

Classes/structs below have minimal or no Doxygen compared to the rest of the codebase (e.g. no `\class` block or only a one-line `///`). Target: add a `/*! \class Name \brief ... */` (or `\struct`) block and, where useful, `\param` / `\return` on public API.

**Reference style** (well-documented): `DatabaseManager`, `Column`, `Table`, `Schema`, `Connection`, `Transaction`, `SqlQuery`, `SqlRecord`, `ResultWriterBase` and all `ResultWriter*` in Iterator.hpp, `CollectionBuffer`, `InterruptException`, `SqlBlob` (Blob.hpp).

---

## include/simdb/sqlite

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `PreparedINSERT` | PreparedINSERT.hpp | One-line `///` only; no `\class` block |
| `ValueContainerBase` | ValueContainer.hpp | Has full block ✓ |
| `Integral32ValueContainer` | ValueContainer.hpp | One-line `///` only |
| `IntegralU32ValueContainer` | ValueContainer.hpp | One-line `///` only |
| `Integral64ValueContainer` | ValueContainer.hpp | One-line `///` only |
| `IntegralU64ValueContainer` | ValueContainer.hpp | One-line `///` only |
| `FloatingPointValueContainer` | ValueContainer.hpp | One-line `///` only |
| `StringValueContainer` | ValueContainer.hpp | One-line `///` only |
| `BlobValueContainer` | ValueContainer.hpp | One-line `///` only |
| `VectorValueContainer<T>` | ValueContainer.hpp | One-line `///` only |

---

## include/simdb/schema

| Class / struct | File | Current docs |
|----------------|------|--------------|
| *(none remaining)* | | |

---

## include/simdb/utils

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `ThreadSafeLogger` | ThreadSafeLogger.hpp | No Doxygen; `//` comments only. Inner `Guard` also undocumented |
| `RunningMean` | RunningMean.hpp | `///` on class and methods; no `\class` block |
| `SelfProfiler` | TickTock.hpp | No Doxygen at all |

---

## include/simdb/pipeline

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `PollingThread` | PollingThread.hpp | `///` only; no `\class` block |
| `PipelineManager` | PipelineManager.hpp | `///` only; no `\class` block |
| `Stage` | Stage.hpp | No class-level Doxygen |
| `DatabaseStageBase` | Stage.hpp | No class-level Doxygen |
| `DatabaseStage<AppT>` | Stage.hpp | No class-level Doxygen |
| `Runnable` | Runnable.hpp | `///` only; no `\class` block |
| `ScopedRunnableDisabler` | Runnable.hpp | `///` only; no `\class` block |
| `QueueBase` | Queue.hpp | `///` only; no `\class` block |
| `Queue<T>` | Queue.hpp | `///` only; no `\class` block |
| `QueuePlaceholder` | QueueRepo.hpp | No class-level docs |
| `InputQueuePlaceholder<T>` | QueueRepo.hpp | Only constructor `///` |
| `OutputQueuePlaceholder<T>` | QueueRepo.hpp | Only constructor `///` |
| `StageQueueRepo` | QueueRepo.hpp | No class-level docs |
| `PipelineQueueRepo` | QueueRepo.hpp | No class-level docs |
| `Pipeline` | Pipeline.hpp | Multi-line `///` but no `\class` block |
| `Flusher` | Flusher.hpp | `///` only; no `\class` block |
| `FlusherWithTransaction` | Flusher.hpp | `///` only; no `\class` block |
| `DatabaseThread` | DatabaseThread.hpp | Multi-line `///`; no `\class` block |
| `DatabaseAccessor` | DatabaseAccessor.hpp | `///` only; no `\class` block |
| `AsyncDatabaseTask` | AsyncDatabaseAccessor.hpp | `///` only; no `\struct` block |
| `AsyncDatabaseAccessHandler` | AsyncDatabaseAccessor.hpp | `///` only; no `\class` block |
| `AsyncDatabaseAccessor` | AsyncDatabaseAccessor.hpp | `///` only; no `\class` block |
| `ThreadMerger` | ThreadMerger.hpp | `///` only; no `\class` block |
| `PipelineSnooper<KeyType,SnoopedType>` | PipelineSnooper.hpp | No Doxygen |

---

## include/simdb/Exceptions.hpp

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `DBException` | Exceptions.hpp | One-line `///` only |
| `SafeTransactionSilentException` | Exceptions.hpp | One-line `///` only |

---

## include/simdb/apps

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `App` | App.hpp | `///` only; no `\class` block |
| `AppFactoryBase` | App.hpp | No Doxygen |
| `AppFactory<AppT>` | App.hpp | No Doxygen |
| `AppRegistrationBase` | AppManager.hpp | No Doxygen |
| `AppRegistration<AppT>` | AppManager.hpp | No Doxygen |
| `AppManager` | AppManager.hpp | Long method docs; class has only `///` |
| `AppManagers` | AppManager.hpp | `///` only; no `\class` block |
| `AppRegistrations` | AppManager.hpp | One-line `///` only |

---

## include/simdb/apps/argos

| Class / struct | File | Current docs |
|----------------|------|--------------|
| `TreeNode` | TreeNode.hpp | `///` multi-line; could add `\struct` block |
| `ArgosRecord` | CollectionPoints.hpp | `///` on struct; no `\struct` block |
| `TickReader` | CollectionPoints.hpp | No class-level Doxygen |
| `CollectionPointBase` | CollectionPoints.hpp | `///` only; no `\class` block |
| `CollectionPoint` | CollectionPoints.hpp | No class-level Doxygen (inherits from CollectionPointBase) |
| `ContigIterableCollectionPoint` | CollectionPoints.hpp | No class-level Doxygen |
| `SparseIterableCollectionPoint` | CollectionPoints.hpp | No class-level Doxygen |

---

## Table.hpp copy-paste fix

In **include/simdb/sqlite/Table.hpp**, the block before `SqlColumns` and the block before `SqlValues` both say `\class SqlTable`. Those should be corrected to `\class SqlColumns` and `\class SqlValues` respectively.

---

## Summary counts

- **Completed:** 1 (SqlBlob).
- **sqlite:** 1 class with full block; 10 with minimal (PreparedINSERT + ValueContainer family).
- **schema:** 0 remaining.
- **utils:** 3 (ThreadSafeLogger, RunningMean, SelfProfiler).
- **pipeline:** 24 classes/templates with minimal or no class-level docs.
- **Exceptions:** 2 with one-liner only.
- **apps:** 8 (App, AppFactoryBase, AppFactory, AppRegistrationBase, AppRegistration, AppManager, AppManagers, AppRegistrations).
- **apps/argos:** 7 (TreeNode, ArgosRecord, TickReader, CollectionPointBase, CollectionPoint, ContigIterableCollectionPoint, SparseIterableCollectionPoint).

**Total remaining:** ~55 class/struct entries (excluding the Table.hpp copy-paste fix).
