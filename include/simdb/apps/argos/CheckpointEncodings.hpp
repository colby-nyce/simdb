// <CheckpointEncodings.hpp> -*- C++ -*-

// clang-format off

#pragma once

/*!
 * \file CheckpointEncodings.hpp
 * \brief Reference for wire delta encodings emitted by PipelineStager checkpointers.
 *
 * The PipelineStager stage in ArgosCollector calls each collectable's checkpointer
 * (`ScalarCheckpointer`, `ContigContainerCheckpointer`, `SparseContainerCheckpointer`)
 * to turn staged checkpoints into wire records. Each record is prefixed with:
 *
 *   [uint16_t cid]     collectable ID
 *   [uint8_t action]   encoding (see Action in Checkpoint.hpp)
 *
 * Delta classification lives in CheckpointDeltas.hpp (`classifyContigChange`,
 * `classifySparseChange`). Heartbeat forcing, absent-CID refresh, and CLOSED priming
 * are handled in Checkpointer.hpp (`CollectableCheckpointer`).
 *
 * \par Encoding reference (tier 1)
 *
 * \verbatim
 * Encoding                 Applies To       Used When
 * ------------------------ ---------------- ------------------------------------------------------------------------------
 * CLOSED                   All              Collectable transitions to closed (`closeRecord`). The checkpointer may emit a
 *                                           priming FULL first when the last snapshot is outside the current heartbeat
 *                                           window.
 * FULL                     All              First data checkpoint (no prior baseline); heartbeat forces a snapshot; delta
 *                                           chain reaches `heartbeat - 1` consecutive non-FULL records; a CLOSED event
 *                                           occurred since the prior data checkpoint; scalar bytes changed; container
 *                                           change pattern is not covered by any delta; collectable did not stage this
 *                                           window but heartbeat refresh requires re-emitting state
 *                                           (`shouldRefreshAbsentCid_`).
 * CARRY                    All              Payload unchanged since the prior data checkpoint and none of the FULL forcing
 *                                           rules apply.
 * \endverbatim
 *
 * Scalars only ever emit CLOSED, FULL, or CARRY. Container delta encodings (tiers 2–4)
 * are reserved in the Action enum and documented in follow-on PRs.
 *
 * \par FULL forcing (checkpointer layer)
 *
 * These conditions override delta classification and force FULL regardless of change size:
 *
 * - \c forceSnapshot_(sim_time) — either `distance_to_snapshot_ + 1 >= heartbeat_`, or the
 *   last FULL sim-time is outside the current heartbeat window (`shouldHeartbeatRefresh_`).
 * - No prior data checkpoint — first collection for this CID.
 * - Closed since prior data checkpoint — a CLOSED lifecycle node sits between the current
 *   data checkpoint and the previous one.
 * - Absent CID refresh — collectable did not participate in this window but heartbeat
 *   policy still requires a wire record (replays latest snapshot as FULL, or CLOSED plus
 *   optional priming FULL if already closed).
 *
 * \par Wire payloads (after the action byte)
 *
 * | Action                 | Payload                                                |
 * |------------------------|--------------------------------------------------------|
 * | CLOSED                 |                                                        |
 * | CARRY                  |                                                        |
 * | FULL (scalar)          | [scalar bytes]                                         |
 * | FULL (contig)          | [count][bin bytes]..[bin bytes]                        |
 * | FULL (sparse)          | [count][bin idx][bin bytes]..[bin idx][bin bytes]      |
 *
 * \par Action enum tiers (Checkpoint.hpp)
 *
 * - Tier 1 (`0x00`–`0x0F`): lifecycle / common — CLOSED, FULL, CARRY; `0x03`–`0x0F` reserved
 * - Tier 2 (`0x10`–`0x1F`): any-container — CONTAINER_SWAP, CONTAINER_MULTI_SWAP;
 *   `0x12`–`0x1F` reserved
 * - Tier 3 (`0x20`–`0x2F`): contig-specific — CONTIG_ARRIVE, CONTIG_DEPART, CONTIG_BOOKENDS,
 *   CONTIG_MIMO; `0x24`–`0x2F` reserved
 * - Tier 4 (`0x30`–`0x3F`): sparse-specific — SPARSE_REMOVE, SPARSE_ADD, SPARSE_MULTI_REMOVE;
 *   `0x33`–`0x3F` reserved
 *
 * \par Related implementation files
 *
 * - Checkpoint.hpp:       Action enum, encode/decode per checkpoint type
 * - CheckpointDeltas.hpp: classifyContigChange, classifySparseChange
 * - Checkpointer.hpp:     heartbeat bookkeeping, encodeForPipeline orchestration
 * - ArgosCollector.hpp:   PipelineStager invokes checkpointer encoding
 */

// clang-format on
