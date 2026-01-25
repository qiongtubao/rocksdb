
# RocksDB Detailed Write Path Flow

This document outlines the detailed flow of a write operation in RocksDB, from the initial `Put`/`Delete` call to the data being flushed to a Level 0 SST file.

```mermaid
graph TD
    subgraph Client
        A[DB::Put / DB::Delete] --> B[DBImpl::Write]
    end

    subgraph DBImpl::WriteImpl [Write Path in DBImpl]
        B --> C{Protection Info?}
        C -- Yes --> D[UpdateProtectionInfo]
        C -- No --> E[WriteImpl]
        D --> E
        
        E --> F[Create WriteThread::Writer]
        F --> G[JoinBatchGroup]
        G --> H{Is Leader?}
        
        H -- No --> H1[Wait for Leader]
        H -- Yes --> I[EnterAsBatchGroupLeader]
        I --> J[Group Multiple Batches]
        
        subgraph WAL [Write Ahead Log]
            J --> K{Disable WAL?}
            K -- No --> L[WriteToWAL]
            L --> L1[Sync Log?]
        end
        
        subgraph MemTableWrite [MemTable Insertion]
            L --> M{Concurrent MemTable Write?}
            K -- Yes --> M
            
            M -- No --> N[WriteBatchInternal::InsertInto (Serial)]
            M -- Yes --> O[LaunchParallelMemTableWriters]
            
            N --> P[Iterate Batch]
            O --> P
            
            P --> Q[MemTable::Add]
        end
        
        Q --> R[ExitAsBatchGroupLeader]
        R --> S[Notify Followers]
        
        H1 --> T[Complete]
        S --> T
    end

    subgraph MemTableAdd [MemTable::Add Detail]
        Q --> U[Calculate Encoded Length]
        U --> V[Allocate Memory (Arena/SkipListRep)]
        V --> W[Encode Internal Key]
        W --> X[Pack Sequence & Type]
        X --> Y[InsertKey (SkipList)]
        Y --> Z[Update Bloom Filter & Stats]
        Y --> ZZ[Check ShouldFlushNow]
    end

    subgraph Flush [Flush to Level 0]
        ZZ -- Yes --> FA[Switch MemTable]
        FA --> FB[Background Flush Scheduled]
        FB --> FC[DBImpl::FlushMemTableToOutputFile]
        
        FC --> FD[Create FlushJob]
        FD --> FE[PickMemTable (Immutable)]
        FE --> FF[FlushJob::Run]
        
        FF --> FG[Iterate MemTable]
        FG --> FH[BuildTable (Create SST File)]
        FH --> FI[InstallSuperVersion]
        FI --> FJ[Update Manifest]
    end
```

## detailed Steps

### 1. Client Write Request (`DBImpl::Write`)
The write operation starts with `DBImpl::Put`, `Delete`, or `Merge`, which wraps the operation into a `WriteBatch` and calls `DBImpl::Write`.

### 2. Batch Grouping (`WriteImpl`)
RocksDB uses a group commit mechanism using `WriteThread`.
- **JoinBatchGroup**: The current thread joins the write queue.
- **Leader Selection**: The first thread in the queue becomes the "Leader". Other threads become "Followers".
- **Grouping**: The Leader groups the batches from waiting Followers into a single `WriteGroup` to maximize I/O throughput.

### 3. Write Ahead Log (WAL)
If `disableWAL` is false, the Leader writes the consolidated `WriteGroup` to the WAL. This ensures durability in case of a crash.

### 4. MemTable Insertion
The Leader (and potentially Followers if concurrent write is enabled) inserts the data into the active `MemTable`.
- **`MemTable::Add`**:
    1.  **Calculate Size**: Determines the total size needed for the key, value, and metadata.
    2.  **Allocate**: Allocates memory from the MemTable's Arena (via `SkipListRep`).
    3.  **Encode**: Encodes the Internal Key (`User Key` + `Sequence Number` + `Value Type`).
    4.  **Insert**: Inserts the encoded entry into the SkipList.
    5.  **Filter**: Updates the Bloom Filter (if enabled).

### 5. Flush to Level 0
When the `MemTable` fills up (based on `write_buffer_size`):
1.  **Switch**: The active `MemTable` becomes immutable (`ImmutableMemTable`), and a new one is created.
2.  **Schedule**: A background flush job is triggered.
3.  **`FlushMemTableToOutputFile`**:
    -   **PickMemTable**: Selects the immutable MemTable(s) to flush.
    -   **Run**: Iterates over the SkipList and writes the key-value pairs to a new SST file on disk (Level 0).
    -   **InstallSuperVersion**: Updates the global `VersionSet` to include the new SST file and removes the flushed MemTable.
