# Schema Registry Migration Flow

## Overall Migration Process

```mermaid
flowchart TD
    Start([Start Migration])
    
    %% Global Mode Setup
    CheckImportMode{DEST_IMPORT_MODE=true?}
    SetGlobalImport[Set Global Mode to IMPORT]
    
    %% Cleanup Phase
    CheckCleanup{CLEANUP_DESTINATION=true?}
    CleanupDest[Clean Destination Registry]
    
    %% Migration Phase
    CheckMigration{ENABLE_MIGRATION=true?}
    CompareOnly[Compare Schemas Only]
    
    %% Dry Run Check
    CheckDryRun{DRY_RUN=true?}
    DryRunMigration[Simulate Migration]
    ActualMigration[Perform Migration]
    
    %% Post Migration
    CheckModeAfter{DEST_MODE_AFTER_MIGRATION set?}
    SetGlobalModeAfter[Set Global Mode to specified value]
    
    End([End])
    
    %% Flow
    Start --> CheckImportMode
    CheckImportMode -->|Yes| SetGlobalImport
    CheckImportMode -->|No| CheckCleanup
    SetGlobalImport --> CheckCleanup
    
    CheckCleanup -->|Yes| CleanupDest
    CheckCleanup -->|No| CheckMigration
    CleanupDest --> CheckMigration
    
    CheckMigration -->|No| CompareOnly
    CheckMigration -->|Yes| CheckDryRun
    CompareOnly --> End
    
    CheckDryRun -->|Yes| DryRunMigration
    CheckDryRun -->|No| ActualMigration
    DryRunMigration --> End
    
    ActualMigration --> CheckModeAfter
    CheckModeAfter -->|Yes| SetGlobalModeAfter
    CheckModeAfter -->|No| End
    SetGlobalModeAfter --> End
```

## Schema Migration Process (Per Subject)

```mermaid
flowchart TD
    StartSubject([Process Subject])
    
    %% Check if schema exists
    CheckExists{Schema already exists?}
    SkipSchema[Skip Schema]
    
    %% ID Preservation
    CheckPreserveID{PRESERVE_IDS=true?}
    CheckSubjectEmpty{Subject empty/non-existent?}
    SetSubjectImport[Set Subject to IMPORT mode]
    SkipIDPreservation[Continue without ID preservation]
    
    %% Mode Handling
    CheckReadOnly{Subject in READONLY?}
    SetReadWrite[Set Subject to READWRITE]
    
    %% Registration
    RegisterWithID[Register Schema with Original ID]
    RegisterWithoutID[Register Schema without ID]
    
    %% Restore Mode
    RestoreMode[Restore Original Subject Mode]
    
    %% Retry Logic
    CheckFailed{Migration Failed?}
    CheckRetry{RETRY_FAILED=true?}
    RetryMigration[Retry with Mode Changes]
    
    NextSubject([Next Subject])
    
    %% Flow
    StartSubject --> CheckExists
    CheckExists -->|Yes| SkipSchema
    CheckExists -->|No| CheckPreserveID
    SkipSchema --> NextSubject
    
    CheckPreserveID -->|Yes| CheckSubjectEmpty
    CheckPreserveID -->|No| CheckReadOnly
    
    CheckSubjectEmpty -->|Yes| SetSubjectImport
    CheckSubjectEmpty -->|No| SkipIDPreservation
    SetSubjectImport --> RegisterWithID
    SkipIDPreservation --> CheckReadOnly
    
    CheckReadOnly -->|Yes| SetReadWrite
    CheckReadOnly -->|No| RegisterWithoutID
    SetReadWrite --> RegisterWithoutID
    
    RegisterWithID --> RestoreMode
    RegisterWithoutID --> RestoreMode
    
    RestoreMode --> CheckFailed
    CheckFailed -->|Yes| CheckRetry
    CheckFailed -->|No| NextSubject
    
    CheckRetry -->|Yes| RetryMigration
    CheckRetry -->|No| NextSubject
    RetryMigration --> NextSubject
```

## Environment Variables Control Flow

```mermaid
graph LR
    subgraph "Global Registry Settings"
        DEST_IMPORT_MODE[DEST_IMPORT_MODE<br/>Sets global IMPORT mode]
        DEST_MODE_AFTER[DEST_MODE_AFTER_MIGRATION<br/>Sets global mode after migration]
    end
    
    subgraph "Migration Control"
        ENABLE_MIGRATION[ENABLE_MIGRATION<br/>Enable/disable migration]
        DRY_RUN[DRY_RUN<br/>Simulate without changes]
        CLEANUP_DEST[CLEANUP_DESTINATION<br/>Clean before migration]
    end
    
    subgraph "Schema Registration"
        PRESERVE_IDS[PRESERVE_IDS<br/>Use subject-level IMPORT]
        RETRY_FAILED[RETRY_FAILED<br/>Retry failed migrations]
    end
    
    subgraph "Authentication"
        SOURCE_AUTH[SOURCE_USERNAME<br/>SOURCE_PASSWORD]
        DEST_AUTH[DEST_USERNAME<br/>DEST_PASSWORD]
    end
    
    subgraph "Context Settings"
        SOURCE_CTX[SOURCE_CONTEXT]
        DEST_CTX[DEST_CONTEXT]
    end
```

## Mode Hierarchy

```mermaid
graph TD
    subgraph "Global Mode (Registry-wide)"
        GM[Global Mode]
        GM --> READWRITE_G[READWRITE]
        GM --> READONLY_G[READONLY]
        GM --> IMPORT_G[IMPORT]
        GM --> READWRITE_OVERRIDE_G[READWRITE_OVERRIDE]
    end
    
    subgraph "Subject Mode (Per-subject)"
        SM[Subject Mode]
        SM --> READWRITE_S[READWRITE]
        SM --> READONLY_S[READONLY]
        SM --> IMPORT_S[IMPORT - for ID preservation]
        SM --> READWRITE_OVERRIDE_S[READWRITE_OVERRIDE]
    end
    
    Note1[Global mode affects all subjects]
    Note2[Subject mode overrides global for that subject]
    Note3[IMPORT mode required for ID preservation]
    
    GM -.-> Note1
    SM -.-> Note2
    IMPORT_S -.-> Note3
```

## ID Preservation Flow

```mermaid
sequenceDiagram
    participant M as Migrator
    participant S as Subject
    participant R as Registry
    
    Note over M,R: PRESERVE_IDS=true
    
    M->>S: Check if subject exists
    alt Subject is empty/non-existent
        M->>S: Set mode to IMPORT
        S->>R: PUT /mode/{subject} {"mode": "IMPORT"}
        R-->>S: 200 OK
        
        M->>R: Register schema with ID
        Note right of M: POST /subjects/{subject}/versions<br/>{"schema": "...", "id": 24}
        R-->>M: 200 OK {"id": 24}
        
        M->>S: Restore original mode
        S->>R: PUT /mode/{subject} {"mode": "READWRITE"}
        R-->>S: 200 OK
    else Subject has existing schemas
        M->>M: Skip ID preservation
        M->>R: Register schema without ID
        Note right of M: POST /subjects/{subject}/versions<br/>{"schema": "..."}
        R-->>M: 200 OK {"id": auto-generated}
    end
```

## Schema-Level Migration Flow (Within a Subject)

```mermaid
flowchart TD
    StartMigration([Start Migration for Subject])
    
    %% Get all schemas
    GetSourceSchemas[Get all schemas from source]
    GetDestSchemas[Get all schemas from destination]
    
    %% Sort schemas
    SortByVersion[Sort source schemas by version number<br/>ascending: v1, v2, v3, ...]
    
    %% Process each version
    ForEachVersion[For each version in order]
    
    %% Check if exists
    CheckSchemaExists{Exact schema<br/>already exists<br/>in destination?}
    SkipVersion[Skip this version]
    
    %% ID Preservation Check
    CheckPreserveID{PRESERVE_IDS=true<br/>AND subject empty?}
    PrepareWithID[Prepare schema with original ID]
    PrepareWithoutID[Prepare schema without ID]
    
    %% Register Schema
    RegisterSchema[Register Schema Version]
    LogSuccess[Log: Successfully migrated<br/>subject version X]
    LogSkipped[Log: Skipped - already exists]
    
    %% Next Version
    MoreVersions{More versions?}
    NextSubject([Continue to next subject])
    
    %% Flow
    StartMigration --> GetSourceSchemas
    GetSourceSchemas --> GetDestSchemas
    GetDestSchemas --> SortByVersion
    SortByVersion --> ForEachVersion
    
    ForEachVersion --> CheckSchemaExists
    CheckSchemaExists -->|Yes| LogSkipped
    CheckSchemaExists -->|No| CheckPreserveID
    
    LogSkipped --> MoreVersions
    
    CheckPreserveID -->|Yes| PrepareWithID
    CheckPreserveID -->|No| PrepareWithoutID
    
    PrepareWithID --> RegisterSchema
    PrepareWithoutID --> RegisterSchema
    
    RegisterSchema --> LogSuccess
    LogSuccess --> MoreVersions
    
    MoreVersions -->|Yes| ForEachVersion
    MoreVersions -->|No| NextSubject
```

## Subject and Schema Processing Order

```mermaid
graph TD
    subgraph "Subject Processing Order"
        S1[Subjects processed in order<br/>returned by source registry API]
        S2[No specific sorting of subjects]
        S3[Each subject processed independently]
    end
    
    subgraph "Schema Version Processing Order"
        V1[Versions sorted by version number]
        V2[Processing order: 1, 2, 3, ...]
        V3[Ensures compatibility chain maintained]
    end
    
    subgraph "Example Processing"
        E1[Subject: user-events<br/>Versions: 3, 1, 2]
        E2[Sorted: 1, 2, 3]
        E3[Migrate v1 → v2 → v3]
        
        E4[Subject: order-events<br/>Versions: 2, 1]
        E5[Sorted: 1, 2]
        E6[Migrate v1 → v2]
    end
    
    S1 --> S2
    S2 --> S3
    
    V1 --> V2
    V2 --> V3
    
    E1 --> E2
    E2 --> E3
    E4 --> E5
    E5 --> E6
```

## Detailed Schema Migration Example

```mermaid
sequenceDiagram
    participant M as Migrator
    participant SR as Source Registry
    participant DR as Dest Registry
    
    Note over M,DR: Processing subject "user-events"
    
    M->>SR: GET /subjects/user-events/versions
    SR-->>M: [2, 1, 3]
    
    M->>M: Sort versions: [1, 2, 3]
    
    loop For each version in order
        M->>SR: GET /subjects/user-events/versions/1
        SR-->>M: {id: 42, schema: "...", version: 1}
        
        M->>DR: POST /subjects/user-events
        Note right of M: Check if exact schema exists
        DR-->>M: 404 Not Found
        
        alt PRESERVE_IDS=true
            M->>DR: PUT /mode/user-events
            Note right of M: {"mode": "IMPORT"}
            DR-->>M: 200 OK
            
            M->>DR: POST /subjects/user-events/versions
            Note right of M: {schema: "...", id: 42}
            DR-->>M: {id: 42}
            
            M->>DR: PUT /mode/user-events
            Note right of M: {"mode": "READWRITE"}
            DR-->>M: 200 OK
        else PRESERVE_IDS=false
            M->>DR: POST /subjects/user-events/versions
            Note right of M: {schema: "..."}
            DR-->>M: {id: 101}
        end
    end
```

## Schema Data Structure and Ordering

```mermaid
classDiagram
    class SourceSchemas {
        +Dict[str, List[Dict]] schemas
        +get_all_schemas()
    }
    
    class SubjectSchemas {
        +str subject_name
        +List[VersionInfo] versions
        +sort_by_version()
    }
    
    class VersionInfo {
        +int version
        +int id
        +str schema
        +str schemaType
    }
    
    SourceSchemas "1" --> "*" SubjectSchemas
    SubjectSchemas "1" --> "*" VersionInfo
    
    note for SubjectSchemas "Versions are sorted by version number\nbefore processing to maintain\ncompatibility chain"
    
    note for VersionInfo "Each version has:\n- version: sequential number\n- id: global schema ID\n- schema: actual schema content\n- schemaType: AVRO/JSON/PROTOBUF"
```

## Key Points About Schema Ordering

```mermaid
graph LR
    subgraph "Why Version Order Matters"
        A[Schema Evolution]
        A --> B[v1: Base schema]
        B --> C[v2: Add optional field]
        C --> D[v3: Add another field]
        
        E[Must register in order<br/>to maintain compatibility]
    end
    
    subgraph "What Happens Without Order"
        F[Try to register v3 first]
        F --> G[Compatibility check fails]
        G --> H[No previous version exists]
    end
    
    subgraph "ID vs Version"
        I[Schema IDs are global]
        J[Version numbers are per-subject]
        K[Sort by version, preserve IDs]
    end
```

## Troubleshooting Version Mismatches

### Common Scenario: Schema Divergence

```mermaid
graph TD
    subgraph "Source Registry"
        S1[v1: Initial schema]
        S2[v2: Add field A]
        S3[v3: Add field B]
        S4[v4-v8: Minor updates]
        S9[v9: Add field C]
        S10[v10-v14: More updates]
    end
    
    subgraph "Destination Registry"
        D1[v1: Initial schema]
        D2[v2: Add field A]
        D3[v3: Add field B]
        D4[v4-v8: Same as source]
        D9[v9: Add field X - DIFFERENT!]
        D10[v10-v20: Evolved differently]
    end
    
    subgraph "Migration Result"
        M1[✓ v1-v3: Already exist, compatible]
        M2[✓ v4-v8: Skipped - identical schemas]
        M3[✗ v9-v14: 409 Conflict - incompatible]
    end
    
    S4 -.->|Same schema| D4
    S9 -.->|Different schema| D9
    
    style M3 fill:#f96
```

### Diagnosis Flow

```mermaid
flowchart TD
    Start([Version Mismatch Detected])
    
    CheckVersions[Compare version counts]
    SourceMore{Source has more versions?}
    DestMore{Dest has more versions?}
    
    CheckSchemas[Compare schema contents<br/>for each version]
    
    IdentifyDivergence[Identify divergence point]
    
    subgraph "Resolution Options"
        O1[Option 1: Force overwrite<br/>CLEANUP_DESTINATION=true]
        O2[Option 2: Merge manually<br/>Export and reconcile]
        O3[Option 3: Use different subject<br/>Create new subject name]
        O4[Option 4: Skip problematic subject<br/>Migrate others only]
    end
    
    Start --> CheckVersions
    CheckVersions --> SourceMore
    CheckVersions --> DestMore
    
    SourceMore -->|Yes| CheckSchemas
    DestMore -->|Yes| CheckSchemas
    
    CheckSchemas --> IdentifyDivergence
    
    IdentifyDivergence --> O1
    IdentifyDivergence --> O2
    IdentifyDivergence --> O3
    IdentifyDivergence --> O4
```

### Debugging Commands

To diagnose version mismatches, you can:

1. **List all versions in both registries**:
```bash
# Source
curl -u $SOURCE_USERNAME:$SOURCE_PASSWORD \
  $SOURCE_SCHEMA_REGISTRY_URL/subjects/YOUR_SUBJECT/versions

# Destination  
curl -u $DEST_USERNAME:$DEST_PASSWORD \
  $DEST_SCHEMA_REGISTRY_URL/subjects/YOUR_SUBJECT/versions
```

2. **Compare specific version schemas**:
```bash
# Get schema from source version 9
curl -u $SOURCE_USERNAME:$SOURCE_PASSWORD \
  $SOURCE_SCHEMA_REGISTRY_URL/subjects/YOUR_SUBJECT/versions/9 | jq .

# Get schema from destination version 9
curl -u $DEST_USERNAME:$DEST_PASSWORD \
  $DEST_SCHEMA_REGISTRY_URL/subjects/YOUR_SUBJECT/versions/9 | jq .
```

3. **Check compatibility**:
```bash
# Test if source schema is compatible with destination
curl -X POST -u $DEST_USERNAME:$DEST_PASSWORD \
  -H "Content-Type: application/json" \
  -d '{"schema": "YOUR_SOURCE_SCHEMA_HERE"}' \
  $DEST_SCHEMA_REGISTRY_URL/compatibility/subjects/YOUR_SUBJECT/versions/latest
```

### Resolution Strategies

```mermaid
graph LR
    subgraph "Strategy 1: Clean Slate"
        C1[Set CLEANUP_DESTINATION=true]
        C2[Delete all destination schemas]
        C3[Migrate fresh from source]
    end
    
    subgraph "Strategy 2: Selective Migration"
        S1[Identify compatible versions]
        S2[Skip incompatible subjects]
        S3[Document divergence]
    end
    
    subgraph "Strategy 3: Version Mapping"
        V1[Export both registries]
        V2[Create version mapping]
        V3[Custom migration script]
    end
    
    subgraph "Strategy 4: New Subject"
        N1[Create new subject name]
        N2[Migrate to new subject]
        N3[Update consumers/producers]
    end
```

### Example: Handling the Reported Issue

For the `payment-transactionPaymentTransactionEvents` case:

```mermaid
sequenceDiagram
    participant M as Migrator
    participant S as Source
    participant D as Destination
    
    Note over M,D: Initial Migration
    M->>S: Get versions 1-14
    M->>D: Check versions 1-20 exist
    M->>M: Skip v4-v8 (identical schemas)
    
    Note over M,D: Retry Phase
    M->>D: Try to register v9
    D-->>M: 409 Conflict (incompatible with existing v9)
    M->>D: Get latest version
    D-->>M: Latest is v20
    
    Note over M,D: Resolution Options
    alt Option 1: Force Clean
        M->>D: DELETE /subjects/../versions
        M->>D: Migrate all versions fresh
    else Option 2: Skip Subject
        M->>M: Log subject as incompatible
        M->>M: Continue with other subjects
    else Option 3: Analyze Differences
        M->>S: Export all versions
        M->>D: Export all versions
        M->>M: Compare and document differences
    end
```

### Practical Example: Resolving Version Mismatches

For the reported issue with `payment-transactionPaymentTransactionEvents`:

#### Option 1: Clean Only the Problematic Subject
```bash
# Set environment variables
export CLEANUP_SUBJECTS="payment-transactionPaymentTransactionEvents"
export PERMANENT_DELETE=true
export ENABLE_MIGRATION=true
export DRY_RUN=false

# Run migration - this will:
# 1. Delete only the specified subject
# 2. Re-migrate it from source
python schema_registry_migrator.py
```

#### Option 2: Investigate the Differences First
```bash
# Run comparison only to see detailed differences
export ENABLE_MIGRATION=false
python schema_registry_migrator.py

# The enhanced error reporting will show:
# - Which fields differ between versions
# - Namespace differences
# - Type mismatches
```

#### Option 3: Clean Multiple Problem Subjects
```bash
# If you have multiple subjects with issues
export CLEANUP_SUBJECTS="subject1,subject2,subject3"
export PERMANENT_DELETE=true
export ENABLE_MIGRATION=true
export DRY_RUN=false

python schema_registry_migrator.py
```
