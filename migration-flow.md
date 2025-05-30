# Schema Registry Migration Flow

## Overall Migration Process

```mermaid
flowchart TD
    Start([Start Migration])
    
    %% Initial Comparison
    GetSchemas[Get schemas from both registries]
    CompareSchemas[Compare schemas and detect collisions]
    
    %% Migration Check
    CheckMigration{ENABLE_MIGRATION=true?}
    CompareOnly[Display comparison results only]
    
    %% Cleanup Phase
    CheckCleanup{CLEANUP_DESTINATION=true?}
    CleanupDest[Clean entire destination registry]
    CheckCleanupSubjects{CLEANUP_SUBJECTS set?}
    CleanupSpecific[Clean specific subjects only]
    
    %% Dry Run Check
    CheckDryRun{DRY_RUN=true?}
    
    %% Import Mode Setup (after cleanup, only if not dry run)
    CheckImportMode{DEST_IMPORT_MODE=true?}
    SetGlobalImport[Set Global Mode to IMPORT]
    
    %% Migration Execution
    DryRunMigration[Simulate Migration]
    ActualMigration[Perform Migration with ID preservation]
    
    %% Compatibility Handling
    CheckCompatibility{AUTO_HANDLE_COMPATIBILITY=true?}
    RetryWithCompatibility[Retry failed subjects with<br/>compatibility set to NONE]
    
    %% Post Migration
    CheckModeAfter{DEST_MODE_AFTER_MIGRATION set?}
    SetGlobalModeAfter[Set Global Mode to specified value]
    
    End([End])
    
    %% Flow
    Start --> GetSchemas
    GetSchemas --> CompareSchemas
    CompareSchemas --> CheckMigration
    
    CheckMigration -->|No| CompareOnly
    CheckMigration -->|Yes| CheckCleanup
    CompareOnly --> End
    
    CheckCleanup -->|Yes| CleanupDest
    CheckCleanup -->|No| CheckCleanupSubjects
    CleanupDest --> CheckDryRun
    
    CheckCleanupSubjects -->|Yes| CleanupSpecific
    CheckCleanupSubjects -->|No| CheckDryRun
    CleanupSpecific --> CheckDryRun
    
    CheckDryRun -->|Yes| DryRunMigration
    CheckDryRun -->|No| CheckImportMode
    
    CheckImportMode -->|Yes| SetGlobalImport
    CheckImportMode -->|No| ActualMigration
    SetGlobalImport --> ActualMigration
    
    DryRunMigration --> End
    
    ActualMigration --> CheckCompatibility
    CheckCompatibility -->|Yes| RetryWithCompatibility
    CheckCompatibility -->|No| CheckModeAfter
    RetryWithCompatibility --> CheckModeAfter
    
    CheckModeAfter -->|Yes| SetGlobalModeAfter
    CheckModeAfter -->|No| End
    SetGlobalModeAfter --> End
    
    style CheckImportMode stroke:#ffd700,stroke-width:3px
    style SetGlobalImport stroke:#ffd700,stroke-width:3px
    style RetryWithCompatibility stroke:#90EE90,stroke-width:3px
```

## Schema Migration Process (Per Subject)

```mermaid
flowchart TD
    StartSubject([Process Subject])
    
    %% Check if subject is empty for ID preservation
    CheckPreserveID{PRESERVE_IDS=true?}
    CheckSubjectEmpty{Subject empty/non-existent?}
    SetSubjectImport[Set Subject to IMPORT mode<br/>for ALL versions]
    SkipIDPreservation[Continue without ID preservation]
    
    %% Process versions
    ProcessVersions[Process all versions in order]
    
    %% For each version
    ForEachVersion[For each version]
    CheckExists{Schema already exists?}
    SkipSchema[Skip Schema]
    
    %% Mode Handling (only if not in IMPORT)
    CheckImportMode{In IMPORT mode?}
    CheckReadOnly{Subject in READONLY?}
    SetReadWrite[Set Subject to READWRITE]
    
    %% Registration
    RegisterWithID[Register Schema with Original ID]
    RegisterWithoutID[Register Schema without ID]
    
    %% Error Handling
    CheckConflict{409 Conflict Error?}
    CheckAutoCompat{AUTO_HANDLE_COMPATIBILITY=true?}
    MarkForRetry[Mark subject for compatibility retry]
    LogFailure[Log failure and continue]
    
    %% Restore Mode
    RestoreMode[Restore Original Subject Mode]
    
    NextSubject([Next Subject])
    
    %% Flow
    StartSubject --> CheckPreserveID
    CheckPreserveID -->|Yes| CheckSubjectEmpty
    CheckPreserveID -->|No| ProcessVersions
    
    CheckSubjectEmpty -->|Yes| SetSubjectImport
    CheckSubjectEmpty -->|No| SkipIDPreservation
    SetSubjectImport --> ProcessVersions
    SkipIDPreservation --> ProcessVersions
    
    ProcessVersions --> ForEachVersion
    ForEachVersion --> CheckExists
    
    CheckExists -->|Yes| SkipSchema
    CheckExists -->|No| CheckImportMode
    
    CheckImportMode -->|Yes| RegisterWithID
    CheckImportMode -->|No| CheckReadOnly
    
    CheckReadOnly -->|Yes| SetReadWrite
    CheckReadOnly -->|No| RegisterWithoutID
    SetReadWrite --> RegisterWithoutID
    
    RegisterWithID --> CheckConflict
    RegisterWithoutID --> CheckConflict
    
    CheckConflict -->|No| ForEachVersion
    CheckConflict -->|Yes| CheckAutoCompat
    
    CheckAutoCompat -->|Yes| MarkForRetry
    CheckAutoCompat -->|No| LogFailure
    
    MarkForRetry --> ForEachVersion
    LogFailure --> ForEachVersion
    
    ForEachVersion -->|More versions| ForEachVersion
    ForEachVersion -->|No more versions| RestoreMode
    
    RestoreMode --> NextSubject
    SkipSchema --> ForEachVersion
    
    style SetSubjectImport stroke:#ffd700,stroke-width:3px
    style CheckSubjectEmpty stroke:#87CEEB,stroke-width:3px
    style RegisterWithID stroke:#90EE90,stroke-width:3px
```

## Environment Variables Control Flow

```mermaid
graph LR
    subgraph "Global Registry Settings"
        DEST_IMPORT_MODE[DEST_IMPORT_MODE<br/>Sets global IMPORT mode<br/>AFTER cleanup]
        DEST_MODE_AFTER[DEST_MODE_AFTER_MIGRATION<br/>Sets global mode after migration]
    end
    
    subgraph "Migration Control"
        ENABLE_MIGRATION[ENABLE_MIGRATION<br/>Enable/disable migration]
        DRY_RUN[DRY_RUN<br/>Simulate without changes]
        CLEANUP_DEST[CLEANUP_DESTINATION<br/>Clean entire registry]
        CLEANUP_SUBJ[CLEANUP_SUBJECTS<br/>Clean specific subjects]
    end
    
    subgraph "Schema Registration"
        PRESERVE_IDS[PRESERVE_IDS<br/>Use subject-level IMPORT]
        RETRY_FAILED[RETRY_FAILED<br/>Retry failed migrations]
        AUTO_COMPAT[AUTO_HANDLE_COMPATIBILITY<br/>Auto-resolve compatibility issues]
    end
    
    subgraph "Authentication"
        SOURCE_AUTH[SOURCE_USERNAME<br/>SOURCE_PASSWORD]
        DEST_AUTH[DEST_USERNAME<br/>DEST_PASSWORD]
    end
    
    subgraph "Context Settings"
        SOURCE_CTX[SOURCE_CONTEXT]
        DEST_CTX[DEST_CONTEXT]
    end
    
    style AUTO_COMPAT stroke:#90EE90,stroke-width:3px
    style DEST_IMPORT_MODE stroke:#ffd700,stroke-width:3px
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
        M->>S: Set mode to IMPORT (once for all versions)
        S->>R: PUT /mode/{subject} {"mode": "IMPORT"}
        R-->>S: 200 OK
        
        Note over M,R: Process ALL versions while in IMPORT mode
        
        loop For each version in order
            M->>R: Register schema with ID
            Note right of M: POST /subjects/{subject}/versions<br/>{"schema": "...", "id": originalId}
            R-->>M: 200 OK {"id": originalId}
        end
        
        M->>S: Restore original mode
        S->>R: PUT /mode/{subject} {"mode": "READWRITE"}
        R-->>S: 200 OK
    else Subject has existing schemas
        Note over M,R: Cannot set IMPORT mode on non-empty subject
        M->>M: Skip ID preservation
        loop For each version
            M->>R: Register schema without ID
            Note right of M: POST /subjects/{subject}/versions<br/>{"schema": "..."}
            R-->>M: 200 OK {"id": auto-generated}
        end
    end
```

### Key Points:
- **IMPORT mode can only be set on empty subjects**
- **Once set, IMPORT mode must be maintained for ALL versions**
- **Cannot switch to IMPORT mode after any schema is registered**
- **All versions must be registered in a single IMPORT session**

## IMPORT Mode Limitations and Behavior

```mermaid
graph TD
    subgraph "Empty Subject"
        E1[Subject has no schemas]
        E2[Can set IMPORT mode ✓]
        E3[Register v1 with ID 886]
        E4[Register v2 with ID 949]
        E5[Register v3 with ID 1022]
        E6[All IDs preserved ✓]
        
        E1 --> E2
        E2 --> E3
        E3 --> E4
        E4 --> E5
        E5 --> E6
    end
    
    subgraph "Non-Empty Subject"
        N1[Subject has schemas]
        N2[Cannot set IMPORT mode ✗]
        N3[Register v1 → ID 100501]
        N4[Register v2 → ID 100502]
        N5[Register v3 → ID 100503]
        N6[Auto-generated IDs ✗]
        
        N1 --> N2
        N2 --> N3
        N3 --> N4
        N4 --> N5
        N5 --> N6
    end
    
    subgraph "Partial IMPORT (Wrong Approach)"
        P1[Set IMPORT mode]
        P2[Register v1 with ID 886 ✓]
        P3[Try to set IMPORT for v2]
        P4[422 Error - Subject not empty]
        P5[Register v2 → ID 100502 ✗]
        P6[Mixed IDs - Inconsistent]
        
        P1 --> P2
        P2 --> P3
        P3 --> P4
        P4 --> P5
        P5 --> P6
    end
    
    style E6 stroke:#90EE90,stroke-width:3px
    style N6 stroke:#ff9999,stroke-width:3px
    style P6 stroke:#ffcc00,stroke-width:3px
```

### Why This Happens:

1. **IMPORT mode is a subject-level setting** that affects how the registry handles schema registration
2. **Once a subject has any schema**, it's no longer considered "empty"
3. **The 422 error** occurs because the registry enforces this rule strictly
4. **Solution**: Set IMPORT mode once before registering ANY schemas, then register ALL versions

### Best Practices:

- Always check if a subject is empty before attempting ID preservation
- If migrating to a non-empty registry, consider using `CLEANUP_SUBJECTS` for specific subjects
- Plan your migration strategy based on whether subjects exist in the destination

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
    
    style M3 stroke:#ff9999,stroke-width:3px
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

## Compatibility Handling Flow

```mermaid
sequenceDiagram
    participant M as Migrator
    participant S as Subject
    participant R as Registry
    
    Note over M,R: AUTO_HANDLE_COMPATIBILITY=true
    
    %% First Pass - Normal Migration
    rect rgb(255, 230, 230)
        Note over M,R: First Pass: Normal Migration
        M->>R: Register schema version
        R-->>M: 409 Conflict (compatibility error)
        M->>M: Mark subject for retry
        M->>M: Continue with next version/subject
    end
    
    %% After all subjects processed
    rect rgb(230, 255, 230)
        Note over M,R: Retry Phase: Mode & Compatibility Handling
        M->>R: GET /mode/{subject}
        R-->>M: Current mode (might be READONLY)
        M->>M: Store original mode
        
        alt Subject not in READWRITE mode
            M->>R: PUT /mode/{subject}
            Note right of M: {"mode": "READWRITE"}
            R-->>M: 200 OK
        end
        
        M->>R: GET /config/{subject}
        R-->>M: Current compatibility or null
        M->>M: Store original compatibility
        
        M->>R: PUT /config/{subject}
        Note right of M: {"compatibility": "NONE"}
        R-->>M: 200 OK
        
        loop For each failed version
            M->>R: Register schema version
            R-->>M: 200 OK (success)
        end
        
        %% Restore original settings
        alt Subject had specific mode
            M->>R: PUT /mode/{subject}
            Note right of M: {"mode": "original_mode"}
            R-->>M: 200 OK
        end
        
        alt Subject had specific compatibility
            M->>R: PUT /config/{subject}
            Note right of M: {"compatibility": "original_value"}
            R-->>M: 200 OK
        else Subject used global compatibility
            M->>R: DELETE /config/{subject}
            Note right of M: Remove subject-level config
            R-->>M: 200 OK
        end
    end
```

## Enhanced Retry Mechanism

```mermaid
flowchart TD
    subgraph "Initial Migration Attempt"
        Try1[Try to register schema]
        Fail1{Failed?}
        Error1[Check error type]
        
        Try1 --> Fail1
        Fail1 -->|Yes| Error1
        Fail1 -->|No| Success1[Success]
        
        Error1 -->|409 Conflict| MarkRetry[Mark for compatibility retry]
        Error1 -->|422 Not in write mode| MarkRetry
        Error1 -->|Other| LogError[Log error]
    end
    
    subgraph "Retry Phase (Per Subject)"
        StartRetry[Start retry for subject]
        
        %% ID Preservation check
        CheckEmpty[Check if subject is empty]
        CanPreserveIDs{Empty & PRESERVE_IDS=true?}
        SetImportMode[Set IMPORT mode for all versions]
        
        %% Mode handling
        CheckMode[Get current mode]
        IsReadWrite{Mode = READWRITE?}
        SetReadWrite[Set mode to READWRITE]
        StoreMode[Store original mode]
        
        %% Compatibility handling
        CheckCompat[Get current compatibility]
        SetNone[Set compatibility to NONE]
        StoreCompat[Store original compatibility]
        
        %% Retry migrations
        RetryMigrations[Retry all failed versions<br/>with or without IDs]
        
        %% Restore settings
        RestoreMode[Restore original mode]
        RestoreCompat[Restore original compatibility]
        
        %% Flow
        StartRetry --> CheckEmpty
        CheckEmpty --> CanPreserveIDs
        CanPreserveIDs -->|Yes| SetImportMode
        CanPreserveIDs -->|No| CheckMode
        SetImportMode --> CheckCompat
        
        CheckMode --> StoreMode
        StoreMode --> IsReadWrite
        IsReadWrite -->|No| SetReadWrite
        IsReadWrite -->|Yes| CheckCompat
        SetReadWrite --> CheckCompat
        
        CheckCompat --> StoreCompat
        StoreCompat --> SetNone
        SetNone --> RetryMigrations
        
        RetryMigrations --> RestoreMode
        RestoreMode --> RestoreCompat
        RestoreCompat --> EndRetry[End retry for subject]
    end
    
    MarkRetry --> StartRetry
    
    style SetImportMode stroke:#ffd700,stroke-width:3px
    style SetReadWrite stroke:#87CEEB,stroke-width:3px
    style SetNone stroke:#90EE90,stroke-width:3px
    style RetryMigrations stroke:#87CEEB,stroke-width:3px
```

### Key Features of Enhanced Retry:

1. **Handles Multiple Error Types**:
   - 409 Conflicts (compatibility issues)
   - 422 Errors (subject not in write mode)
   - Other registration failures

2. **Subject-Level Processing**:
   - Groups failures by subject for efficient retry
   - Applies mode and compatibility changes per subject
   - Restores original settings after retry

3. **Graceful Degradation**:
   - Continues even if mode changes fail
   - Logs warnings but doesn't stop the process
   - Handles ID preservation fallback

4. **State Management**:
   - Stores original mode and compatibility
   - Properly restores settings after retry
   - Handles both subject-level and global settings

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

## Complete Execution Order

```mermaid
flowchart TD
    subgraph "1. Initialization"
        Init1[Load environment variables]
        Init2[Create source & dest clients]
        Init3[Get schemas from both registries]
    end
    
    subgraph "2. Comparison Phase"
        Comp1[Compare all schemas]
        Comp2[Detect ID collisions]
        Comp3[Display comparison results]
    end
    
    subgraph "3. Pre-Migration Checks"
        Check1{ENABLE_MIGRATION=true?}
        Check2{ID collisions exist?}
        Check3{CLEANUP_DESTINATION=true?}
    end
    
    subgraph "4. Cleanup Phase"
        Clean1[Clean entire destination<br/>if CLEANUP_DESTINATION=true]
        Clean2[Clean specific subjects<br/>if CLEANUP_SUBJECTS set]
        Clean3[Refresh destination schemas]
    end
    
    subgraph "5. Mode Setup"
        Mode1{DRY_RUN=false?}
        Mode2{DEST_IMPORT_MODE=true?}
        Mode3[Set global IMPORT mode]
    end
    
    subgraph "6. Migration Execution"
        Mig1[Sort subjects]
        Mig2[For each subject:<br/>- Sort versions by number<br/>- Check if exists<br/>- Set subject IMPORT if needed<br/>- Register schema<br/>- Handle 409 conflicts]
    end
    
    subgraph "7. Compatibility Retry"
        Compat1{AUTO_HANDLE_COMPATIBILITY=true<br/>AND failures exist?}
        Compat2[For each failed subject:<br/>- Set mode to READWRITE<br/>- Set compatibility to NONE<br/>- Retry all failed versions<br/>- Restore original mode<br/>- Restore original compatibility]
    end
    
    subgraph "8. Post-Migration"
        Post1[Validate migration results]
        Post2{DEST_MODE_AFTER_MIGRATION set?}
        Post3[Set global mode to specified value]
    end
    
    %% Flow connections
    Init1 --> Init2 --> Init3
    Init3 --> Comp1 --> Comp2 --> Comp3
    Comp3 --> Check1
    
    Check1 -->|No| End1([End - Comparison Only])
    Check1 -->|Yes| Check2
    
    Check2 -->|Yes| Check3
    Check2 -->|No| Clean2
    Check3 -->|No| End2([End - Cannot proceed])
    Check3 -->|Yes| Clean1
    
    Clean1 --> Clean3
    Clean2 --> Clean3
    Clean3 --> Mode1
    
    Mode1 -->|No| Mig1
    Mode1 -->|Yes| Mode2
    Mode2 -->|No| Mig1
    Mode2 -->|Yes| Mode3
    Mode3 --> Mig1
    
    Mig1 --> Mig2
    Mig2 --> Compat1
    
    Compat1 -->|No| Post1
    Compat1 -->|Yes| Compat2
    Compat2 --> Post1
    
    Post1 --> Post2
    Post2 -->|No| End3([End - Success])
    Post2 -->|Yes| Post3
    Post3 --> End3
    
    style Mode3 stroke:#ffd700,stroke-width:3px
    style Compat2 stroke:#90EE90,stroke-width:3px
    style Clean1 stroke:#ffcccc,stroke-width:3px
```
