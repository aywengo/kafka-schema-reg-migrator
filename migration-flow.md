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

## Complete Example Flow

```mermaid
flowchart LR
    subgraph "Environment Variables"
        ENV1[ENABLE_MIGRATION=true]
        ENV2[DRY_RUN=false]
        ENV3[DEST_IMPORT_MODE=true]
        ENV4[PRESERVE_IDS=true]
        ENV5[CLEANUP_DESTINATION=true]
        ENV6[DEST_MODE_AFTER_MIGRATION=READWRITE]
    end
    
    subgraph "Execution Flow"
        S1[1. Set global IMPORT mode]
        S2[2. Clean destination]
        S3[3. For each subject:<br/>- Set to IMPORT mode<br/>- Register with ID<br/>- Restore mode]
        S4[4. Set global READWRITE mode]
    end
    
    ENV3 --> S1
    ENV5 --> S2
    ENV4 --> S3
    ENV6 --> S4
``` 