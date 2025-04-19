# EMu views

This directory contains the views derived from EMu data.
Each module contains one view subclass along with any additional functions required to
make the view work.
Additionally, there is a `utils` module containing common functions and variables.

## Common checks

### Departments

In ecatalogue derived views it is common for the first value in the `ColDepartment`
field to be checked against this list of values:

- Botany
- Entomology
- Mineralogy
- Palaeontology
- Zoology

If the value is in this list then the record is valid and can proceed with any other
filter checks.

### Disallowed states

In ecatalogue derived views it is common for the first value in the `SecRecordStatus`
field to be checked against this list of values:

- DELETE
- DELETE-MERGED
- DUPLICATION
- Disposed of
- FROZEN ARK
- INVALID
- POSSIBLE TYPE
- PROBLEM
- Re-registered in error
- Reserved
- Retired
- Retired (see Notes)
- SCAN_cat
- See Notes
- Specimen missing - see notes
- Stub
- Stub Record
- Stub record

If the value matches any of these values it will be rejected.

## View Definitions

### preparation

Represents a preparation record and is published to the sample dataset on the Data
Portal.

#### Filter flowchart
```mermaid
flowchart TD
    START(ecatalogue record)
    PREP{ColRecordType is preparation}
    MCF{ColSubDepartment is molecular collections}
    MGP{ColRecordType is mammal group part}
    MAMMALS{ColSubDepartment is ls mammals}
    DTOL{NhmSecProjectName first value is Darwin Tree of Life}
    WP{AdmPublishWebNoPasswordFlag first value is y}
    GUID{AdmGUIDPreferredValue first value is valid GUID}
    DIS{SecRecordStatus first value is in list of disallowed states}
    DEPT{ColDepartment first value is in list of allowed departments}
    LOAN1{LocPermanentLocationRef first value is 3250522}
    LOAN2{LocCurrentSummaryData contains the word loan}
    NO[Reject]
    YES[Success, is preparation record]

    START --> PREP
    PREP -->|Yes| MCF
    PREP -->|No| MGP
    MCF -->|Yes| WP
    MCF -->|No| NO
    MGP -->|Yes| MAMMALS
    MGP -->|No| NO
    MAMMALS -->|Yes| DTOL
    MAMMALS -->|No| NO
    DTOL -->|Yes| WP
    DTOL -->|No| NO
    WP -->|Yes| GUID
    WP -->|No| NO
    GUID -->|Yes| DIS
    GUID -->|No| NO
    DIS -->|No| DEPT
    DIS -->|Yes| NO
    DEPT -->|Yes| LOAN1
    DEPT -->|No| NO
    LOAN1 -->|No| LOAN2
    LOAN1 -->|Yes| NO
    LOAN2 -->|No| YES
    LOAN2 -->|Yes| NO
```
