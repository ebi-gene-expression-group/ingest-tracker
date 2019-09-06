# atlas-ingest



This repo's scope is an ingestion framework to gather and prepare Atlas datasets. This includes tracking the status of datasets through ingestion and tools to curate and ingest datasets.

**So far a first iteration of state tracking has been implemented for initial testing and feedback.**



## Navigating the code
### Status Tracking

The class `status_crawler.py` can be ran with `workflows/run_status_crawler.py` passing arguments for config files required. These are private and only available locally.
The status tracker crawls the filesystem directed by `sources_config.json` to determine the state of each dataset.
The state and other metadata is collected on each dataset and made available in the class `status_crawler` namespace.
An overview is written to `https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=1140221211` which is updated on every run.

#### State definitions 

| State  | Description  |
|---|---|
| **external** | Datasets are in external resources and we don't know about them in Atlas but we may have metadata on them via various data discovery mechanisms.  |
| **incoming??** | A dataset **should** be imported (not what could be imported). |
|**loading**|A dataset is actively undergoing curation, review and ingest|
|**analysing**|A dataset has completed curation and passed validation. It is now undergoing analysis.|
|**processed**|Analysis is complete.|
|**published**|A dataset is available to the frontend web application.|

### Workflows

These are scripts that use the `ingest_tools.py` and the previously discussed `status_crawler` class to push data through the system.
These can be triggered manually or automatically to work on one or many datasets.

#### Phase 1: Ingestion
**UNDER DEVELOPMENT**
###### summary
The script `phase1_ingestion.py`:
1. Performs a duplication check to ensure the dataset has not already been ingested
1. Gets MAGE-TAB from the appropreate source (this may involve MAGE-TAB conversion)
1. General MAGE-TAB validation (not experiment type specific)
1. Writes results to log file mapping external accession to MAGE-TAB files ready for manual inspection by a curator. If MAGE-TAB files are converted they are placed in a temporary location for inspection.
###### params
| Flag  | Description  |
|---|---|
| -a | The external accession of the experiment. Could be from GEO/HCA/ENA/PRIDE etc  |
| -s | External source. One of ('ae', 'geo', 'hca', 'ena' or 'pride') |

 
 ### Phase 2: Accessioning
**UNDER DEVELOPMENT**
 ###### summary
 1. Specific MAGE-TAB validation (experiment type and source specific validation)
 1. Creates an internal accession
 1. Ensures MAGE-TAB is copied to correct path
 1. Generate config generated and copied to correct path
 
###### params
Lots of params needed by the curator including:
- External accession (this is used to get MAGE-TAB from phase 1)
- Experiment type enum [baseline, differential]
- Technology enum [one-color microarray, single-cell RNA-seq, RNA-seq]
- Optional ref group, ignore contrast, lib layout

 ### Phase 3: Approval
**UNDER DEVELOPMENT**
 ##### summary
 Once the config and metadata has been manually reviewed analysis is triggered. Thsi involves moving a file to the correct analysis space.
 ##### params
 - internal accession
 
 

Quick notes to be added:

Lots of file fail to open die to decode errors. These can mostly be ignored because they occur in files that are not in atlas.
DB connections were neccesary to get atlas eligibility results as these results are not written to a log anywhere. 

 
