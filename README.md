# ingest-tracker

This repo contains an internal Atlas dataset tracker. The tracker writes out to google sheets. There are additional functions that are useful for curators e.g. duplication checkers, log of previous status and retrieval of metadata files.

**So far a first iteration of state tracking has been implemented for initial testing and feedback.**


## Navigating the code
### Status Tracking

The class `status_crawler.py` can be ran with `workflows/run_status_crawler.py` passing arguments for config files required. These are private and only available locally.
The status tracker crawls the filesystem directed by `sources_config.json` to determine the state of each dataset.
The state and other metadata is collected on each dataset and made available in the class `status_crawler` namespace.
An overview is written to `https://docs.google.com/spreadsheets/d/1rIf3t2wcfYdE8rgxDhYOwr-NIGTIzWpAfrlFeuiD9qE/edit#gid=2054734368` which is updated on every run.


#### Deployment
A git push triggers Jenkins run. Runs are also schedules 3 per day to update the sheet. A dev sheet is used for local development at `https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=1140221211`

Before starting you require various configuration files in `app/etc`. These allow db connections, navigation of paths on nfs and permissions for google sheet writing. 

#### State definitions 

| State  | Description  |
|---|---|
|**external**| Datasets are in external resources and we don't know about them in Atlas but we may have metadata on them via various data discovery mechanisms.  |
|**incoming??**| A dataset **should** be imported (not what could be imported). |
|**loading**|A dataset is actively undergoing curation, review and ingest|
|**analysing**|A dataset has completed curation and passed validation. It is now undergoing analysis.|
|**processed**|Analysis is complete.|
|**published**|A dataset is available to the frontend web application.|


#### Known issues

- Lots of file fail to open die to decode errors. These can mostly be ignored because they occur in files that are not in atlas.
- DB connections were neccesary to get atlas eligibility results as these results are not written to a log anywhere. 

 
