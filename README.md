# ingest-tracker

This repo contains an internal Atlas dataset tracker. The tracker writes out to google sheets. There are additional functions that are useful for curators e.g. duplication checkers, log of previous status and retrieval of metadata files.

**So far a first iteration of state tracking has been implemented for initial testing and feedback.**


## Navigating the code

N.B. You cannot run this code without private config files which point the code at nfs metadata paths and internal DBs.
 
An example of using the main class can be found in `run_status_crawler.py`

e.g.
```
trackerBuild.tracker_build(args.sources_config, args.db_config, args.google_client_secret, args.google_output, args.sheetname)
```
This will update the overview which is written to `https://docs.google.com/spreadsheets/d/1-RaroLU9eTbhM3dcgCLxOV8WyAlXEnV5hodRDEfeOSo/edit#gid=201942787`

_Old spreadsheet before codon migration is at `https://docs.google.com/spreadsheets/d/1rIf3t2wcfYdE8rgxDhYOwr-NIGTIzWpAfrlFeuiD9qE/edit#gid=2054734368`_

The main class is in `trackerBuild.py` and works approximately like so:

1. crawls nfs for experiment paths and accessions
1. opens idf/sdrf file to extract specific experiment metadata
1. extract metadata from DBs
1. compile a summary dataframe
1. output to google sheet API

Each of these task are carried out sequentially and call separate scripts.


#### Deployment
A git push triggers Jenkins run. Runs are also schedules 3 per day to update the sheet. A dev sheet is used for local development at `https://docs.google.com/spreadsheets/d/13gxKodyl-zJTeyCxXtxdw_rp60WJHMcHLtZhxhg5opo/edit#gid=1140221211`

Before starting you require various configuration files in `app/etc`. These allow db connections, navigation of paths on nfs and permissions for google sheet writing. 

#### State definitions 

| State  | Description  |
|---|---|
|**external**| Datasets are in external resources and we don't know about them in Atlas but we may have metadata on them via various data discovery mechanisms.  |
|**incoming**| A dataset **should** be imported (not what could be imported). |
|**loading**|A dataset is actively undergoing curation, review and ingest|
|**analysing**|A dataset has completed curation and passed validation. It is now undergoing analysis.|
|**processed**|Analysis is complete.|
|**published**|A dataset is available to the frontend web application.|

#### Config

Supports paths on nfs or public lookup via e.g. 'https://www.ebi.ac.uk/gxa/json/experiments'. Use URL as entry name instead of path.