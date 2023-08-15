# Project: Live Performance Tracking of Delivery Associates at Amazon

## Introduction

The team at **Whizzard** aimed to achieve visibility and accessibility of performance data for all Delivery Associates (DAs) across various Amazon units, including Node (Distribution Unit), Merchant Fulfilled Network (MFN), and E-commerce (Delivery Unit) sites.

### Objectives

This project's primary goal is to monitor the following DA statuses on an hourly basis:
- Constant monitoring of DAs performance.
- Identifying inactive DAs.
- Tracking DAs lagging in delivery.
- Monitoring DAs who haven't commenced their tasks and are still at the hub.
- Reviewing the performance data from the past three days.

## Data Extraction: Cortex Portal

Performance data for DAs is primarily extracted from the [Cortex Portal](https://logistics.amazon.in/). This portal helps in tracking the performance of all DAs across Amazon sites. 

### Navigation
After logging in:
- Navigate to `Dashboard → Operations → Delivery`.
- Enter the desired site code in the top left corner.
  
The same login credentials grant access to DA performance metrics across all sites. For viewing a different site's data, choose that site from the top-left corner.

### Data Compilation
The portal provides DA performance matrices at an individual site level. To consolidate data across all sites, one must access each site separately and then aggregate the data.

### Data Details
On the portal:
- The left panel lists DAs.
- Each DA has a unique ID assigned by Amazon.
- Clicking on a specific DA reveals their performance metrics.

**Information Extracted:** 
The data extracted from the Cortex portal includes orders delivered, orders assigned, stops assigned, stops completed, and other metrics indicating the performance of all DAs.

## Data Sharing and Storage

### How is data shared?
Performance data is disseminated to stakeholders through a Google sheet named **amazon-live-cotex-data**. This sheet undergoes hourly updates.

### Update Schedule
The Google sheet receives updates every hour from 8 AM to 10 PM. A final update is made around 11:50 PM to wrap up the day.

### Data Storage
Post-extraction, the data is securely stored in an Amazon S3 bucket.

## Important Links and Files

| File Name | Description |
|-----------|-------------|
| `script.py` | Script for scraping DA performance at Amazon MFN, Node, and E-commerce stations. |
| `amazon-live-cotex-data` | Google Sheet through which performance data is shared with stakeholders. |

## Impact

This project significantly reduced manual efforts by automating reporting tasks and facilitating hourly DA monitoring. All vital information is now automatically compiled and disseminated.
