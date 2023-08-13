Project: Live Performance Tracking of Delivery Associates at Amazon.

The team at Whizzard wanted visibility and accessibility of the performance data for all DAs across all Amazon Node (Distribution Unit), Merchant Fulfilled Network (MFN), and E-commerce (Delivery Unit) sites.

The objectives of this project are to get the following status of DAs on an hourly basis:
➢	Constant monitoring of DAs performance.
➢	Inactive DAs 
➢	DAs who are behind with delivery.
➢	DAs who are still in the hub and have not started yet.
➢	Last three days' performance data.

1.	Getting Performance Data from Corte Portal

The DAs performance data is extracted from the Cortex Portal - https://logistics.amazon.in/.

We are keeping track of the performance of all DAs at all Amazon sites. 

After logging in go to Dashboard → Operations → Delivery → on the top left corner enter the site code. 

The same login credentials can be used to access DAs performance for all sites. At any given time, performance levels can be viewed for one site, to view data for a different site please select that site from the top left corner (as shown below).
 
We will get the performance matrix of DAs at a site level on this page. In order to get data for all sites we would have to enter each site one by one and then combine the data.

Here on the left side list of DAs is given and below this is the unique ID Amazon has given a DA.
  
The status of each DA is given in the list along with DAs information and if you click on each DA for getting their performance. 


What is the information extracted from the Cortex portal?
We are extracting orders delivered, orders assigned, stops assigned, stops completed, and other details indicating the performance of all DAs.

How is Information/ data shared?
The data is shared with the stakeholders through a Google sheet “amazon-live-cotex-data”. The data in this file is updated on an hourly basis.

When is the information shared?
The data is shared through a Google sheet daily and the sheet is updated after every hour from 8 AM to 10 PM and then at the end of the day around 11:50 PM.

How is data saved? 
Data after extraction is saved in the S3 bucket 

2.	Important Links

File Name	Description
script.py
Script for Scraping DAs Performance at Amazon MFN, Node, and E-commerce Stations.
amazon-live-cotex-data
Performance data is shared through this Google Sheet with the stakeholders.
 
By undertaking this project many man-hours have been saved by automating the reporting task and monitoring all DAs on an hourly basis. 
All the information is now being gathered and communicated automatically.
