# ETL-MeshJoin

## Overview

Applications which require a high extent of consistency between operational data and analytics require an active warehouse that can reflect changes in the operational data at shorter time intervals. This project aims to implement an ETL process that is appropriate for an Active Warehouse. It targets a common scenario of a fast stream of new operational data and a collection of Master Data on disk. The stream of new data must be enriched with the stored Master Data in order to include necessary information for analysis with each stream record.

This project employs the use of the MeshJoin algorithm to enrich streaming Transaction (operational) data with Master Data. This amortizes the cost of accessing Master Data via slow disk I/O and enables Active (real-time) Warehousing.

The scenario for this project is a chain of stores, with different outlets and a variety of products for sale in each store. Both operational data (which in the example scenario used in this project is transactional data) and the master data used to enrich the transactional are provided in ```sql_scripts/Transaction_and_MasterData_Generator.sql```.

More details about this project, including the specifications for the example scenario, have been detailed in ```projectReport.pdf``` present at the root of this repository.

## Run MeshJoin

1. Open MySQL terminal (or workbench / phpMyAdmin editor)
2. Open createDW.sql -> enter the first 2 lines in the terminal to create the database we will use
3. Copy and paste the ```Transaction_and_MasterData_Generator.sql``` file into the terminal to create the operational DB tables we will use as the source for our warehouse
4. Copy the rest of ```createDW.sql``` (skip the first 2 lines executed before) into the MySQL terminal to create the schema we will use for our warehouse
5. Open the MeshJoin folder in an IDE like Intellij IDEA
6. Edit the ```config.properties``` file to enter mysql's username and password. Edit ```db.url``` if you used a different database in Step 2
7. The IDE should identify ```MeshJoin.java``` as the class which has the ```main()``` function, and generate the run configuration on its own
8. Run the project and wait for the program to exit with code 0
9. Query the warehouse using ```queriesDW.sql```

## Support
Contact me via email - [Farjad Ilyas](mailto:ilyasfarjad@gmail.com?subject=[GitHub]%20Source%20Han%20Sans)

