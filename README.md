# Event-Based ETL Demo on Microsoft Fabric

This repo demonstrates an **event-driven ETL pattern** in **Microsoft Fabric** using **Activator** subscriptions on **Lakehouse file creation events** to kick off downstream notebooks.

## What it does
- Uses a Fabric **Data Pipeline** to pull **NYC TLC High Volume For-Hire (FHV) trip data** from a public web source.
- Lands the raw files into the **Lakehouse** `fhvhv` folder.
- An **Activator** listens for **FileCreated** events in that folder.  There is a filter on `api` to only trigger on `FlushWithClose`.
- When a new file arrives, Activator triggers the **Move One Parquet File** notebook to process **one file per event**.