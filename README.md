# Bash Dataflow with CoGbk


Dataflow reads two files from storage. The first with 1m of rows, the second with 138 rows, joining both(CoGbk) and saving the result in bigquery

## Ejectuar
java -jar storage-to-bigquery-dl.jar --project=[id de proyecto] --zone=us-central1-a --runner=DataflowRunner --numWorkers=1 --workerMachineType=n1-standard-1
