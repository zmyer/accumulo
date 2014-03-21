Command to run from command line

Can run this test with pre-existing splits; use the following command to create the table with
100 pre-existing splits 

> `$ ../../../bin/accumulo 'org.apache.accumulo.server.test.TestIngest$CreateTable' \  
0 5000000 100 <user> <pw>`

Could try running verify commands after stopping and restarting accumulo

When write ahead log is implemented can try killing tablet server in middle of ingest

Run 5 parallel ingesters and verify:

> `$ . ingest_test.sh`  
(wait)  
`$ . verify_test.sh`  
(wait)

Overwrite previous ingest:
> `$ . ingest_test_2.sh`  
(wait)  
`$ . verify_test_2.sh`  
(wait)

Delete what was previously ingested:
> `$ . ingest_test_3.sh`  
(wait)
