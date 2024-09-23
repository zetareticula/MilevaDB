
## What is MilevaDB?

- __The transactions can span multiple tables__

 The go database is able to support any number of concurrent readers without locking, and allows a writer to make progress. MilevaDB supports rich transactions, in which multiple objects are inserted, updated or deleted. 

- __Cloud-Native and Serverless__

MilevaDB is designed to work in the cloud -- public, private, or hybrid -- making deployment, provisioning, operations, and maintenance simple. 


- __Favor availability over consistency in the presence of network partitions__

MilevaDB is a distributed database with a focus on high availability and horizontal scalability. It is based on egalitarian multi-paxos algorithm and optimized for writes.

##Write-optimized storage__

MilevaDN is a highly-available, distributed database that uses the Reliable Data Replication (RDR) protocol to provide consistent and reliable data.

MilevaDB is a distributed relational database. It is an ACID compliant transactional database that can be sharded and replicated across multiple nodes, with support for full cross-shard transactions.

It supports SQL queries and joins, as well as schema changes such as adding columns to tables without requiring downtime.

- __Graphs are hard to shard.__

The first challenge is to design a graph database that can be distributed across multiple machines. This requires us to have a mechanism for partitioning the data across the machines in such a way that each machine has only partial information about the graph, but still allows the machines to work together to answer queries.

The second challenge is that in a graph, the same entity can be connected to multiple other entities. In a relational database, this is not an issue because each row in a table has its own unique identifier and hence, it’s easy to identify the entity that is being referenced. However, in a graph, there is no such identifier and hence, it’s not possible to identify the same entity across multiple connections.