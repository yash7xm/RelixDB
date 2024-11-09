# RelixDB

##### High-Performance Relational Database System

RelixDB is a custom-built key-value and relational database system developed in Go. It features advanced B-tree indexing, persistence, and concurrency, supporting efficient data storage and querying. It also offers a custom query language and atomic transactions, making it a powerful tool for various database applications.

### Features

##### Key-Value Store

-   Efficient B-Tree Indexing: For fast and scalable data lookups.
-   Persistence to Disk: Ensures data durability and crash recovery.
-   Free List Management: Reuses disk space optimally by managing free pages effectively.

##### Relational Database

-   Rows and Columns: Supports traditional relational data structures.
-   Range Queries: Perform efficient range-based queries on indexed data.
-   Secondary Indexing: Enables faster lookups for non-primary key fields.

##### Concurrency & Transactions

-   Atomic Transactions: Ensures all-or-nothing execution, maintaining data integrity.
-   Concurrent Readers/Writers: Allows simultaneous reads and writes, improving throughput under high load.
-   ACID Compliance: Guarantees data reliability through atomicity, consistency, isolation, and durability.

##### Custom Query Language

-   Parser and Executor: A custom query language parser for creating, updating, and querying data.
-   SQL-like Syntax: Simple and intuitive for developers familiar with SQL-style commands.

### Getting Started

##### Installation

To install RelixDB, clone this repository and build the project:

```
git clone https://github.com/yash7xm/Relix.git
cd RelixDB
go build
```

##### Architecture

RelixDB is built with a focus on:

-   B-tree indexing: Efficient data structure for fast retrievals and range queries.
-   Persistence: Data is safely stored on disk using a log-structured approach.
-   Concurrency Control: Allows multiple transactions to be processed concurrently, using optimistic concurrency techniques.

##### Components

-   B-Tree Index: Used to organize data pages for fast access and updates.
-   Free List: Tracks unused pages for efficient space reuse.
-   Transaction Manager: Handles atomic transactions and ensures ACID properties.

##### Performance

RelixDB has been optimized for:

-   High throughput: Supports concurrent transactions with minimal contention.
-   Scalability: Efficiently handles large datasets through indexing and optimized query execution.
-   Durability: Safeguards data through persistent storage and transaction logging.

##### Contributing

Contributions are welcome! Please fork the repository, create a new branch, and submit a pull request.

##### License

RelixDB is open-sourced under the MIT License. See the LICENSE file for more information.
