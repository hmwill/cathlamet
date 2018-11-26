# Quick Notes

## System Overview

Cathlamet provides a distributed data processing cluster. Initially, its capabilities are exposed
as SQL database, which can be accessed using HTTP requests. We anticipate that later versions will 
provide additional developer APIs and system interfaces.

The processes making up a Cathlamet cluster are the following:

- _Storage_: Storage nodes form the work-horse of a Cathlamet cluster. Storage nodes are responsible
    for storaging one or more data partitions within a clustered database. They also provide
    computational resources for query processing.
- _Router_: Router nodes form the external interface for client applications into a Cathlamet
    cluster. During data ingestion, there role is to route records to the appropriate storage nodes
    within the system. During query processing, router nodes are responsible for farming out 
    query requests to the relevant storage nodes, and to return a single result stream to the client
    based on partial results retrieved from the cluster.
- _Master_: Master nodes are responsible for storing global system meta dat within a Cathlamet 
    cluster. Master nodes are also responsible for orchestrating repartitioning of data in response
    to increasing or decreasing data volumes across storage nodes.
- _Client_: Client processes are processes external to a Cathlamet cluster, which initiate
    operations within the cluster through the _Client API_ component. A particular client process
    is the _CLI_, which provides a direct SQL command line interface for interacting with a 
    Cathlamet cluster.

## Component Overview

- _client_: This component provides a client library to access a Cathlamet cluster from within
    a client process. 
- _common_: This component provides common library components that can be used across all other
    parts of a Cathlamet cluster.
- _core_: This component provides core building blocks of the Cathlamet cluster infrastructure, 
    such as library operating system, serialization, meta data representation and file system 
    abstractions.
- _fabric_: This component provides distributed communication abstractions that are used to
    coordinate data ingestion and query execution across participating processes within a
    Cathlamet cluster.
- _query_: This component provides the machinery for distributed query execution and an 
    internal language to represent queries.
- _service_: This component provides teh service end-point to which the _client_ connects to.
- _sql_: This component provides a SQL implementation and its translation into the internal query 
    language.

## Data Model

### Schema

### Tables

### Partitions

## Client Protocol

### HTTP

Cathlamet provides an HTTP based protocol for client applications. Data collections (tables)
maintained within a Cathlamet cluster can be modified using the following HTTP requests:

- _POST_: Post requests are used to insert new data into an existing table. Tables are organized
    as two-level namespace, comprised of a schema name and a table name. The body of the _POST_
    request is parsed as sequence of JSON-encoded rows, each line corresponding to a data row to
    be inserted into the database. If the primary key is autogenerated, then it is not necessary 
    to include its value as part of the row specification.
- _PUT_: Put requests are used to update existing records in an existing table. The body of the 
    _PUT_ request is parsed as sequence of JSON-encoded rows, each line corresponding to a data row
    to be updated in the database. The primary key must be specified for each row given.
- _DELETE_: Delete requests are used to delete existing records from a table. The body of the 
    _DELETE_ request is parsed as a sequence of JSON-encoded rows, where each line corresponds to
    the primary key of a data row to be deleted from the database. Delete requests are only valid
    for tables with a primary key.

The database can be queried using HTTP _GET_ requests. _GET_ requests take a URL-encoded SQL query 
as query parameter (following a '?'), which is interpreted using the default schema associated with
the client session into which the _GET_ request is being submitted. The result of a _GET_ request
is a sequence of JSON-encoded data rows, each row corresponding to one element of the result set
of the SQL query.

### JSON

#### NULL 

Null values are mapped to the JSON `null` value.

#### Numeric Types

#### BOOLEAN

Boolean values, if not null, are mapped to JSON `true` and `false` literals.

#### CHAR

#### VARCHAR

#### BINARY

#### VARBINARY

#### TIME

Time values are represented as string value following the ISO 8601 conventions. Externalized time 
values are represented using a `hh:mm:ss.sss` format.

#### DATE

Dates are represented as string value following the ISO 8601 conventions. Externalized date 
values are represented using a `YYYY-MM-DD` format.

#### DATETIME

A combination of date and time is represented as string value following ISO 8601 conventions.
Specifically, date and time are combined into a single string using a `<date>T<time>`
representation.

#### Records

Records are represented as JSON objects. The field names are used as key, the value is encoded
using one of the previous encoding schemes.
