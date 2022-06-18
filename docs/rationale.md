# Rationale for Cathlamet

Purpose of this document is to provide a rationale for the design of the Cathlamet system. We can think of it as
an attempt to provide a high-level framing of needs we seek to address without anchoring ourselves to deep in
specific individual use cases or functional requirements.

## A gap in the database management systems landscape

Database management systems (DBMS) are a foundational element of the modern computing landscape, and over
the previous decades a plethora of such systems have been developed in industry and academia. To illustrate this fact,
we call out the [db-engines](https://db-engines.com/en/ranking) ranking of database systems, which at the time of 
writing this document collected information about 363 distinct systems. However, when we  reflect on the existing 
categorizations and classifications that are being used to describe various systems, we come to the conclusion that
there is a conceptual gap hiding in this abundance of systems. Specifically, when we look at categoizations of DBMS,
we typically find two forms of categorization applied:

- General-purpose versus special-purpose
- Single-model versus multi-model

### General-purpose versus special-purpose RDBMS

General-purpose DBMSs aim to meet the needs of as many applications as possible. At some point, general-purpose DBMS systems were considered effectively equivalent to relational systems implementing 
the [SQL language](https://en.wikipedia.org/wiki/SQL) and providing 
[ACID transactions](https://en.wikipedia.org/wiki/ACID). The common implmentation techniques included data stores 
utilizing [BTree data structures](https://en.wikipedia.org/wiki/B-tree), 
[Write-ahead Logging (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging) for durability (such as 
[ARIES](https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics)), and primary/stand-by 
failover to achieve [high availability (HA)](https://en.wikipedia.org/wiki/High_availability).

However, in some cases a general-purpose DBMS may introduce unnecessary overhead and may thus not be the best solution 
for given data management problem. Special-purpose DBMS seek to overcome these limitations by specifically addressing 
the needs of the specific problem. A common example is an email system: email systems are designed to optimize the 
handling of email messages, and do not need significant portions of a general-purpose DBMS functionality.

Such challenges to a general-purpose DBMS may concern the underlying data model, "Big Data" qualities (such as
volume, velocity, variety) as well as the operational environment (embedded, cloud, edge), special performanced needs
(such as realtime) or resource and cost constraints. 

### Single-model vs multi-model RDBS

Here we are using the underlying datamodel of the DBMS as differentiating attribute. The most common models are:

- Relational model: The [relational model](https://en.wikipedia.org/wiki/Relational_model), originally proposed 
    in the late 60-ies and commercialized with the advent of SQL DBMS systems represents data as collections of
    tables with identified relations between them.
- Document model: The document model allows storage and retrieval of nested data structures. Early examples were
    databases designed as storage engines for document management systems (such as XML databases). With the NoSQL
    movement, the document model has also become popular for storing more general nested structures, such as JSON 
    objects.
- Key-value: Probably the simplest data model employed in DBMS is the key value model, where each entry is comprised
    by a key that serves as identifier and an associated data payload value. The key-value data model is particular
    useful for describing caches and received quite a bit of attention in the early days of the NoSQL movement.
- Graphs: The graph data model represents data as identifiable nodes, commonly with additional attribute values, which
    are interlinked using direct, traversable edges. 
- Time series: Time series organize data through associated pairs of time(s) and values, which commonly represent 
    profiles, curves, traces or trends or sensor data.
- Geospatial: Geospatial data is organized along two or three dimensions and is often accessed with a notion of proximity.
    Spatial and temporal aspects are often combined, leading to spatio-temporal data models.

## Vision: Re-inventing the general purpose database management system


- We have single-model and multi-model database management systems
- We have general-purpose and special-purpose database management systems
