// for compilation purpose: this file should be content that gets included elsewhere
// the proposal source will get imported into different templates for pdf generation and static website compilation
// For styled pdf output, use proposal-typeset.typ

#import "template.typ": docHeader

#docHeader(
  title: "Adaptive Parallel Execution of GROUP BY Aggregation Queries",
  authors: (
    (name: "Leon Lu", email: "lianglu@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
    (name: "Hao Kang", email: "haok@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
  ),
)

= Title

*Project Title:* "Adaptive Parallel Execution of GROUP BY Aggregation Queries"

*Team Members:* Leon Lu \<#link("mailto:lianglu@andrew.cmu.edu")\>, Hao Kang \<#link("mailto:haok@andrew.cmu.edu")\>

= URL

*Project GitHub Repository:* #link("https://github.com/haok1402/parallel-groupby")[github.com/haok1402/parallel-groupby]

*Project Web Page:* #link("https://chaosarium.github.io/adaptive-parallel-groupby")[chaosarium.github.io/adaptive-parallel-groupby]

= Summary

We plan to parallelize SQL aggregation queries with `GROUP BY`, specifically focusing on dynamically optimizing the coordination and distribution of work based on column data characteristics. Our goal is to develop an adaptive parallelization strategy for aggregation queries that reduces cache evictions, optimizes synchronization and communication, and adapts to varying data distributions. We will focus on shared-memory multi-core systems and, as bonus challenges, explore scaling to other platform configurations, such as multi-node systems, if we have time.

= Background

SQL aggregation queries with `GROUP BY` are common operations in relational databases. In simple terms, these queries aggregate the value of a specific column based on a grouping condition, such as `SELECT AGGREGATION_FUNCTION(column1) FROM table1 GROUP BY column2`. However, performance challenges arise when the distribution of values in the grouping column (i.e., `column2`) is highly skewed or unknown. This makes it difficult to partition the work, balance computation across threads, and predict the overhead needed to merge together aggregated results.

// TODO diagram showing group by aggregation

Without considering parallelization, the aggregation algorithm is not complicated: one common approach is to keep a hash table mapping the group key (i.e. distinct values in the column being grouped by) to the aggregated value so far @raasveldt_parallel_2022. To parallelize grouped aggregation, one could partition the input data and compute partial aggregation results for each partition in parallel, but challenges arise when we think about how the partial results should be merged together.

Many modern databases already implemented some level of parallelism for `GROUP BY` operations. One common approach performs aggregation in two phases: first divide the rows into contiguous chunks and assign each chunk to a separate thread for aggregation @raasveldt_parallel_2022, then merge the partial results to form an overall answer. The second phase can be centralized @raasveldt_parallel_2022 or done in parallel @shatdalAdaptive1995 @raasveldt_parallel_2022. This merge operation, however, can be expensive in situations where there are lots of group keys. Another approach is to first partition input data according to the group key @raasveldt_parallel_2022, avoiding the extra work of merging results associated with the same group key across partitions. This may help reduce the merge overhead but at the expense of additional work to assign work and risk load imbalance due to skewed distribution of group by keys.

#figure(
  image("2phase illustration.png", width: 80%),
  caption: [
    An illustration of the different merge cost when performing two-phase parallel aggregation (with two threads) for different distributions of the group key. Top: due to a large number of distinct group keys, the two-phase approach resulted in large partial result tables that are expensive to communicate and merge. Bottom: since there are fewer distinct group keys, there is less overhead to storing and merging partial aggregation results.
  ]
) <figTwoPhaseIllustration>

To illustrate, consider a table where the column being grouped by contains as many distinct values as the number of rows in the table, such as the top example shown in @figTwoPhaseIllustration. If we follow the two-phase strategy, each thread will create an aggregation table with as many entries as its portion of the input, and the merge process will thus involve, in some way, checking every group key against every other partial aggregation table to confirm that no merging is needed. With a single-node, multi-thread setup, this process could mean high memory consumption and cache eviction rate as threads compete for cache space used for their partial result, followed by an expensive second phase. To lower the merge cost for both examples in @figTwoPhaseIllustration, one could partition the input data by the group key to the same group key being included in multiple partial aggregation results, but such partitioning would be extra work. Additionally, if the entire table only contains a few unique group keys with imbalanced frequency, assigning data to threads by the group key could result in load imbalance.

Another interesting consideration is that different types of aggregation of aggregation can also lead to different performance characteristics @grayData2007, including _distributive aggregations_ like minimum, sum, and count for which each thread only needs to keep track of one state and the merge ordering doesn't matter, _algebraic aggregations_ like finding the $k$ numbers, which requires tracking extra data in the partial results, and _holistic aggregations_ such as finding the most frequent element, which can require unbounded memory to store intermediate results. Aggregations other than distributive aggregations require communicating extra data and doing extra work beyond applying the aggregation function on the partial results, making them more challenging to parallelize. 

Our project will focus on enhancing existing methods by creating an adaptive strategy that dynamically partitions and assigns work based on the group keys. This involves examining the tradeoffs between different methods of work assignment, scheduling, minimizing cache misses, reducing synchronization overhead, and possibly leveraging SIMD instructions where possible#footnote[Although SIMD seems like an optimization orthogonal to the adaptive stretegy, they may shift the proportion of time we spend aggregating vs. communicating, which might affect the cost model we use.].

= The Challenge

The problem is challenging for several reasons:

+ *Unknown Data Distribution:* We do not know in advance how the data will be distributed across the grouping column. This creates a problem in estimating the optimal parallel execution algorithm. Additionally, the distribution may also change as we stream through more data. A challenging aspect of our project is, therefore to try to predict the cost of future execution when having only seen part of the input.
+ *Overhead of Adaptation:* Given the unknown data distribution, it is difficult to select the best strategy for parallelism when we start to execute the query. With our adaptive approach, we aim to switch to a more optimal strategy mid-way through execution in the hope that we still finish sooner despite the overhead of analyzing different strategies and performing a strategy switch (e.g. cost of switching to a different data structure) when deemed beneficial. In order to perform effective adaptation, we will likely need a cost estimation model that understands the performance characteristic of each parallel strategy as well as the cost of switching between any pair of strategies.
+ *Cache Efficiency:* Cache capacity is a resource constraint we need to keep in mind. Building multiple large aggregation tables in parallel can lead to significant cache misses, as rows that should be close together may end up being scattered across the cache, causing costly evictions and slowdowns. One potential problem for cache optimization is figuring out how to partition the input data to minimize the number of group keys that are duplicated on multiple threads.
+ *Synchronization and Communication when Aggregation:* The overhead of merging results from different threads can become a dominating factor as the number of distinct group keys grows because threads have to communicate more partial results for the same amount of input data. When the number of group keys seen by each thread is imbalanced, we can also experience an imbalance in communication. 
+ *Synchronization and Communication for Adaptation:* Before the final merger of all partial aggregation results, threads also need to communicate about the distribution of the data they have seen so far. This information needs to be gathered to make a decision on how to adapt. The adaptation then needs to be agreed upon and executed by all processors. This whole process may need to happen multiple times during query execution, which can become a significant amount of communication and synchronization if we are not careful.
+ *Lock Contention:* Locks involved when communicating data across nodes can also cause contention issues and performance degradation in multi-threaded or distributed settings. 
+ *Dependencies:* A two-phase parallel algorithm, where we wait for the partial aggregations to finish before merging @shatdalAdaptive1995, makes it easy to reason about data dependency correctness, but it might also mean that we need a barrier between the phases. We could explore, if we find this to be a significant bottleneck, potential opportunities to remove this barrier by having threads that finish aggregating early merge early.
+ *Multi-Node Scalability#footnote[We think this will be a bonus challenge, as there are already many moving parts to experiment with just under a single-node, multi-thread, shared-address-space environment.]:* We also need to consider the potential for distributed execution (across multiple nodes), where communication between different nodes can further complicate synchronization and data partitioning.


= Resources

== Compute

We have available and can utilize the following machines for our project:

+ *Hao's Desktop*
  - *CPU*: Intel(R) Core(TM) i7-14700K (28 cores, 2 threads per core, 20 cores total)
  - *Architecture*: x86_64
  - *Total CPU Threads*: 56
  - *Caches*:
    - L1d: 768 KiB (20 instances)
    - L1i: 1 MiB (20 instances)
    - L2: 28 MiB (11 instances)
    - L3: 33 MiB (1 instance)
+ *GHC Machines*
  - *CPU*: Intel(R) Core(TM) i7-9700 (8 cores, 1 thread per core, 8 cores total)
  - *Architecture*: x86_64
  - *Total CPU Threads*: 8
  - *Caches*:
    - L1d: 256 KiB (8 instances)
    - L1i: 256 KiB (8 instances)
    - L2: 2 MiB (8 instances)
    - L3: 12 MiB (1 instance)
+ *PSC Machine*
  - *CPU*: AMD EPYC 7742 64-Core Processor (128 cores, 1 thread per core, 128 cores total)
  - *Architecture*: x86_64
  - *Total CPU Threads*: 128
  - *Caches*:
    - L1d: 32 KiB (128 instances)
    - L1i: 32 KiB (128 instances)
    - L2: 512 KiB (128 instances)
    - L3: 16 MiB (2 instances)

We believe we have figured out how to obtain the compute resources we will need. Due to our focus on multicore CPUs, we do not need access to any special machine.
    
== Code Base

We plan to start from scratch. The motivation is to simplify the setup so that the effect of the `GROUP BY` executor is isolated. This also helps us focus our work on optimization as opposed to the software engineering to start from other existing systems.

To compare our system to off-the-shelf systems, such as those discussed in @plantoachieve, we run the compiled versions of the off-the-shelf systems.

== Background Literature

The following papers or articles are related work that we should read:

+ "Parallel Grouped Aggregation in DuckDB" is an article from DuckDB describing their approach of parallelizing grouped aggregation queries. Their approach is similar to the two-phase algorithm described by #cite(<shatdalAdaptive1995>, form: "author"). It is stated that DuckDB relies on one single magic number 10000 to decide whether to switch to a partitioned hash table approach, which we believe is a shortcoming when compared to our proposed adaptive approach.
+ "Adaptive Parallel Aggregation Algorithms" by Shatdal and Naughton #cite(<shatdalAdaptive1995>), which discusses adaptive aggregation algorithms for parallel query processing. The paper also provides useful definitions and outlines common parallel aggregation approaches.
+ "Adaptive and Big Data Scale Parallel Execution in Oracle" by Bellamkonda et al., which presents adaptive parallelization techniques in Oracle RDBMS for scalability in SQL queries, including aggregation operations #cite(<bellamkondaAdaptive2013>).
+ "Scalability and Data Skew Handling in GroupBy-Joins using MapReduce" by Hassan and Bamha, focusing on handling data skew in distributed environments, particularly for group-by and join operations #cite(<hassanScalability2015>).
+ "Data Cube: A Relational Aggregation Operator Generalizing Group-By, Cross-Tab, and Sub-Totals" by Gray et al., which defines the data cube operator for generalizing relational aggregation #cite(<grayData2007>). Their theoretical characterization of aggregation functions can provide insight on the type of aggregation functions our strategy needs to be able to optimize for.
+ "Adaptive Aggregation on Chip Multiprocessors" by Cieslewicz and Ross, which explores adaptive aggregation algorithms for multi-core processors and highlights key performance factors for aggregation operations #cite(<cieslewiczAdaptive2007>). Their experiment design involving multiple input distributions can also inform our experiment design.

= Goals and Deliverables

== Plan to Achieve <plantoachieve>

+ A correct, sequential implementation of `GROUP BY` query that works with distributive aggregation functions like `COUNT`, `MIN`, `MAX` and `SUM`.
+ A naive, parallel implementation of `GROUP BY` query that works with distributive aggregation functions and statically assigns each worker a partition of the full table to work with.
+ An optimized, parallel version of `GROUP BY` query that works with distributive aggregation functions and dynamically adapts to various frequency distributions of the group keys and permutation of the row arrangements.
+ An infrastructure for generating various data distributions and a cost model to that include the type of aggregation function, the machine configurations (i.e., cache line size, number of CPU cores, SIMD width, ...), the data distribution processed so far as parameters to guide the engine.
+ An extensive performance benchmark of the above implementations and other popular OLAP databases and toolkits like DuckDB, Pandas, Polars and Arrow.

== Hope to Achieve

6. We expect the performance of our optimized, adaptive parallel implementation to be close-to and slightly-above DuckDB on most data distributions yet take a win on certain types of distributions.
+ We hope the performance of our adaptive system will, for all the test data distributions, reach similar performance to the best non-adaptive strategy for each test data distribution. 
+ Implement the optimized, parallel version of `GROUP BY` query that works with holistic aggregation functions, which has no constant bound on the size of the storage being used to describe a sub-aggregate. These include functions like `Median`, `MostFrequent`, and `Rank`.
+ Implement the `GROUP BY` query on multi-node scenarios, where the dataset has been partitioned by certain criteria in advance, like the primary key. This can be challenging since the keys to group rows by aren't necessarily the same set of keys to partition the table, which can lead to an imbalanced workload across different nodes. To shuffle, not to shuffle, or how much to shuffle, can be an open challenge yet to be resolved.
+ Perhaps, for fun, we could provide a GPU implementation for `GROUP BY` query, though we expect the performance to be worse, due to factors including the data transfer overhead from host to device and the difficulty of replicating certain operations such as testing hash tables or merging results without lots of divergent execution.

= Platform Choice

We will be focusing on multicore CPUs. It is a common practice to deploy databases on multicore processors @cieslewiczAdaptive2007, making it an important platform to optimize for. Moreover, query execution can be complex and contain parts that are not necessarily data parallel, especially when it comes to making adaptive decisions on how to execute the query. 

We decided not to focus on GPUs because simply relying on SIMD programming and using GPUs may not be sufficiently expressive or efficient for implementing and experimenting with our strategies and will thus not be a primary platform choice.

= Schedule


#table(columns: (1fr, 3fr),
  align: left,
  [*Date*], [*Task*],
  [March 26, 2025], [
    - *Submit Project Proposal*.
  ],
  [March 31, 2025], [
    - Develop infrastructure for generating test data according to various distributions.
    - Implement a correct, sequential `GROUP BY` query for distributive aggregation functions.
    - Implement a correct, parallel `GROUP BY` using #cite(<shatdalAdaptive1995>, form: "author")'s two-phase algorithm with centralized merge phase and a static partitioning strategy.
  ],
  [April 07, 2025], [
    - Expand the two-phase parallel `GROUP BY` implementation to include a parallel merge phase using an approach similar to DuckDB @raasveldt_parallel_2022.
    - Implement a correct, parallel `GROUP BY` repartitioning algorithm described by #cite(<shatdalAdaptive1995>, form: "author").
    - Design and implement an API for cost estimation during execution.
    - Implement a basic cost estimation model to predict the remaining execution time of any given strategy.
    - Benchmark existing OLAP engines (DuckDB, Pandas, Polars, Arrow) on our generated test data.
    - Obtain initial performance data for the three non-adaptive strategies
    - Begin profiling the three non-adaptive strategies to understand their shortcomings
  ],
  [April 15, 2025], [
    - Implement an optimized, parallel `GROUP BY` query that uses the cost estimation model to adapt to the data distribution seen so far.
    - Create infrastructure to evaluate the accuracy of our cost models.
    - *Submit Project Milestone Report*.
  ],
  [April 22, 2025], [
    - Expand the cost model to include machine configurations and data distribution parameters.
    - Perform extensive benchmarking of optimized implementations against OLAP databases.
  ],
  [April 28, 2025], [
    - *Submit Final Project Report*. 
    - Evaluate potential extensions: holistic aggregations (`Median`, `MostFrequent`, `Rank`)
    - If we have time, we can work on bonus challenges like multi-node execution and GPU acceleration.
    - *Create presentation poster*.
  ],
  [April 29, 2025], [
    - *Do the presentation*. 
  ],
)

#bibliography("references.bib")