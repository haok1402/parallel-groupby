#import "template.typ": docHeader

#docHeader(
  title: [Adaptive Parallel Execution of GROUP BY Aggregation Queries: Milestone Report#footnote[Link to milestone report: #link("https://chaosarium.github.io/adaptive-parallel-groupby/web-milestone.html")[chaosarium.github.io/adaptive-parallel-groupby/web-milestone.html]]],
  authors: (
    (name: "Leon Lu", email: "lianglu@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
    (name: "Hao Kang", email: "haok@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
  ),
)

= Background 

Our project investigates the parallelization of `GROUP BY` query execution in relational databases. Due to potentially unknown input data distribution, determining an effective strategy for `GROUP BY` query execution can be challenging. Many notable challenges arise when partial aggregation results computed on different threads need to be merged together, as detailed in the project proposal.

= Progress Summary and Schedule Review 

*Summary:* We created benchmarking tools to facilitate testing and evaluating parallel aggregation systems, including our system and off-the-shelf systems (See @test-infra). We created test aggregation queries that we run against the TPC-H#footnote[https://www.tpc.org/tpch/] dataset. We attempted to work with DuckDB's built-in tool and its code base to better understand the multithread scaling of its aggregation operation but with limited success. We switched back to creating our own implementation that isolates the grouped aggregation operation and implemented various baseline strategies, as well as a simple adaptive strategy similar to DuckDB's approach (See @base-par-strat). We collect data on off-the-shelf systems and our own implementations and discuss some preliminary observations.

*Schedule Review:* The completion status of our schedule up until 15 April is as follows. The last row outlines unplanned items that were completed.

#text(size: 0.8em)[
#table(columns: (1fr, 2fr, 1fr),   inset: (x: 0.4em, y: 0.6em),
  align: left,
  [*Original Schedule Date*], [*Task*], [*Comment*],
  
  [March 26, 2025], [
    - *Submit Project Proposal*.
  ],
  [Achived],
  [March 31, 2025], [
    - Develop infrastructure for generating test data according to various distributions.
    - Implement a correct, sequential `GROUP BY` query for distributive aggregation functions.
    - Implement a correct, parallel `GROUP BY` using #cite(<shatdalAdaptive1995>, form: "author")'s two-phase algorithm with centralized merge phase and a static partitioning strategy.
  ],
  [Achieved with delay on the sequential and parallel solutions to between April 7 and April 15. Attention was shifted to evaluating DuckDB and trying to work with its code base],
  
  [April 07, 2025], [
    - Expand the two-phase parallel `GROUP BY` implementation to include a parallel merge phase using an approach similar to DuckDB @raasveldt_parallel_2022.
    - Implement a correct, parallel `GROUP BY` repartitioning algorithm described by #cite(<shatdalAdaptive1995>, form: "author").
    - Design and implement an API for cost estimation during execution.
    - Implement a basic cost estimation model to predict the remaining execution time of any given strategy.
    - Benchmark existing OLAP engines (DuckDB, Pandas, Polars, Arrow) on our generated test data.
    - Obtain initial performance data for the three non-adaptive strategies
    - Begin profiling the three non-adaptive strategies to understand their shortcomings
  ],
  [
    Achieved all of the non-adaptive parallel implementions or their replacement (See @base-par-strat) with some delay. 
    Cost estimation and profiling is pushed off following some discussions and pending investigation about shifting towards cache-aware adaptations instead of adaptations based based solely on data distribution (See @challenges).
    Initial benchmark results for off-the-shelf OLAP engines and non-adaptive strategies are obtained (See @prelim-res). We decided to focus on DuckDB and Polars due to their good parallelism support.
  ],

  [April 15, 2025], [
    - Implement an optimized, parallel `GROUP BY` query that uses the cost estimation model to adapt to the data distribution seen so far.
    - Create infrastructure to evaluate the accuracy of our cost models.
    - *Submit Project Milestone Report*.
  ],
  [
    As mentioned above, whether to focus on adapting to data distribution or cache usage is under some pending investigation and discussion. We have not completed the original plan of creating a cost model based on data distribution.
    Project milestone report is complete.
  ],

  [Items not originally planned], [
    - Profile DuckDB's execution of various grouped aggregation queries on TPC-H at various scaling factors.
    - Study the runtime of each operator in the execution plan and analyze the per-operator speedup or latency as the number of threads changes.
    - Read the DuckDB scheduler code.
    - Attempted to modify DuckDB's scheduler to scale down the number of threads used for a given operator under some condition, but experienced difficulty given its thread pool architecture.
  ],
  [
    After some discussions, we realized that DuckDB's adaptation to data distribution should already work well in many cases and were not sure how significant of an improvement we can make. However, for reasons discussed in @duckdb-detour, we spent a week trying to work within DuckDB.
  ]
)
]

== Testing Framework Development <test-infra> 

We spent some time developing toolings to benchmark various query execution systems, including off-the-shelf reference systems and our own implementation. Some parameters supported by our test system:

- The number of threads
- The input database (we use DuckDB `.db` file as input)
- The parallel execution strategy
- The execution system
- The number of trials, the number of warmup runs, etc.
- Transformations applied to the input data

We also implemented a data generation tool in which we can control the number of group keys, number of rows, the distribution of group key counts, etc. However, it was not efficient enough to generate large input files (maybe Python was not the best idea).

We realized that evaluating query execution can be tricky. When evaluating off-the-shelf systems, for example, we can see different behavior depending on whether the same query has been executed previously, possibly because of data persisting in the buffer pool. Additionally, different systems may do different work, such as Polars producing the output in a dataframe format but DuckDB writing to a temporary table. Because of many hidden unknowns like this, we designed our experiments to follow certain evaluation principles, including:

- We focus on steady state performance—that is, when data are already in memory and and have hopefully been previously accessed. This is likely the common case where the aggregation operator takes input written by upstream executors. We achieve this by first running the test query for a few warm-up runs and then discarding the results. We then run multiple trials and take the average.
- When comparing our implementation to off-the-shelf systems that include writing to output, we include the time it takes for our own implementation to write all the results to a memory location.
- When comparing across strategies we implement ourselves, we do not focus on the time it takes to write to output. We consider the aggregation operation to be complete once the output data can be trivially iterated over (e.g. once all final values of result rows are computed and stored in a vector, a map, a vector of vector, etc.). 
- Some strategies have similar phases, such as scanning the input table and building thread-local partial aggregation result tables. We will therefore collect and study the per-phase runtimes for the different strategies and investigate the trade-off between different phases as thread count changes.

== A DuckDB and Cache Detour <duckdb-detour> 

After discussion with TA about the effectiveness of data distribution that is already implemented in DuckDB, we realized that their approach might already be suitable for many input data distributions. We benchmarked TPC-H queries on DuckDB and, while initially noticing performance degradation at high core counts, the performance issue goes away on subsequent runs of the same query, likely due to earlier runs being cold (See @test-infra). Somewhat unfortunately, we learned that DuckDB profiler's per-operator runtime reporting may not be reliable in a multi-threaded setting, so it was difficult to tell what the speedup of the grouped aggregation operator is in isolation.

One challenge we anticipated earlier was that hash tables can consume cache space and so having too many thread-local partial aggregation tables can result in high eviction. In theory, then, there might be a point beyond which adding more threads slows down parallel aggregation. If this happens, it would arguably be beneficial for the system to detect this and reduce the number of threads executing the aggregation query dynamically. 

The morsel scheduling paper suggested that it is usually beneficial to keep all worker threads busy even if obtaining work requires data movement, so if we demonstrate cases where cache bottleneck results in an upside-down U-shaped speedup curve, we might have good evidence that there are situations where keeping all threads active negatively impacts performance. However, we have not yet identified input data and queries that result in this situation.

If such a situation does arise, it would be very interesting to see if we can modify the DuckDB scheduler to become aware of performance degradation at run-time and scale back the number of threads used to execute the grouped execution query.


== Baseline Sequential Implementation <base-seq-impl> 

We implemented simple hash-map based aggregation, which can be used to check the correctness of our other implementations. We tried several programming languages due to seeing confusing initial performance. We were eventually able to fix a performance bug and achieve reasonable performance. Two of our implementations, in C++ and Rust respectively, had better single-thread performance than DuckDB, which seems reasonable as DuckDB probably have other overheads. Our sequential Go implementation was slower than DuckDB.

== Baseline Parallel Strategies <base-par-strat> 

As proposed, we implemented various parallel strategies for preliminary testing, such as centralized 2-phase aggregation outlined in #cite(<shatdalAdaptive1995>, form: "author"). After additional research and discussions with TA, we found some strategies that are not mentioned in our original schedule, including DuckDB's approach that uses radix partitioning @raasveldt_parallel_2022. Finally, we also discovered that certain strategies we intended to implement are not actually designed for shared-memory space architecture, namely data repartitioning @shatdalAdaptive1995. We implemented an alternative strategy that somewhat mimics the repartitioning strategy.

We decided to use OpenMP as our parallelism framework as we are working with a shared address space setup and often needed parallel for loops. OpenMP facilitates dynamic scheduling over things like rows, partitions, etc. However, we do acknowledge that it does not fully replicate a full DBMS environment with buffer pools, schedulers, etc. One thing we spent too much time earlier was trying to figure out all the complicated moving pieces in DuckDB, so we think a simpler OpenMP would let us better focus on doing the parallelism by isolating the grouped aggregation execution step in a more isolated environment we control.

So far, the strategies we implemented are as follows:

+ *Global Lock:* Have all threads scan a subset of input and update the same hash aggregation table in a critical section.
+ *Two Phase Centralized Merge:* Consisting of a first phase where each thread builds a partial aggregation table and a second phase where a single thread merges the results, similar to the centralized two-phase algorithm presented by #cite(<shatdalAdaptive1995>, form: "author")
+ *Simple Two Phase Radix:* Similar to Two Phase Centralized Merge, but instead of keeping one partial aggregation table per thread, we partition the group key hashes by their lowest $log_2(k N)$ bits and keep a partial aggregation table per partition per thread, where $k$ is some constant and $N$ is the number of threads. This way, the partition for different bit suffixes can be merged independently and in parallel.
+ *Simple Three Phase Radix:* Similar to Simple Two Phase Radix, but instead of always going through two layers (partition access then hash map access) of indirection, to update entries in the aggregation map, we first create one partial aggregation result in phase 1. In phase 2, each thread splits its local aggregation map into martitions, and in phase 3, threads are assigned to merge partitions in parallel.
+ *Implicit Repartitioning:* This is to replicate a similar effect as data repartitioning @shatdalAdaptive1995. The original algorithm repartitions the data by their group key and sends data over the network. In our shared-memory setup, there is no need to send data over network. Instead, we have each thread scan the whole table but only aggregate group keys match their thread ID, effectively producing partial aggregations on what would be repartitioned data.
+ *DuckDB-Like Two Phase:* DuckDB uses a single threshold to decide whether to switch from having a single thread-local aggregation table to using radix partitioning. We replicate their simple adaptation algorithm. The aggregation starts the same as Two Phase Centralized Merge, but if at least one thread sees more than 10,000 (or some other configurable constant, but 10,000 is what DuckDB does) tables, it switches to radix-partitioning and informs other threads to do the same. The merge phase is done sequentially or in parallel, depending on whether radix partitioning is used.

Interestingly, even though our sequential solution is faster than DuckDB, running these parallel strategies with 1 thread is slower than DuckDB. We suspect this might be due to some OpenMP overhead.

= Goal and Deliverable Review 

== Plan-to-Achieve

Among the 5 plan-to-achieve items, we have completed or are on track to complete at least 3: a sequential reference implementation, various basic parallel grouped aggregation strategies, and a pipeline for benchmarking approaches. In terms of generating input data of certain distribution and developing cost models to adapt to data distribution, we should be able to implement them; they are indeed determined to be the optimization we want to focus on. As discussed in @duckdb-detour, we should gather more data to determine whether we should adapt directly to the input data distribution or adapt indirectly by being more aware of cache evictions. Regardless, we think we will have some implementation that does adaptation in some form.

== Hope-to-achieve

Based on preliminary results, it appears that DuckDB scales quite well for typical input data, and given the overhead from running OpenMP alone (@base-par-strat), it seems unlikely that we can get close to DuckDB's performance. However, looking at the scaling in our preliminary results (@prelim-res), it seems that, with some more work, we can achieve a similar scaling.

= Presentation Plan 

We plan to present graphs showing experiment results in the poster session. We have conducted many preliminary experiments on baseline approaches that we have implemented. Some of the data we gather include speedup, overall query latency, as well as per-phase execution time of steps within our parallel aggregation algorithms. We imagine the results to be presented will be similar to those in @prelim-res, but with more sophisticated parallel implementations and more interesting input data.

= Preliminary Results and Discussions <prelim-res> 

== Results

=== Baseline, Distributive Aggregation, Hao's Desktop, TPC-H SF-1:

To understand how existing engines handle parallelizable workloads, we benchmarked distributive aggregations (i.e. `SUM`) using DuckDB and Polars across varying thread counts. For each configuration, we performed 3 warm-up runs followed by 5 measured trials and report the average runtime.

As shown in the latency plot (right), DuckDB is significantly faster than Polars across all thread counts, with sub-0.05 ms latency as early as 4 threads. In contrast, Polars starts with over 3ms latency on a single thread and remains above 0.5ms even at 16 threads. This highlights DuckDB's efficient single-threaded execution and superior scalability.

The speedup plot (left) further confirms that both engines benefit significantly from increased parallelism, but DuckDB sales more efficiently, reaching nearly 7x speedup at 16 threads, compared to around 6x for Polars.

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/baseline_speedup_desktop_sf1_distributive.png"),
    image("figures/baseline_latency_desktop_sf1_distributive.png"),
  ),
  caption: [
    Left: Speedup across thread counts for baseline engines (DuckDB and Polars). Right: Latency across thread counts for the same engines.
  ],
)

=== Baseline, Distributive Aggregation, Hao's Desktop, TPC-H SF-10:

In a second, similar experiment, we extended this analysis to TPC-H with scale factor 10 (SF-10), a larger and more demanding variant compared to TPC-H SF-1. This allows us to examine performance trends evolve under heavier workloads.

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/baseline_speedup_desktop_sf10_distributive.png"),
    image("figures/baseline_latency_desktop_sf10_distributive.png"),
  ),
  caption: [
    Left: Speedup across thread counts for baseline engines (DuckDB and Polars). Right: Latency across thread counts for the same engines.
  ],
)

=== Baseline, Holistic Aggregation, Hao's Desktop, TPC-H SF-1:

In the third part of our evaluation, we examined holistic aggregation, specifically computing the median, which requires maintaining non-constant-sized intermediate states. This workload is inherently more complex than distributive functions like `SUM`, and typically demands additional memory and coordination. Interestingly, we observed a consistent anomaly in DuckDB's 2-thread performance, where it runs slower than the single-threaded baseline. The cause of this degradation is currently unclear, but it may stem from thread contention, synchronization overhead, or suboptimal scheduling in this configuration.

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/baseline_speedup_desktop_sf1_holistic.png"),
    image("figures/baseline_latency_desktop_sf1_holistic.png"),
  ),
  caption: [
    Left: Speedup across thread counts for baseline engines (DuckDB and Polars). Right: Latency across thread counts for the same engines.
  ],
)

=== Baseline, Holistic Aggregation, Hao's Desktop, TPC-H SF-10:

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/baseline_speedup_desktop_sf10_holistic.png"),
    image("figures/baseline_latency_desktop_sf10_holistic.png"),
  ),
  caption: [
    Left: Speedup across thread counts for baseline engines (DuckDB and Polars). Right: Latency across thread counts for the same engines.
  ],
)


=== Ours, Distributive Aggregations, Hao's Desktop, TPC-H SF-1:

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/ours_speedup_desktop_sf1.png"),
    image("figures/ours_latency_desktop_sf1.png"),
  ),
  caption: [
    Left: Speedup across thread counts for our distributive aggregation strategies. Right: Latency across thread counts for the same strategies.  
  ],
)

=== Ours, Distributive Aggregations, Hao's Desktop, TPC-H SF-10:

#figure(
  grid(
    columns: 2,
    gutter: 1em,
    image("figures/ours_speedup_desktop_sf10.png"),
    image("figures/ours_latency_desktop_sf10.png"),
  ),
  caption: [
    Left: Speedup across thread counts for our distributive aggregation strategies. Right: Latency across thread counts for the same strategies.  
  ],
)

== Discussions 

With off-the-shelf systems like DuckDB and Polars, we generally see good scaling on distributive aggregation. Interestingly, DuckDB's scaling seems worse for holistic aggregation (`MEDIAN` to be specific).

Other than degenerate strategies (global locking and centralized merge), our implementations of basic parallel strategies seem to have okay scaling. Very unexpectedly, however, Implicit Repartitioning had the lowest latency among all approaches at high thread count, along with good scaling up to 8 threads. This is not what we expected as the approach is, in theory, work-inefficient—each thread needs to scan through the entire input data. We have yet to figure out why we are observing this situation. One hypothesis is that even though we read input data much more often, there is some level of temporal locality across threads.

When our particular test query is run against TPC-H data, the resulting number of group keys is large, making the DuckDB-Like Two Phase strategy similar to Three Phase Radix. It appears that, for this particular query, it is more beneficial to start doing radix partitioning directly (as done by Two Phase Radix), rather than paying the rehashing to partition after the scan. If we see a different pattern on other input data or queries, this might be a promising opportunity for adaptation.

As expected, we observe that, for approaches that involve a parallel scan phase and a merge phase, increasing the number of threads often trades higher merge cost for faster parallel scan, and the sweet spot seems to vary depending on the approach.


= Challenges Faced <challenges> 

We identify some observations or challenges we hope to address that we did not initially anticipate.

- We probably spent too much time trying to work within the DuckDB code base. DuckDB is a complex system with many moving pieces, and we found it difficult to isolate the effect of parallelizing the grouped aggregation component. We think we will continue working on our own implementation for more control.
- We are noticing situations where fully-parallelized table (i.e. each thread scanning different chunks independently) scans gain little additional speedup beyond a certain thread count and suspect some memory bottleneck at play. If this is the case, data compression might be needed to effectively parallelize the aggregation.
- It is somewhat difficult to perform large-scale testing because of the large disk requirement and the logistics of generating and moving gigabytes of files, so we might want to look for some more efficient ways to experiment with varying data distributions (instead of, say, generating 20GB of input file for every input distribution)
- Although OpenMP lets us focus our time on the parallelism aspect, its overhead seems more costly than off-the-shelf systems, so we might not be able to fully replicate the buffer pools and schedulers that a real DBMS would use.
- Our theory about cache concention slowing down multithread aggregation (@duckdb-detour) is still unconfirmed.

= Recalibration 

There are a few key considerations to keep in mind and decisions to make as we move forward:

- The complexity of working with the DuckDB code base makes working with it time-consuming, and writing our implementation within DuckDB might give us limited control over what happens in the system. Due to limited time, we think we should focus on our own implementation. 
- We need to make an important decision of whether the focus of our adaptation should be cache-awareness or input data distribution. We need a better way of generating test query and data while keeping in mind disk constraint. Hopefully, we can find data distributions that lead to different performance characteristics, including cache usage characteristics. 
- Instead of trying to compete with off-the-shelf systems, which might have other optimizations other than execution strategy adaptation, we should probably focus more on scaling.
- It is possible that simpler aggregations like `SUM` become memory bound once enough threads try to scan the input.
- Seeing DuckDB's worse scaling when computing `MEDIAN`, we might find more opportunities parallelizing holistic query @grayData2007 and/or window aggregation @wesleyFast2021
  
We readjust our schedule to the following:

#text(size: 0.8em)[
#table(columns: (1fr, 3fr),
  align: left,
  [*Date*], [*Task*],
  [April 22, 2025], [
    - Improve validation of the correctness of aggregation result beyond spot checking.
    - Implement a more efficient input data generation pipeline to facilitate testing with more control over input data distribution.
    - Make a decision on whether to focus on cache adaptation or data distribution adaptation.
    - Begin profiling the three non-adaptive strategies to understand their shortcomings
  ],
  [April 26, 2025], [
    - Implement an more optimized executor that uses the cost estimation model to adapt to data or performance characteristics seen so far in the execution, which could include any of:
      - Cost estimation model based on data distribution
      - Run-time timing collection and performance some degredation detection mechanism
      - Mechanism by which threads can collectively agree how to adapt
    - Perform extensive benchmarking of optimized implementations against OLAP databases.
  ],
  [April 28, 2025], [
    - *Submit Final Project Report*. 
    - Evaluate potential extensions: holistic aggregations (`Median`, `MostFrequent`, `Rank`)
    - *Create presentation poster*.
  ],
  [April 29, 2025], [
    - *Do the presentation*. 
  ],
)
]


#bibliography("references.bib")