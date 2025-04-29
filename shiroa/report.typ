#import "template.typ": docHeader

#import "@preview/babble-bubbles:0.1.0": *
#import "@preview/lovelace:0.3.0": *
#let pp = $hat(p)$
#let aa = $hat(a)$
#let rr = $hat(r)$
#let GG = $hat(G)$
#let gg = $tilde(g)$
#show smallcaps: set text(font: "linux libertine")
#let fEstmBestStrat = text(smallcaps[EstimateBestStrategy])
#let fEstmNumGroups = text(smallcaps[EstimateGrounpCount])
#let fEstmMergeCost = text(smallcaps[EstimateMergeCost])
#let fEstmScanCost = text(smallcaps[EstimateTupleProcessingCost])
#let fUpdateStats = text(smallcaps[UpdateStatistics])
#let fMinCostOf = text(smallcaps[MinEstimatedCostOf])
#let ub = math.underbracket
#show image: it => { align(center, it) }

#docHeader(
  title: [Adaptive Parallel Execution of GROUP BY Aggregation Queries: Final Project Report],
  authors: (
    (name: "Leon Lu", email: "lianglu@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
    (name: "Hao Kang", email: "haok@andrew.cmu.edu", affiliation: "Carnegie Mellon University"),
  ),
)

= Summary

We developed an adaptive parallel execution engine for `GROUP BY` aggregation queries, capable of dynamically selecting execution strategies based on runtime data characteristics. We implemented and benchmarked various strategies—such as two-phase centralized merge, radix partitioning, and lock-free hash table—and compared them against widely used systems like DuckDB and Polars. We develop adaptive algorithms that estimate the data distributions on the fly to optimize performance. Our adaptive algorithms are, in many cases, successful at arriving at the best aggregation strategy available. Our project deliverables include the 4 adaptive execution prototype algorithms under 3 adaptation frameworks, along with extensive benchmarking results. Experiments were primarily conducted on multicore CPU systems at the Pittsburgh Supercomputing Center (PSC).

= Background

== Problem Definition <problemDef>

We narrow our focus on two-column aggregations with distributive aggregation functions (that is, aggregation functions that require no aggregation order requirement and keep track of constant states while aggregating, such as sum, count, and min).

Our algorithm's *input* is a table consisting of two columns:

- `key`: the grouping attribute (`int64_t`)
- `val`: the attribute to be aggregated (`int16_t`)

Its output is the aggregation result that would be produced by executing the following SQL query:

```sql
SELECT key, COUNT(val), SUM(val), MIN(val), MAX(val)
FROM input_table
GROUP BY key;
```

== Constraints <constraints>

In database systems, the number of rows and the number of groups is often not known ahead of time. For example, the group-by operator could consume tuples produced by an upstream operator that filters rows—so we would not know how many rows pass the filter and get sent to the group-by aggregation operator ahead of time. Likewise, we would not know the number of groups in advance. In other words, our algorithm should behave like an online algorithm consuming an input stream.

== Data Structures Used <dstruct>

We primarily use *hash tables* for storing partial and final aggregation results, enabling fast group lookups and updates. We have two special cases of hash tables:

- *Radix-Partitioned Hash Tables:* Partition by key hash to allow independent parallel merging of each partition (See @nonAdptStrats).
- *Lock-Free Hash Tables:* Use atomic operations (e.g., `compare-and-swap`) to allow multiple threads to concurrently insert and update entries without locks, minimizing contention during aggregation.

The key operations on hash tables include:

- *Insert*: Add a new entry corresponding to a new group key.
- *Modify*: Update an existing entry by aggregating new values.
- *Lookup*: Search for an existing group key to determine whether to insert or modify.
- *Merge*: Integrate two aggregation structures, implemented as a series of lookups and insert/modify.

One other data structure non-essential to our primary task is *arrays*. We use simple row-major arrays to store input data. Unlike traditional analytical databases that favor column-major storage, we use row-major format because only `key` and `val` columns are accessed.

== Parallelism Opportunity <parOpportunity>

The input aggregation itself can be parallelized effectively. For example, one simple approach is by assigning each thread to a portion of the input data and have them independently perform the aggregation. However, significant extra work arises during the merging of partial results produced by different threads. While parallel aggregation reduces the initial processing time, this extra work can offset the aggregation benefit. The cost of merging varies depending on the input characteristics, such as the number of distinct group keys. In some cases, the merge overhead can be so substantial that the total execution time exceeds that of a sequential solution. Parallelizing aggregation should therefore pay attention to reducing the extra work at the merge phase.

== Workload and Challenges <workloadChallenges>


The aggregation phase is fully parallelizable, as each thread can independently process disjoint segments of the input table without any synchronization.

In contrast, the merge phase introduces dependencies: producing a single output row requires aggregating all partial results associated with the corresponding group key. Thus, completion of an output row depends on the availability of all relevant intermediate results. Additionally, in the case where we do not know the location of the intermediate results, we would need to check all potential locations of intermediate results to ensure correctness (e.g., we may need to probe all other threads' partial results to find out if there is any data dependency with any other thread).

Parallelism during the merge phase can be increased by structuring the dependency graph into more favorable shapes, such as a tree-based directed acyclic graph (DAG), which enables concurrent merging across different subtrees. Another strategy is to partition the group key space, thereby decomposing the dependency graph into multiple disconnected components that can be processed independently in parallel.

Regarding data locality, the input table aggregation benefits from spatial locality due to its row-major layout, enabling cache-friendly sequential access. However, hash table accesses—necessary during aggregation and merging—typically exhibit poor spatial locality due to their inherently random access patterns. If open addressing with sequential probing is employed, some degree of spatial locality can be partially recovered.

SIMD acceleration is feasible, particularly during the aggregation phase and certain aggregation operations. However, SIMD optimization was not the primary focus of our project, as our work emphasizes higher-level parallel execution strategies and dynamic adaptation to merge dependencies, which offer greater overall performance opportunities under varying data distributions.

== The Group Count Trade-off Between Merge Strategies

It is often the case that, if the number of groups is small, the merge phase is inexpensive even if done sequentially, making optimizing the parallel aggregation phase dominate. However, in the case where the number of groups is large, the merge phase can become so expensive that doing extra work at the aggregation phase to facilitate parallel merge, such as by radix partitioning, can help significantly.

As discussed in @constraints, unfortunately, we often do not know the number of groups in advance, making it hard in practice to balance this tradeoff on the fly.

= Approach


== Technologies Used <techUsed>

We implemented our system in C++ and utilize OpenMP to parallelize computation across multiple threads in a shared-memory environment. CPU programming provides the flexibility we need to express and experiment with adaptation mechanisms. While we did not work within a database system environment, we approximate morsel-driven scheduling with dynamic work scheduling in OpenMP.

Our experiments were conducted on machines provided by the Pittsburgh Supercomputing Center (PSC), which offer high-core-count CPUs and sufficient RAM, suitable for evaluating parallel execution performance on relatively large datasets.

PSC nodes are each equipped with two AMD EPYC 7742 64-Core processors, giving it a total of 128 cores. To avoid any cross-CPU considerations that may influence our benchmarks, we focus mostly on using up to 64 cores. In  @memConsider and @cacheConsider, we conclude that memory bottleneck is not a major concern and that aggregation strategies that use per-thread hash tables may track states that do not fit in the cache.


== Mapping Problem to Hardware


*Cores and Threads.* Each OpenMP thread was pinned to a hardware core where possible. During the initial aggregation phase, threads independently processed disjoint subsets of the input table, exploiting data parallelism at the tuple level.

*Lock-Free Hash Table Strategy.* In the lock-free strategy, all threads performed concurrent updates to a shared aggregation table using atomic operations (e.g., `compare-and-swap`, `fetch-and-add`), which mapped directly to hardware-supported atomic primitives on CPUs.

== Changing the Serial Algorithm to Enable Algorithm


We consider a serial aggregation that consumes tuples and updates entries in a hash table to be the "original serial algorithm". This algorithm does not consist of a merge phase, as there is only one aggregation table.

The original serial algorithm is largely preserved during the initial aggregation phase, where input rows are processed independently in a data-parallel manner. However, significant changes were necessary for merging intermediate results. In particular, parallel execution required the design and implementation of new merging algorithms—such as centralized two-phase merging, tree-based merging, and radix partitioned merging—which are not needed in the purely sequential execution. These adaptations were essential to effectively map the aggregation workload onto a parallel machine and mitigate merge-phase bottlenecks. In cases where the merge algorithm requires a certain data placement, such as in radix partitioning, the aggregation phase is modified to put the hash table entries in the appropriate location in the data structure.

== Code Library Usage


Code libraries we use in this project include:

- *DuckDB:* Used only for loading data and evaluating query results. Although we initially explored DuckDB’s scheduler implementation, we ultimately did not use it in our execution engine.
- *Cyan4973/xxHash:* Imported as a fast, non-cryptographic hash function for use in our aggregation.
- *skarupke/flat_hash_map:* Used for open-addressing hash tables in several baseline aggregation strategies. Note that we implemented our own custom lock-free aggregation map, based on linear probing, atomic operations, and compare-and-swap primitives. No external library was used for that.
- *OpenMP:* Used as the parallelism framework for thread-based data processing.
- *CLI11:* Used as C++ command line parsing library
- *p-ranav/indicators:* Used to make progress bars when generating input data

== Non-Adaptive Strategies <nonAdptStrats>


We retain strategies from our milestone report that we think are interesting to evaluate further or use as strategies that adaptation algorithms can decide to switch to. We introduce two new non-adaptive strategies: Tree Merge and Lock-Free Hash Table. Our non-adaptive strategies include:


+ *Central Merge:* In the aggregation phase, each thread builds a partial aggregation table. In the merge phase, a single thread merges the results. This strategy is similar to the centralized two-phase algorithm presented by #cite(<shatdalAdaptive1995>, form: "author").
+ *Tree Merge:* Use the same aggregation phase as Central Merge. In the merge phase, construct a full binary tree with each thread at a leaf. Merge results from leaves towards the root direction.
+ *Radix Merge:* Instead of keeping one partial aggregation table per thread, we partition the group key hashes by their lowest $log_2(k N)$ bits and keep a partial aggregation table per partition per thread, where $k$ is some constant and $N$ is the number of threads. This way, the partition for different bit suffixes can be merged independently and in parallel. Radix Merge is largely inspired by DuckDB's implementation of parallel grouped aggregation @raasveldt_parallel_2022.
+ *Lock-Free Hash Table:* Only keep one hash table across threads. In the aggregation phase, all threads update the shared hash table. The lock-free hash table is responsible for ensuring correctness amidst concurrent operations. See @lockFreeMethodDesc for implementation details.


== Lock-Free Hash Table Implementation <lockFreeMethodDesc>

Our lock-free aggregation map adopts a single-array, open-addressed design with linear probing. Each slot contains the group key and four aggregate accumulators (i.e., `cnt`, `sum`, `min`, `max`), all declared as `std::atomic<int64_t>`. An unused slot is denoted by `key = INT64_MIN`; this sentinel allows threads to distinguish empty from occupied positions without additional metadata.

During an `upsert`, the thread hashes the key, then probes cyclically until it (i) claims an empty slot or (ii) finds the key it is responsible  for. Claiming uses `compare_exchange_strong` on `key` with `memory_order_acq_rel`, guaranteeing that a thread that wins the race both (a) publishes the key before any updates to the value fields and (b) subsequently observes its own writes. Once ownership is established, updates to the value fields proceed with relaxed atomics:

```
cnt : fetch_add(1)
sum : fetch_add(v)
min : CAS loop on min if v < min
max : CAS loop on max if v > max
```

Because the arithmetic operators are associative and commutative, and because each field is updated with atomic RMW operations, the algorithm achieves lock-free progress --- every contending thread is guaranteed to complete after a finite number of CAS retries, even under aggressive interference.

One important note is that our lock-free hash table implementation does not support resizing. Under the constraint that inputs are read as streams, it would not be possible to determine the size of the hash table in advance. When evaluating a lock-free hash table in isolation, we consider a more relaxed problem where the total number of rows is known. This allows us to understand the performance that is possible with a lock-free hash table, but not evaluate its performance as a practical parallel aggregation solution.

== The Group Count Estimation Problem <ngroupEstm>

As the performance characteristics of the non-adaptive strategies vary a lot depending on the number of group keys, we would like to make estimations of the number of group keys in order to make decisions on how to parallelize. However, this is a difficult problem in general.

To illustrate, consider a situation where we have seen 1000 input tuples so far. If we only observe 4 groups in these 1000 inputs, it would be a reasonable guess that 4 is the number of groups. However, on ther other extreme, if we see 1000 distinct groups among the first 1000 input tuples, it will be very difficult to guess the total number of groups: it could be that we see 1000 more rows each having a unique group key, or 1000000 more rows each with a unique group key.

If the number of group keys seen so far is smaller than the rows seen so far, and we make the assumption that group keys are drawn from a discrete uniform random distribution. Let $S$ be the number of rows seen so far, and let $G$ be the size of the pool of keys we draw samples from. We can compute the expected number of group keys $g$ we will see. Let  $I_j$ be an indicator for whether group $j$ shows up in the $S$ rows seen, we can notice that

$
PP[I_j = 1] &= 1 - PP["group" j "does not show up in" S "rows"] \
&= 1 - ((1 - G)/(G))^S
$

And therefore, by linearity of expectation,

$
EE[ "number of groups seen" ] = sum_(i=1)^G EE[I_j = 1] = G(1 - ((1 - G)/(G))^S)
$ <EEthread>

One way to use this equation in our adaptive algorithm is that, when we see $gg$ groups in $S$ rows, we can set $gg$ to be the expectation and solve for $G$ to obtain an estimated number of groups, $GG$:

$
gg = GG(1 - ((1 - GG)/(GG))^S), "with" S "and" gg "known"
$ <EEthread2>


In practice, we use binary search to approximate a solution to @EEthread2. We observe that the estimation error tends to be high when $gg$ is close to $S$ (for reasons analogous to the 1000 groups in 1000 rows example). We therefore often have thresholds to decide whether we are confident about the prediction, especially when deciding to use a lock-free hash table, which requires committing to a capacity.

== Adaptive Framework I

Using the method derived in @ngroupEstm, our first adaptive algorithm design is to sample a prefix of the number of input rows, estimate $GG$, and make a decision based on $GG$. We set a constant $K = 10000$ as the length of this prefix. This setup defines a class of adaptation algorithms with the following structure:

#pseudocode-list[
  + $K <-$ prefix size for estimation
  + $p <-$ thread count
  + $sigma <-$ data structure to track $S$ and $gg$
  + *while* there exists remaining input row $r$ within the first $K$ rows:
    + $sigma <- fUpdateStats(sigma, r)$
    + Perform aggregation from $r$ using the default _Central Merge_ strategy, using only $1$ threads
  + $GG <- fEstmNumGroups(sigma)$
  + $aa <- fEstmBestStrat(p, GG, S)$
  + *while* there exists remaining input row $r$:
    + Perform aggregation from $r$ using $aa$ using all $p$ threads
  + Combine results from aggregating the first $K$ rows with the aggregation result of the remaining rows
]


== Adaptive Algorithm 1:  Simple Decision Tree

Under _Adaptive Framework I_, we develop the following decision tree to guess which non-adaptive algorithm to use, informed by non-adaptive strategy experiments in @resBaeslines:

#pseudocode-list[
  + *if* $GG < 500000$ and $p < 32$:
    + *if* $p <= 4$:
      + set $aa$ to _Central Merge_
    + *else*
      + set $aa$ to _Tree Merge_
  + *else*:
    + *if* $p < 16$ and $gg < 0.95 S$:
      + set $aa$ to _Lock-Free Hash Table_
    + *else*:
      + set $aa$ to _Radix Merge_
]

Due to many hand-crafted decisions, we anticipate that the decision tree estimation will work well under some situations but may not generalize well to others.

== Cost Models <costModel>

If we can predict future tuple processing and merge costs, we can identify the data structure that would minimize merge overhead. We build cost models to estimate the cost of _Radix Merge_ vs. _Central Merge_ vs. _Tree Merge_. We develop a cost model directly based on the total number of group keys and assume the same hash function with the same hashing cost across all hash tables in the system. We also take into account $S$, the number of tuples we will see, which we can use to estimate the cost of aggregating new inputs and the expected sizes of partial aggregation tables.

Given two hash tables $A$ and $B$, and that there's no guarantee that $A$ and $B$ don't have overlapping keys, we will need to probe the other hash table with every key in one of the hash tables. We can choose the probe from the smaller table to reduce the number of probe operations, but ultimately the size of $A$ and $B$ will be estimated from group-to-row ratios. 

$
fEstmMergeCost(A, B, "keys may overlap") = min(|A|, |B|)
$

But if we know that $A$ and $B$ do not have overlapping keys, we can "merge" them lazily, i.e. stream the rows from one table after another without actually merging the tables. We consider this a constant cost

$
fEstmMergeCost(A, B, "keys do not overlap") = 1
$

When performing radix partitioning, we make the assumption that the groups are divided evenly across partitions:

$
EE[ "number of groups in one partition" ] = EE[ "number of groups seen by one thread" ] / N
$ <EEpartition>

With _Central Merge_, we would merge $p$ hash tables, each with expected size computed in @EEthread. The expected cost is thus:

$
(p-1) dot GG(1 - ((1 - GG)/(GG))^S)
$

With _Tree Merge_, we have $lg p$ level of parallel merges, each containing partial aggregation maps of increasing expected size. Performing a tree merge also incurs overhead synchronization cost that scales with the number of levels. We estimate this to be a factor $lambda > 1$ times the number of levels. We used a value of $lambda = 1.1$. The estimated cost is thus:

$
(lambda lg p) dot sum_(l in {0, ..., (lg p) - 1}) GG(1 - ((1 - GG)/(GG))^(2^l S))

$

When merging radix partitions, each partition can be merged in parallel. If we assume a balanced workload, with thread merging $N / p$ partitions, the estimated span of the merge phase would be:

$
GG(1 - ((1 - GG)/(GG))^S) dot 1/N dot N / p
$

At the same time, radix partitioning is not free: we need to do extra work, hashing the group keys into their partition and another level of indirection to go into the partition's hash table. Due to twice as many indirections, an estimation could be that:

$
fEstmScanCost("radix partitioning") = 2 S \
fEstmScanCost("no radix partitioning") = 1 S
$ <badScanCost>

However, this ignores an important aspect: without radix partitioning, and in the case where there is a large number of group keys, the hash table can grow quickly, incurring a high resize cost. This is indeed supported by our experimental evidence, where, at $r >= 1/40$, radix partitioning is faster at the aggregation phase. We revise @badScanCost to take into account a resizing cost, assuming resize happens every time we double the number of keys (growing to a hash table of size $GG$ will therefore have $GG lg GG$ cumulative resize cost):

$
fEstmScanCost("radix partitioning")  = 2 S + N( GG/N lg GG/N) = 2 S + GG lg GG/N
\
fEstmScanCost("no radix partitioning") = 1 S + GG lg GG
$ <updatedScanCost>

== Adaptive Algorithm 2: Group count estimation and cost model <alg2>

With an estimation on $GG$, the idea is to start estimating the cost of potential future mergers. However, a problem of not knowing the future number of rows arises, as $S$, the number of rows, is an input to our cost models (@costModel).

As a workaround before developing more sophisticated methods, Algorithm 2 makes additional simplifications and assumptions: when the number of rows is needed to estimate the size of partial aggregation tables, we instead take a pessimistic approach of assuming every aggregation table will contain every possible key. When estimating the cost of processing upcoming new tuples, we make a guess that there will be 8 times the number of tuples as the number of keys.



With the simplified cost models and the $GG$ estimation, _Adaptive Algorithm 2_ implements #fEstmBestStrat under _Adaptive Framework I_ by picking the merge strategy with the lowest estimated cost.

One design observation is that the estimate we get from the fixed $K = 10000$ prefix size becomes less and less accurate as the number of groups increases. While _Adaptive Algorithm 2_ might select reasonable merge strategies for cases with low group count, it falls short when we scale the number of group keys.

It would not be reasonable to work around this problem by simply increasing the sample size. From the algorithm's perspective, the input is a stream, and we might need to arbitrarily increase the value of $K$ to get accurate estimations. This leads us to another design idea: _Algorithm Framework II_, which aims to readapt as we learn more about the input data rather than relying on an initial estimation.

== Adaptive Framework II

One issue with _Adaptive Framework I_ is that not knowing the number of rows and thus the number of groups that will end up in the aggregation tables makes cost estimation difficult. _Adaptive Framework II_ aims to address this challenge by expecting an exponentially increasing number of new rows and estimating the best strategy for each increment. In this iterative adaptation process, we can also get more confident about our $GG$ estimation, enabling new strategy choices like _Lock-Free Hash Table_.

_Adaptive Framework II_ follows this general structure:



#pseudocode-list[
  + $B <-$ batch size (i.e. how many tuples per task)
  + step size $S <- B$
  + $p <-$ thread count
  + $pp <- min(4, p)$, initial number of active threads
  + $aa <- $ _central-merge_, initial algorithm choice
  + $sigma <-$ data structure for computing $GG$, the estimated number of groups
  + *while* we have more input data to process:
    + *for* input row $r$ the next chunk of $S$ input tuples:
      + Perform aggregation from $r$ using $aa$ with $pp$ threads
      + *if* some sampling condition met:
        + $sigma <- fUpdateStats(sigma, r)$ 
    + $S <- 2 S$
    + $GG <- fEstmNumGroups(sigma)$
    + $pp, aa <- fEstmBestStrat(pp, p, aa, GG, S)$
    + Perfom any necessary work to switch to strategy $aa$ (e.g. resizing lock-free hash table)
  + Combine results using an algorithm appropriate for the set of all strategies used during execution
]

== Adaptive Algorithm 3: Merge Cost Model and Greedy Merge Optimization 

Under _Adaptive Framework II_, this algorithm implements a #fEstmBestStrat that uses cost models (@costModel) to determine the best merge strategy and a set of conditions based on $GG$ to decide whether to switch to using lock-free hash table. This is an important decision to make as aggregating a high number of group keys can have significant benefits, but under- or over-estimating the size of the hash table to create can entail high resize costs or excessive memory footprints.

_Adaptive Algorithm 3_ only decides to switch to it if it's sufficiently confident with the $GG$ estimation. With an insufficient number of samples, our estimation method often underestimates $GG$. When the number of samples increases and the estimation becomes more confident, its growth rate often decreases (we set a growth rate threshold of 1.2 and require at least having seen 3 million rows for an estimation to be considered confident). Once confident about our estimation, we use a threshold of 300,000 groups to determine whether to switch to a lock-free hash table. If we decide not to switch, the next strategy is determined by $fMinCostOf\($_Central Merge_, _Tree Merge_, _Radix Merge_$)$


It is important to acknowledge that this adaptive algorithm only greedily picks strategies and does not consider costs across adaptation steps. The hope is that with the exponential increase of $S$, earlier decisions that pick bad merge strategies do not hurt the overall cost too much, and later decisions are more accurate due to more informed statistics $sigma$. Additionally, with the hope that the $GG$ estimation converges, we should be less susceptible to perturbations in merge strategy choices.



== Adaptation Framework III: Distribution Shift Readaptation

All previous adaptation frameworks assume that the data distribution stays constant across the table. However, in the real world, it is not hard to imagine situations where the frequency of new group keys changes. As a hypothetical example, say a photo database library workflow requires bucketing dates of photos such that days at different distance into history gets different resolution, such as having photos this year be groupped by days while photos more a year ago be groupped by months, then we can have a higher frequency of group keys as we scan more recent photos.

To overcome this possibility, we can modify _Adaptation Framework II_ to allow include a detection mechanism for when the number of group keys in the recently-seen data changes dramatically. If it does, the algorithm should consider resetting $S$ and relearning about $sigma$. We also set an upper bound $u$ on the value of $S$ to disallow endless exponential growth that could delay our discovery of a distribution change.


#pseudocode-list[
  + $B <-$ batch size (i.e. how many tuples per task)
  + step size $S <- B$
  + $p <-$ thread count
  + $pp <- min(4, p)$, initial number of active threads
  + $aa <- $ _central-merge_, initial algorithm choice
  + $sigma <-$ data structure for computing $GG$, the estimated number of groups
  + *while* there is more input rows to porcess:
    + $sigma' <-$ data structure for computing $GG'$, the estimated number of groups seen recently
    + *for* input row $r$ the next chunk of $S$ input tuples:
      + Perform aggregation from $r$ using $aa$ with $pp$ threads
      + *if* some sampling condition met:
        + $sigma <- fUpdateStats(sigma, r)$ 
        + $sigma' <- fUpdateStats(sigma', r)$ 
    + $S <- min(2 S, u)$
    + $GG <- fEstmNumGroups(sigma)$
    + $GG' <- fEstmNumGroups(sigma')$
    + *if* $GG'$ is determined to differ significantly from $GG$:
      + $S <- B$
      + $sigma <- sigma'$
      + $GG <- GG'$
    + $pp, aa <- fEstmBestStrat(pp, p, aa, GG, S)$
    + Perfom any necessary work to switch to strategy $aa$ (e.g. resizing lock-free hash table)
  + Combine results using an algorithm appropriate for the set of all strategies used during execution
]


== Adaptive Algorithm 4: Implementing Adaptation Framework III

_Adaptive Algorithm 4_ is an updated version of _Adaptive Algorithm 3_ that implements the changes suggested in _Implementing Adaptation Framework III_ to readapt to changed distribution. We set the upper bound on $S$ to be 128 times the batch size.

= Results <Results>

In @resBaeslines, we present extensive benchmarking results from non-adaptive strategies and reference systems. We also discuss how these results reveal adaptation opportunities. In @resAdaptive, we present results from our adaptation algorithms. We find that _Adaptation Framework I_ generally leads to predictable results can often successfully pick strategies that are close to optimal. We find _Adaptation Framework II_ to be more adventurous and unpredictable, but nonetheless succeeding at optimizing towards the performance of lock-free hash table when noticing a high number of group keys. In @resAdversarial, we run evaluation on an adversarially-constructed input table and found some evidence that _Adaptive Algorithm 4_'s readaptation is helpful.

In this section, we may use the following to refer to strategies, algorithms, or frameworks by their shortened names:

- _Lock-Free_  = _Lock-Free Hash Table_
- _Adaptive 1_ = _Adaptive Algorithm 1_
- _Adaptive 2_ = _Adaptive Algorithm 2_
- _Adaptive 3_ = _Adaptive Algorithm 3_
- _Adaptive 4_ = _Adaptive Algorithm 4_
- _Framework I_ = _Adaptive Framework I_
- _Framework II_ = _Adaptive Framework II_
- _Framework III_ = _Adaptive Framework III_

== Metric and Optimization Objective


Our project focuses on parallel group-by aggregation execution. We measure performance using two key metrics: query execution time (i.e., latency) and scaling speedup with increasing core counts. Latency is critical because, regardless of scalability, an approach that achieves an order-of-magnitude reduction in absolute query time provides immediate and significant benefits to end-users. This is especially important in typical analytical workflows in databases, which often involve billions of rows. At the same time, scaling speedup is important to evaluate how well an approach leverages additional computational resources, ensuring efficiency at larger scales. Both metrics together provide a comprehensive view of practical performance.

In our experiments, we observe that among speedup and latency, latency changes in a more interesting way when the input data distribution changes. We report speedup data obtained from evaluating non-adaptive strategies and reference systems in @appSpeedupBaselines. We focus solely on latency in adaptation experiments.

As described in our Milestone Report, we are interested in steady-state performance. That is, when the data is in memory and has previously been accessed, as is often the case in long-running database systems. We achieve this steady-state by repeatedly running the aggregation query a few times before measuring its performance. Performance data is averaged across 5 runs in all experiments.

== Experiment Setup <expSetup>


Our experimental setup is designed to isolate and study parallel group-by aggregation, focusing specifically on distributive aggregation functions: `MIN`, `MAX`, `COUNT`, and `SUM` @grayData2007. We generate synthetic datasets across different input distributions#footnote[This can be thought of as another layer of distribution on top of varying group-to-row ratios. Given a fixed group-to-row ratio that controls the number of groups, these four distribution categories control the relative frequency of occurrences of those groups.]—uniform, normal, exponential, and biuniform#footnote[We implement what we call a "biuniform" distribution by picking from two uniform distributions with equal probability.]—to reflect a range of realistic data skews. A key parameter we control is the group-to-row ratio, as the number of groups relative to the number of rows critically affects aggregation difficulty.

Initially, we considered using the TPC-H benchmark suite, a standardized set of database queries commonly used to evaluate analytical performance #cite(<tpch>). However, TPC-H queries involve many operations beyond aggregation, such as complex filtering and multi-table joins, which are outside the focused scope of our study. To better isolate aggregation behavior, we opted for synthetic data.

Given the scale of our experiments—datasets with up to hundreds of millions of rows—data generation speed becomes a practical concern. For instance, generating an 800-million-row dataset with a uniform distribution on a single thread would take over 24 hours. To accelerate this, we parallelize data generation across four logical threads (IDs 0, 1, 2, 3), with each thread initialized using a random seed of `42 + thread_id` to ensure reproducibility while maintaining speed.

In our task, the notion of task size can mostly be captured by two aspects: the number of input rows and the number of groups, with the latter determining the number of output rows. Alternatively, one can specify the input size by the number of input rows and the group-to-row ratio, which might be more convenient for studying the effect of the output size given a fixed input size.

The full experiment space we could consider is very large:

$
ub({1,2,4,8,16,32,64,128}, "number of threads") times ub({"uniform", "normal", "binormal", "exponential"}, "input data distrubtion") \
times ub({8"M", 80"M", 800"M"}, "input row count") times ub({1/4, ..., 1/4000}, "group-to-row ratio") times ub({"radix merge", "adaptive algorithm 1", ...}, "large set of algorithms")
$

Because the full search space of parameters would take days to exhaust#footnote[Running only a subset of half the algorithms and without 800M row scale would already take 12 hours on Pittsburgh Supercomputing Center nodes.], we restrict our optimization experiments to a practical subset. Specifically, we focus primarily on 80M-row inputs when testing adaptive strategies and rarely perform benchmarks on 800M-row inputs. When comparing latencies, we focus on thread counts in ${4,8,16,32,64}$, which are the most interesting. We also narrow our focus on the simple uniform distribution, as we saw limited interesting trend testing different distributions when benchmarking non-adaptive strategies. 

All experiments are performed on PSC Bridge 2 Regular Memory machines.

== Benchmarks of Non-Adaptive Strategies And Reference Systems <resBaeslines>


We evaluate the execution time of different non-adaptive GROUP BY aggregation strategies under two input scales: 8 million rows and 80 million rows. For each input scale, we test four output scales corresponding to group-to-row ratios in ${1/4, 1/40, 1/400, 1/4000}$. All experiments were conducted on a shared-memory CPU system (PSC machines, to be specific), using OpenMP-based parallel implementations. Measurements were taken after discarding warm-up runs to capture steady-state performance, following the evaluation methodology outlined previously.

For the 8 million row dataset, we varied the number of distinct group keys across 2K, 20K, 200K, and 2M. The resulting latency curves are shown below. Each curve plots the execution time as a function of the number of threads, illustrating how different aggregation strategies respond to varying group cardinalities.

#figure( 
  image("figures/latency_curves_combined_8M.svg", width: 95%), 
  caption: [Latency curves#footnote[See @lockFreeMethodDesc for a caveat on the Lock-free hash table evaluation.] for 8 million input rows under varying the output size i.e. numbers of group keys (2K, 20K, 200K, 2M). Dashed line indicates a reference system.]
) <latency_curves_combined_8M>

For the 80 million row dataset, we varied the number of distinct group keys across 20K, 200K, 2M, and 20M. The latency curves corresponding to these configurations are presented below.

#figure( 
  image("figures/latency_curves_combined_80M.svg", width: 95%), 
  caption: [Latency curves#footnote[See @lockFreeMethodDesc for a caveat on the Lock-free hash table evaluation.] for 80 million input rows under varying the output size i.e. numbers of group keys (20K, 200K, 2M, 20M). Dashed line indicates a reference system.]
) <latency_curves_combined_80M>

We make an important observation from these results: no fixed aggregation strategy achieves optimal performance across all data distributions. For instance, the _Tree Merge_ strategy performs exceptionally well under the 80 million rows and 20K groups configuration, delivering some of the lowest latencies among all methods tested. Strategies with a similar aggregation phase, such as _Central Merge_, perform similarly well. This is very likely attributed to the low cost of the merge phase. However, under the more challenging 20M groups setting, the same strategy degrades substantially, becoming one of the worst-performing approaches, second only to the _Central Merge_. These observations highlight the critical need for adaptive execution, where the choice of aggregation strategy dynamically responds to observed workload characteristics rather than relying on static design decisions.

_Radix Merge_, on the other hand, shines when the output size is large. This can be attributed to its highly parallelizable merge phase, where partitions can be merged independently. However, the extra work needed to prepare these partitions is sometimes not negligible.

Another observation is that _Lock-Free_ often performs poorly on small output sizes but performs surprisingly well on large output sizes. We speculate that this is due to higher contention for the same set of keys when the total number of keys is small.

These differences in behavior for varying group-to-row ratios suggest that, if we keep the input size constant and vary the output size, there will be cross-over points between different strategies' latency, which is what our adaptive parallel aggregation algorithms designs will design and try to adapt to. In @resAdaptive, we make finer-grain output size variations and more clearly see these cross-over behaviors.






To analyze what limits speedup, we decompose the aggregation process into two phases: Phase 1 (aggregation), where each thread independently aggregates input rows, and Phase 2 (merge), where the intermediate thread-local results are consolidated into the final output.

#figure( image("figures/latency-per-phase.svg", width: 85%), caption: [Per-phase latency breakdown. Phase 1 (parallel aggregation) scales well with increasing threads, while Phase 2 (merge) often dominates total latency due to dependency overheads.]) <latency-per-phase>

The experimental results show that Phase 2 (merge) incurs significant overhead, particularly for centralized merging strategies, where a single-threaded bottleneck emerges as the number of threads increases. Meanwhile, strategies such as tree-based merging and radix partitioned merging demonstrate flatter Phase 2 latency curves, benefiting from reduced synchronization and localized merging, though they still suffer some coordination costs.


While speedup is important, we observe from experimenting with static strategies a high disparity of latency at low thread counts, likely due to the overhead of certain algorithms (e.g. doing radix partitioning when it's not expensive to merge across a small number of threads). 

== Performance of Adaptive Algorithms <resAdaptive>

We evaluated the performance of the four adaptive algorithms under varying group-to-row ratios. Our analysis aims to assess whether dynamic adaptation offers advantages over fixed execution strategies and to understand how different adaptation frameworks behave under diverse conditions. Figure @adaptFig4 and @adaptFig64 compare their performance to other systems for 4 cores and 64 cores, respectively. Full results are available in @allAdaptiveFigs. 

#figure(
  grid(
    columns: 2, gutter: 0pt,
    image("curve3-alg123/latency_vs_output_size_4_cores.svg", width: 110%),
    image("curve3-alg123/latency_vs_output_size_4_cores_zoomed.svg", width: 110%),
  ),
  caption: [Comparison of the latency of adaptive algorithms and other systems at two zoom levels, ran on PSC machines and using 4 cores]
) <adaptFig4>

#figure(
  grid(
    columns: 2, gutter: 0pt,
    image("curve3-alg123/latency_vs_output_size_64_cores.svg", width: 110%),
    image("curve3-alg123/latency_vs_output_size_64_cores_zoomed.svg", width: 110%),
  ),
  caption: [Comparison of the latency of adaptive algorithms and other systems at two zoom levels, ran on PSC machines and using 64 cores]
) <adaptFig64>

The experimental results demonstrate that adaptive execution can indeed improve performance compared to any single static strategy. 
With 4 cores, _Adaptive 1_ was effective at staying close to the minimum value of the three possible merge schemes, as evidenced at both zoom levels, while _Adaptive 2_ did not choose the right strategy for some of the high group-to-row ratios. With 64 cores, both _Adaptive 1_ and _Adaptive 2_ picked good strategies at low and high group-to-row ratios. Interestingly, at 64 cores, the _Tree Merge_ and _Radix Merge_ latency curves cross twice, and both adaptive strategies failed to identify the better strategy between the crossovers. 


On the other hand, _Adaptive 3_ and _Adaptive 4_ tend to struggle at low group-to-row ratios, often having higher latency than all the non-adaptive strategies. At high core counts, however, it was able to more accurately estimate the number of groups and consider the option of switching to _Lock-Free_, significantly reducing the latency at high group-to-core ratios. In some cases, the performance at high group-to-row ratio becomes close to the reference line shown in the graphs, which indicates the performance attainable by _Lock-Free_ if we learn from an oracle about how large the hash table should be.

It appears that different adaptation frameworks exhibit trade-offs. _Framework I_, which rely on a fixed-size sampling prefix and a single early decision, performs well when the input exhibits low to moderate group cardinalities. Their light estimation overhead enables quick decision making with little extra cost. In these cases, the simplicity of the decision-making process leads to efficient execution without significant additional cost. Additionally, _Framework I_ is more predictable and interpretable, which can be a desirable property when reliability is important.

_Framework II_, on the other hand, is more adventurous and less predictable. In its multi-step decision-making process, it is difficult to reason about how decisions made across time steps interact to impact performance. Nevertheless, its longer-term data gathering allows it to make more informed decisions and, in some cases, commit to the more efficient _Lock-Free_.











== Performance on Adversarial Distribution <resAdversarial>

_Adaptation Framework II_ makes strong assumptions about the distribution of upcoming rows, namely that they will look similar to previously seen data. If this is not the case, we hypothesize that our performance will worsen if the distribution of group keys shifts dramatically. To test this, we construct an extreme-case, adversarial distribution: an 80M-row input with four equal-sized segments of varying distribution:

- Segment 1: 20M rows containing 20M group keys in random order
- Segment 2: 20M rows with keys randomly sampled from a pool of 20 keys
- Segment 3: 20M rows with keys randomly sampled from a pool of 2M keys
- Segment 4: 20M rows with keys randomly sampled from a pool of 20 keys

#figure(
  image("adversarial-exp/adversarial-exp-res.svg"),
  caption: [Results on the adversarially constructed input dataset]
) <adversairlFig>

@adversairlFig shows the latency of different strategies on the adversarially constructed dataset. Under a uniform distribution, 80M input rows, and 20M group keys (See @resAdaptive), we can expect _Adaptive 3_ and _Adaptive 4_ to switch to using _Lock-Free_ and achieve lower latency. Here, they do the same when processing segment 1. However, what follows in the segment 2 and 4 would slow down _Lock-Free_ as threads access the same small set of locations in the hash table. While _Adaptive 3_ and _Adaptive 4_ were comparable in earlier experiments, _Adaptive 4_ performs better here thanks to it detecting a sudden drop in the number of group keys in recent input rows and switching out of _Lock-Free_. This readaptation, although still more expensive than _Radix Merge_ in this case, allowed _Adaptive 4_ to perform better than _Lock-Free Hash Table_, which it deemed favorable earlier (at least on high thread counts).

Given that there is no way of telling apart favorable vs. unfavorable distributions beyond segment 1, _Adaptive 4_'s ability to reconsider its strategy can be a very useful property to have.







== Review of Target Machine Choice


Our target machine choice was sound. Optimizing parallel grouped aggregation for CPU was already pretty difficult with a large design space to explore. Additionally, CPU programming gave us enough flexibility to do adaptation.

#bibliography("references.bib", title: "References", full: true)


#let appendix(body) = {
  set heading(numbering: "A.1.", supplement: [Appendix])
  counter(heading).update(0)
  body
}

// #pagebreak()

#show: appendix

= Memory Bandwidth Consideration <memConsider>

Reading rows quickly may be memory-bound. We estimate the theoretical limit on the latency we can achieve if we just scan the input rows.

Each row is 16 bytes (two 64-bit integers), and so the estimated size of the input tables is as follows:

- 8M: 0.128 GB
- 80M: 1.28 GB
- 800M: 12.8 GB

We did not find information on the type of memory channel on PSC machines other than that it has 8 channels. If assuming a 25 GB/sec CPU memory bandwidth per channel based on lecture, we should expect the 80M data to take 52 ms on a single channel or 6 ms on all 8 channels. This does not account for memory access to the hash table and is probably an underestimate. In practice, we see that the table aggregation phase (i.e. before merging partial results) of many of our algorithms and DuckDB both take more than 200 ms (e.g. 200.2 ms with _Central Merge_ and XXHash). The 200 ms result would only be memory-bound if the memory bandwidth were closer to 6.4 GB/sec, so we conclude that we should focus on other optimizations.

= Cache consideration <cacheConsider>

The hash table size, containing at least a key (8 byte) and four values per key (32 byte) will be at least:

- 2K: 0.08 MB
- 20K: 0.8 MB
- 200K: 8 MB
- 2M: 80 MB
- 20M: 800 MB

The number of partial aggregation tables may grow as the number of threads, and we expect this to degrade performance due to the hash tables not being able to fit in PSC's 256MB L3. 



= Speedup of Non-Adaptive Strategies and Reference Systems <appSpeedupBaselines>

== 8M Input Size, Variable Output Size, Variable Distribution 

#image("speedups/8M-agg-speedup.svg", width: 90%)

== 80M Input Size, Variable Output Size, Variable Distribution
#image("speedups/80M-agg-speedup.svg", width: 90%)


= All Adaptive Algorithm Results <allAdaptiveFigs>

== Adaptive Algorithm Results: 4 Cores

#image("curve3-alg123/latency_vs_output_size_4_cores.svg", width: 80%)
#image("curve3-alg123/latency_vs_output_size_4_cores_zoomed.svg", width: 80%)

== Adaptive Algorithm Results: 8 Cores
#image("curve3-alg123/latency_vs_output_size_8_cores.svg", width: 80%)
#image("curve3-alg123/latency_vs_output_size_8_cores_zoomed.svg", width: 80%)

== Adaptive Algorithm Results: 16 Cores
#image("curve3-alg123/latency_vs_output_size_16_cores.svg", width: 80%)
#image("curve3-alg123/latency_vs_output_size_16_cores_zoomed.svg", width: 80%)

== Adaptive Algorithm Results: 32 Cores
#image("curve3-alg123/latency_vs_output_size_32_cores.svg", width: 80%)
#image("curve3-alg123/latency_vs_output_size_32_cores_zoomed.svg", width: 80%)

== Adaptive Algorithm Results: 64 Cores
#image("curve3-alg123/latency_vs_output_size_64_cores.svg", width: 80%)
#image("curve3-alg123/latency_vs_output_size_64_cores_zoomed.svg", width: 80%)



