#import "/book.typ": book-page

#show: book-page.with(title: "Proposal")

#text(size: 32pt)[TITLE]

#let authors(
  authors: ()
) = {
  grid(
    columns: (1fr,) * 2,
    row-gutter: 24pt,
    ..authors.map(author => [
      #author.name \
      #author.affiliation \
      #link("mailto:" + author.email)
    ]),
  )
}

#authors(authors: (
  (
    name: "Theresa Tungsten",
    affiliation: "Artos Institute",
    email: "tung@artos.edu",
  ),
  (
    name: "Eugene Deklan",
    affiliation: "Honduras State",
    email: "e.deklan@hstate.hn",
  ),
))

= Summary

We plan to parallelize SQL aggregation queries with `GROUP BY`, specifically focusing on dynamically optimizing the work distribution based on column data characteristics. Our goal is to develop an adaptive parallelization strategy for aggregation queries that reduces cache evictions, optimizes synchronization and communication, and adapts to varying data distributions. Initially, we will start with shared-memory multi-core systems and explore scaling to multi-node systems later.

= Background

SQL aggregation queries with `GROUP BY` are common operations in relational databases. In simple terms, these queries aggregate the value of a specific column based on a grouping condition, such as `SELECT COUNT(column1) FROM table GROUP BY column2`. However, performance challenges arise when the distribution of values in the grouping column (i.e., `column2`) is highly skewed or unknown. This makes it difficult to evenly partition the work and balance computation across threads.

= Test


3. *PSC Machine*
  - *CPU*: AMD EPYC 7742 64-Core Processor (128 cores, 1 thread per core, 128 cores total)
  - *Architecture*: x86_64
  - *Total CPU Threads*: 128
  - *Caches*:
    - L1d: 32 KiB (128 instances)
    - L1i: 32 KiB (128 instances)
    - L2: 512 KiB (128 instances)
    - L3: 16 MiB (2 instances)

= Schedule

#table(columns: (1fr, 1fr),
  [*Date*], [*Leon's will do...*],
  [26 March 2025], [Submit proposal],
  [30 March 2025], [idk],
)
