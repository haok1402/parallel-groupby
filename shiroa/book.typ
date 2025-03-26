
#import "@preview/shiroa:0.2.0": *

#show: book

#book-meta(
  title: "Parallel GROUP BY",
  summary: [
    #prefix-chapter("sample-page.typ")[Hello, typst]
    #chapter("test.typ", section: "1.1")[Proposal]
  ]
)



// re-export page template
#import "/templates/page.typ": project
#let book-page = project
