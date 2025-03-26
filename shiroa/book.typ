#import "@preview/shiroa:0.2.0": *
#show: book

#book-meta(
  title: "",
  summary: [
    = Parallel GROUP BY
    #prefix-chapter("web-proposal.typ")[Proposal]
    #prefix-chapter("web-milestone.typ")[Milestone]
    #prefix-chapter("web-report.typ")[Report]
    = #text(weight: 400)[Static site generated using https://myriad-dreamin.github.io/shiroa/]
  ]
)

// re-export page template
#import "/templates/page.typ": project
#let book-page = project
