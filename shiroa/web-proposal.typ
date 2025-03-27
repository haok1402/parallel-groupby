#import "/book.typ": book-page

#show: book-page.with(title: "Proposal")
#set heading(numbering: "1.1.")
#set text(font: "Linux Libertine")

#show table: set par(justify: false)
#set table(inset: (x: 0pt, y: 0.1in))
#show table: set table(
  stroke: (x, y) => (
    x: none,
    bottom: 0.8pt+black,
    top: if y == 0 {
      0.8pt+black
    } else if y==1 {
      0.4pt+black
    } else { 
      // 0pt 
      0.2pt+gray
    },
  )
)
#set table.hline(stroke: 0.4pt+black)
#set table.vline(stroke: 0.4pt)

#include("proposal.typ")