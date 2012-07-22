library(RSQLite)
library(igraph)

vertexIndex <- function(x, graph) {
  match(x, V(graph)$name)-1
}

con <- dbConnect(dbDriver("SQLite"),dbname='../enron.db')

el <- dbGetQuery(con,"select senderId, recipientId, count(messages.id) as count from messages inner join recipients where messages.id==recipients.id group by senderId, recipientId")

reportsTo <- dbGetQuery(con, "select subId, supId from hierarchy where distance==1")

orgGraph <- graph.edgelist(sapply(reportsTo, as.character))
orgGraph <- decompose.graph(orgGraph)[[1]]

validIds = intersect(unique(c(unlist(reportsTo))),V(orgGraph)$name)

el <- el[el[,1] %in% validIds & el[,2] %in% validIds,]

emailGraph <- graph.edgelist(na.omit(apply(el[,c("senderId", "recipientId")],
                                   MARGIN=2,
                                   function(x){
                                     vertexIndex(as.character(x),
                                                 orgGraph)
                                   }))
                             )

E(emailGraph)$weight <- el$count

emailGraph <- simplify(emailGraph, remove.loops=TRUE)


# find the root of the org graph
vid <- 0
while(length(n <- neighbors(orgGraph,vid,mode='out'))>0){
    print(n)
    vid <- n[1]
}

emailGraph$layout <- layout.reingold.tilford(orgGraph,
                                             params = list(root=vid,
                                               circular=FALSE))

max_email_count = max(log(E(emailGraph)$weight))

plot(emailGraph,
     vertex.label='',
     vertex.label.cex = .6,
     vertex.label= '',
     vertex.size=(betweenness(emailGraph) + 1)^1/100,
     edge.color=rainbow(1, alpha=log(E(emailGraph)$weight)/(max_email_count*3)),
     edge.arrow.size=.3,
     edge.arrow.width=.3,
     edge.curved=.1
     )

