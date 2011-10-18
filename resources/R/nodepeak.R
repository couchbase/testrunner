#
# Make a pdf out for node peak performance tests
#
# To make a pdf from one of those uploads:
#
#    Rscript --vanilla 

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))
uploadName <- args[1]
#uploadName = "3m-moxi-oct-03"
cat(paste("upload name : ",uploadName),sep="\n")
pdf(paste(uploadName,sep="",".pdf"))


xx <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/mbstats", sep=''))$rows
bb <- plyr::ldply(xx, unlist)
names(bb) <- c('id','build','ram','os','rowid','mem_used','flusher_todo')
b <- transform(bb,rowid=as.numeric(rowid), mem_used=as.numeric(mem_used),ram=as.numeric(ram), flusher_todo=as.numeric(flusher_todo))
p <- ggplot(b,aes(rowid,mem_used,fill=build)) + labs(x="",y="")
p <- p + geom_line(aes(rowid, mem_used,color=build))
print(p)
p <- ggplot(b,aes(rowid,flusher_todo,fill=build)) + labs(x="",y="")
p <- p + geom_line(aes(rowid, flusher_todo,color=build))
print(p)

dev.off()
