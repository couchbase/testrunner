# Print out Graphs for each test
# i.e. 
# Drain Rate, Latency and OPS

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))
for(arg in args) {
	tname <- arg
	#tname = "NPP-01-1k"
	print(tname)
	cat(paste("Test Case : ", tname),sep="\n")
	pdf(paste(tname,sep="",".pdf"))
	
	# Drain Rate
	membasestats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/membasestats", sep=''))$rows
	bb <- plyr::ldply(membasestats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'rowid', 'drainRate')	
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), drainRate=as.numeric(drainRate), unique_col=paste(substring(doc_id, 28, 32), '-', build))
 	bb <- bb[bb$test==tname,]
	p <- ggplot(bb,aes(rowid, drainRate, fill=build)) + labs(x="samples", y="ms")
	p <- p + geom_line(aes(rowid, drainRate, color=build))
	p <- p + labs(y='ms', x="samples")
	p <- p + opts(title=paste("Drain Rate"))
	print(p)
	
	# Latency
	opsstats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/opsstats", sep=''))$rows
	bb <- plyr::ldply(opsstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'rowid','startTime','endTime','totalGets','totalSets')	
	bb <- transform(bb, build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb, build=paste(gsub("membase-","",build)))
	bb <- transform(bb, rowid=as.numeric(rowid),ram=as.numeric(ram), test_time=as.numeric(test_time), startTime=as.numeric(startTime), endTime=as.numeric(endTime), unique_col=paste(substring(doc_id,	28,32),'-',build))
	bb <- transform(bb, avgLatency = (endTime-startTime))
	bb <- bb[bb$test == tname,]
	p <- ggplot(bb,aes(rowid, avgLatency, fill=build)) + labs(x="samples", y="ms")
	p <- p + geom_line(aes(rowid, avgLatency, color=build))
	p <- p + labs(y='ms', x="samples")
	p <- p + opts(title=paste("Avg. Latency"))
	print(p)
	
	# OPS
	bb <- plyr::ldply(opsstats,unlist)
	names(bb) <- c('id','build','ram','os','doc_id','test','test_time','test_name','rowid','startTime','endTime','totalGets','totalSets')	
	bb <- transform(bb,build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb,build=paste(gsub("membase-","",build)))
	bb <- transform(bb, rowid=as.numeric(rowid),ram=as.numeric(ram), test_time=as.numeric(test_time),opsPerSecond=(as.numeric(totalGets)+as.numeric(totalSets))/(as.numeric(endTime) - 		as.numeric(startTime)),unique_col=paste(substring(doc_id,28,32),'-',build))
	bb <- bb[bb$test == tname,]
	builds = factor(bb$build)
	p <- ggplot(bb, aes(rowid, opsPerSecond, fill=build)) + labs(x="samples", y="OPS")
	p <- p + geom_line(aes(rowid, opsPerSecond, color=build))
	p <- p + labs(y='OPS', x="samples")
	p <- p + opts(title=paste("OPS"))
	print(p)

dev.off()	
	
	
}
