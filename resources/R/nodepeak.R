#
# Make a pdf out for node peak performance tests
#
# To make a pdf from one of those uploads:
#  usage:
#    Rscript --vanilla nodepeak.R NPP-01-1k NPP-03-1k

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))
mbstats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/mbstats", sep=''))$rows
procstats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/procstats", sep=''))$rows
	
for(arg in args) {
	tname <- arg
	#tname = "NPP-01-1k"
	cat(paste("Test Case : ", tname),sep="\n")
	pdf(paste(tname,sep="",".pdf"))
	bb <- plyr::ldply(mbstats, unlist)
	names(bb) <- c('id','build','ram','os','doc_id','test','rowid','mem_used','flusher_todo')
	bb <- transform(bb,build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb,build=paste(gsub("membase-","",build)))
	b <- transform(bb,rowid=as.numeric(rowid), mem_used=as.numeric(mem_used)/1000000,ram=as.numeric(ram), flusher_todo=as.numeric(flusher_todo),unique_col=paste(substring(doc_id,28,32),'-',build))
	df <- b[b$test==tname,]
	df <- transform(df,rowid=as.numeric(rowid), mem_used=as.numeric(mem_used)/1000000,ram=as.numeric(ram), flusher_todo=as.numeric(flusher_todo),unique_col=paste(substring(doc_id,28,32),'-',build))	
	p <- ggplot(df,aes(rowid,mem_used,fill=unique_col)) + labs(x="",y="")
	p <- p + geom_line(aes(rowid, mem_used,color=unique_col))
	p <- p + labs(y='Memory Usage in MB', x="Build Number")
	p <- p + opts(title=paste("memcached mem_used"))
	print(p)
	df <- b[b$test==tname,]
	df <- transform(df,rowid=as.numeric(rowid), mem_used=as.numeric(mem_used)/1000000,ram=as.numeric(ram), flusher_todo=as.numeric(flusher_todo),unique_col=paste(substring(doc_id,28,32),'-',build))
	p <- ggplot(df,aes(rowid,flusher_todo,fill=unique_col)) + labs(x="",y="")
	p <- p + geom_line(aes(rowid, flusher_todo,color=unique_col))
	p <- p + opts(title=paste("memcached flusher_todo"))
	p <- p + labs(y='Number of Items In the Outgoing Queue', x="Build Number")
	print(p)


	bb <- plyr::ldply(procstats, unlist)
	names(bb) <- c('id','build','ram','os','doc_id','test','process','rowid','time','resident')
	bb <- transform(bb,build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb,build=paste(gsub("membase-","",build)))
	b <- transform(bb,rowid=as.numeric(rowid), resident=as.numeric(resident)/1000000,ram=as.numeric(ram),unique_col=paste(substring(doc_id,28,32),'-',build))	
	b <- b[b$test==tname,]
	processes = factor(bb$process)
	for(pname in levels(processes)) {
		cat(paste(pname,"\n"))
		df <- b[b$process==pname,]
		df <- transform(df,rowid=as.numeric(rowid), resident=as.numeric(resident)/1000000,ram=as.numeric(ram),unique_col=paste(substring(doc_id,28,32),'-',build))		
		p <- ggplot(df,aes(rowid,resident,fill=unique_col)) + labs(x="",y="")
		p <- p + geom_line(aes(rowid, resident,color=unique_col))
		p <- p + labs(y='Resident Memory Size', x="Build Number")
		p <- p + opts(title=paste("process: ",pname," resident memory size ","test :",tname))
		print(p)
	}
	dev.off()

}

