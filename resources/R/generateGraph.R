# Print out Graphs for each test
# i.e. 
# Drain Rate, Latency and OPS

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))
for(arg in args) {
	tname <- arg
	#tname = "NPP-01-1k"
	print(tname)
	cat(paste("Test Case : ", tname),sep="\n")
	pdf(paste(tname,sep="",".pdf"))
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 13)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'drainRate', 'unique_col')	


	# Drain Rate
	# Does not import the null values if present in json 
	membasestats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/membasestats", sep=''))$rows
	bb <- plyr::ldply(membasestats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'drainRate')	
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), drainRate=as.numeric(drainRate), unique_col=paste(substring(doc_id, 28, 32), '-', build))
 	bb <- bb[bb$test==tname,]

 	(temp_data_frame <- data_frame[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time,]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, drainRate, fill=build)) + labs(x="samples", y="ms")
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, drainRate, color=build))
	p <- p + labs(y='ms', x="samples")
	#labs <- cbind('test_name',temp_data_frame$test_name[0])
	#labs <- cbind('test_time',temp_data_frame$test_time[0])
	#labs <- cbind('items',temp_data_frame$max_items[0])
	#labs <- cbind('value-size',temp_data_frame$min_value_size[0])
	#g <- tableGrob(labs)
	#grid.arrange(p, g, nrow=4, widths=c(5,5))
	p <- p + opts(title=paste("Disk Write Queue"))
	print(p)
	
	# Latency
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 17)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'startTime','endTime','totalGets','totalSets',	'unique_col', 'avgLatency')	

	opsstats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/opsstats", sep=''))$rows
	bb <- plyr::ldply(opsstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'startTime', 'endTime', 'totalGets', 'totalSets')	
	bb <- transform(bb, build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb, build=paste(gsub("membase-","",build)))
	bb <- transform(bb, rowid=as.numeric(rowid),ram=as.numeric(ram), test_time=as.numeric(test_time), startTime=as.numeric(startTime), endTime=as.numeric(endTime), unique_col=paste(substring(doc_id,	28,32),'-',build))
	bb <- transform(bb, avgLatency = (endTime-startTime))
	bb <- bb[bb$test == tname,]
	# Drain Rate tests have no OPS/Latency
	if(nrow(bb) != 0){
		print(bb)
		(temp_data_frame <- data_frame[FALSE, ])
		print(bb$build)
		print(bb)
		builds = factor(bb$build)

		for(a_build in levels(builds)) {
			filtered <- bb[bb$build == a_build,]
			print(filtered)
			max_time <- max(filtered$test_time)
			print(max_time)
			graphed <- bb[bb$build == a_build & bb$test_time == max_time,]
			temp_data_frame <- rbind(temp_data_frame,  graphed)
		}
		print(temp_data_frame)
		p <- ggplot(temp_data_frame, aes(rowid, avgLatency, fill=build)) + labs(x="samples", y="ms")
		p  <-   p + stat_smooth(se = FALSE)
		#p <- p + geom_line(aes(rowid, avgLatency, color=build))
		p <- p + labs(y='ms', x="samples")
		p <- p + opts(title=paste("Avg. Latency"))
		print(p)
	}
	# OPS
	bb <- plyr::ldply(opsstats,unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'startTime', 'endTime', 'totalGets', 'totalSets')	
	bb <- transform(bb,build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb,build=paste(gsub("membase-","",build)))

	bb <- transform(bb, rowid=as.numeric(rowid),ram=as.numeric(ram), test_time=as.numeric(test_time),opsPerSecond=(as.numeric(totalGets)+as.numeric(totalSets))/(as.numeric(endTime) - 		as.numeric(startTime)),unique_col=paste(substring(doc_id,28,32),'-',build))
	bb <- bb[bb$test == tname,]
	if(nrow(bb) != 0){
		(temp_data_frame <- data_frame[FALSE, ])
		builds = factor(bb$build)
		for(a_build in levels(builds)) {
			filtered <- bb[bb$build == a_build,]
			max_time <- max(filtered$test_time)
			print(max_time)
			graphed <- bb[bb$build == a_build & bb$test_time == max_time,]
			temp_data_frame <- rbind(temp_data_frame,  graphed)
		}
		p <- ggplot(temp_data_frame,aes(rowid, opsPerSecond, fill=build)) + labs(x="samples", y="OPS")
		p  <-   p + stat_smooth(se = FALSE)
		#p <- p + geom_line(aes(rowid, opsPerSecond, color=build))
		p <- p + labs(y='OPS', x="samples")
		p <- p + opts(title=paste("OPS"))
		print(p)
	}
	# System Stats (Memory)
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 15)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'process', 'rowid', 'time_sample', 'rss', 'unique_col')	

	systemstats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/rviews/_view/systemstats", sep=''))$rows
	bb <- plyr::ldply(systemstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'process', 'rowid', 'time_sample', 'rss')		
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), time_sample=as.numeric(time_sample), rss=as.numeric(rss), unique_col=paste(substring(doc_id, 28, 32), '-', build))
	bb <- bb[bb$test==tname,]
	(temp_data_frame <- data_frame[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time,]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, rss, fill=build)) + labs(x="samples", y="Memory")
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, rss, color=build))
	p <- p + labs(y='Memory (in MB)', x="samples")
	#labs <- cbind('test_name',temp_data_frame$test_name[0])
	#labs <- cbind('test_time',temp_data_frame$test_time[0])
	#labs <- cbind('items',temp_data_frame$max_items[0])
	#labs <- cbind('value-size',temp_data_frame$min_value_size[0])
	#g <- tableGrob(labs)
	#grid.arrange(p, g, nrow=4, widths=c(5,5))
	p <- p + opts(title=paste("beam.smp"))
	print(p)
	
dev.off()	
	
	
}
