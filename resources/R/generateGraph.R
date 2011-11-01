# Print out Graphs for each test
# i.e. 
# Drain Rate, Latency OPS and SYstem stats

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
	data_frame <- data.frame(t(rep(NA, 19)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'drainRate', 'unique_col')	

	# Drain Rate
	# Does not import the null values if present in json 
	membasestats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/pythonperformance","/_design/rviews/_view/membasestats", sep=''))$rows
	bb <- plyr::ldply(membasestats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'drainRate')	
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb,rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), drainRate=as.numeric(drainRate), unique_col=paste(substring(doc_id, 28, 32), '-', build))
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
	p <- ggplot(temp_data_frame, aes(rowid, drainRate, fill=build, label=drainRate)) + labs(x="----time (every 10 sec)--->", y="ms")
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, drainRate, color=build))
	
	p <- p + opts(title=paste("Disk Write Queue"))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	testName <- unlist(strsplit(temp_data_frame$test_name[1], "\\."))[4]
	print(testName)
	values <- c(paste('test:', testName), paste('items:', temp_data_frame$total_items[1]), paste('value_size:',temp_data_frame$min_value_size[1]), paste('total_gets:', temp_data_frame$total_gets[1]), paste('total_sets:', temp_data_frame$total_sets[1]), paste('total_creates:', temp_data_frame$total_creates[1]), paste('run_time:', temp_data_frame$run_time[1]) )
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
    popViewport()

	
	# Latency
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 17)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'startTime','endTime','totalGets','totalSets',	'unique_col', 'avgLatency')	

	opsstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/pythonperformance","/_design/rviews/_view/opsstats", sep=''))$rows
	bb <- plyr::ldply(opsstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'rowid', 'startTime', 'endTime', 'totalGets', 'totalSets')	
	bb <- transform(bb, build=paste(gsub("couchbase-","",build)))
	bb <- transform(bb, build=paste(gsub("membase-","",build)))
	bb <- transform(bb, rowid=as.numeric(rowid),ram=as.numeric(ram), test_time=as.numeric(test_time), startTime=as.numeric(startTime), endTime=as.numeric(endTime), unique_col=paste(substring(doc_id,	28,32),'-',build))
	bb <- transform(bb, avgLatency = (endTime-startTime))
	bb <- bb[bb$test == tname,]
	# Drain Rate tests have no OPS/Latency
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
		p <- ggplot(temp_data_frame, aes(rowid, avgLatency, fill=build, color=build, label=avgLatency)) + labs(x="samples", y="ms")
		p  <-   p + stat_smooth(se = FALSE)
		#p <- p + geom_line(aes(rowid, avgLatency, color=build))
		p <- p + labs(y='ms', x="----time --->")
		p <- p + opts(title=paste("Avg. Latency"))
	    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    	p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
		print(p)
		pushViewport(viewport())
		y <- 0.75 
		for (row in 1:7){
			grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
			y <- y + 0.026	
		}
	    popViewport()
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
		p <- ggplot(temp_data_frame,aes(rowid, opsPerSecond, fill=build, color=build, label=opsPerSecond)) + labs(x="samples", y="OPS")
		p  <-   p + stat_smooth(se = FALSE)
		#p <- p + geom_line(aes(rowid, opsPerSecond, color=build))
		p <- p + labs(y='OPS', x="----time --->")
		p <- p + opts(title=paste("OPS"))
	    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    	p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
		print(p)
		pushViewport(viewport())
		y <- 0.75 
		for (row in 1:7){
			grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
			y <- y + 0.026	
		}
	    popViewport()
	}
	# System Stats (Memory for Beam.smp)
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 21)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'time_sample', 'rss', 'process','unique_col')	

	systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/pythonperformance","/_design/rviews/_view/systemstats", sep=''))$rows
	bb <- plyr::ldply(systemstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time','rowid', 'time_sample', 'rss', 'process','cpu_time')		
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
		graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == 'beam.smp',]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, rss, fill=build, label=rss))
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, rss, color=build))
	p <- p + labs(y='Memory (in MB)', x="----time (every 10 secs) --->")
	p <- p + opts(title=paste("beam.smp"))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	popViewport()
	
	# System Stats (Memory for Memcached)
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 21)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'time_sample', 'rss', 'process','unique_col')	

	systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/pythonperformance","/_design/rviews/_view/systemstats", sep=''))$rows
	bb <- plyr::ldply(systemstats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time','rowid', 'time_sample', 'rss', 'process','cpu_time')
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), time_sample=as.numeric(time_sample), rss=as.numeric(rss), unique_col=paste(substring(doc_id, 28, 32), '-', build),cpu_time=as.numeric(cpu_time))
	bb <- bb[bb$test==tname,]
	(temp_data_frame <- data_frame[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == 'memcached',]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, rss, fill=build, label=rss))
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, rss, color=build))
	p <- p + labs(y='Memory (in MB)', x="----time (every 10 secs) --->")
	p <- p + opts(title=paste("Memcached"))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	print(p)
	
	
	bb <- bb[bb$test==tname,]
	(temp_data_frame <- data_frame[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == 'memcached',]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, cpu_time, fill=build, label=cpu_time))
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(rowid, rss, color=build))
	p <- p + labs(y='Memory (in MB)', x="----time (every 10 secs) --->")
	p <- p + opts(title=paste("Memcached"))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	
	
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	popViewport()

	
dev.off()	
	
	
}
