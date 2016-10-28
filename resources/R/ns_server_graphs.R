# Print out Graphs for each test
# i.e. 
# Drain Rate, Latency OPS and SYstem stats

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
library(methods)
args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
pdf(paste('perf_graphs',sep="",".pdf"))

args <- unlist(strsplit(args," "))
for(arg in args) {
	tname <- arg
	#tname = "Experimental"
	print(tname)
	cat(paste("Test Case : ", tname),sep="\n")
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 25)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'timestamp','ops', 'disk_write_queue', 'memory_used', 'resident_ratio', 'drain_rate', 'active_disk_queue','disk_reads','unique_col')	

	# Does not import the null values if present in json 
	cat("generating ops/sec graph")
	stats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/data", sep=''))$rows
	bb <- plyr::ldply(stats, unlist)
	names(bb) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'timestamp','ops', 'disk_write_queue', 'memory_used', 'resident_ratio', 'drain_rate', 'active_disk_queue','disk_reads')	

	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb,test_time=as.numeric(test_time),unique_col=paste(substring(doc_id, 28, 32), '-', build))
	bb <- transform(bb, timestamp=as.numeric(timestamp), ops=as.numeric(ops), disk_write_queue=as.numeric(disk_write_queue), drain_rate=as.numeric(drain_rate))
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
	p <- ggplot(temp_data_frame, aes(timestamp, ops, fill=build, label=ops)) + labs(x="----time (sec)--->", y="OPS")
	p <- p + opts(title=paste("Operations Per Sec", sep=''))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))	
	p  <-  p + stat_smooth(se = TRUE)
#	p  <-  p + geom_smooth(aes(timestamp, ops, color=build), stat="identity") 
#	p <- p + geom_line(aes(timestamp, ops, color=build))
	testName <- unlist(strsplit(temp_data_frame$test_name[1], "\\."))[4]
	print(testName)
	values <- c(paste('test:', testName), paste('items:', temp_data_frame$total_items[1]), paste('value_size:',comma(temp_data_frame$min_value_size[1])), paste('total_gets:', comma(temp_data_frame$total_gets[1])), paste('total_sets:', comma(temp_data_frame$total_sets[1])), paste('total_creates:', comma(temp_data_frame$total_creates[1])))
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
    popViewport()

	cat("generating disk write queue graph")
	p <- ggplot(temp_data_frame, aes(timestamp, disk_write_queue, fill=build, label=ops)) + labs(x="----time (sec)--->", y="dwq")
	#p  <-   p + stat_smooth(se = FALSE)
	#p <- p + geom_line(aes(timestamp, disk_write_queue, color=build))
	p  <-  p + stat_smooth(se = TRUE)
	p <- p + opts(title=paste("Disk Write Queue", sep=''))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")

    popViewport()

	cat("generating disk drain rate graph")
	p <- ggplot(temp_data_frame, aes(timestamp, drain_rate, fill=build, label=ops)) + labs(x="----time (sec)--->", y="drain_rate")
	p <- p + geom_line(aes(timestamp, drain_rate, color=build))
	#p  <-  p + stat_smooth(se = TRUE)
	p <- p + opts(title=paste("Drain Rate", sep=''))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
    popViewport()

    #system stats
    # System Stats (Memory for Beam.smp)
    cat("Generating Memory usage for beam.smp and memcached")
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 21)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'time_sample', 'rss', 'process','unique_col')	

	systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/systemstats", sep=''))$rows
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
	p <- p + labs(y='Memory (in MB)', x="----time (sampling ~ 10 secs)) --->")
	p <- p + opts(title=paste("Memory profile: beam.smp", sep=''))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.71, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
	popViewport()
	
	# System Stats (Memory for Memcached)
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 21)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'rowid', 'time_sample', 'rss', 'process','unique_col')	

	systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/systemstats", sep=''))$rows
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
	p <- p + labs(y='Memory (in MB)', x="----time (sampling ~ 10 secs)) --->")
	p <- p + opts(title=paste("Memory profile: Memcached", sep=''))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	print(p)
    pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
    popViewport()
    
	# Does not import the null values if present in json 
	cat("generating memcached cpu usage graphs")
	systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/systemstats", sep=''))$rows
	bb <- plyr::ldply(systemstats, unlist)
 	names(bb)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time','rowid', 'time_sample', 'rss', 'process','cpu_time')

	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb,test_time=as.numeric(test_time),unique_col=paste(substring(doc_id, 28, 32), '-', build))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), time_sample=as.numeric(time_sample), rss=as.numeric(rss), unique_col=paste(substring(doc_id, 28, 32), '-', build),cpu_time=as.numeric(cpu_time))
 	bb <- bb[bb$test==tname,]

 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == 'memcached',]
#		graphed$cpu_time_diff <- c(NA, diff(graphed$cpu_time))
#		temp_data_frame $cpu_time_diff <- c(NA, diff(temp_data_frame$cpu_time))
		temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	p <- ggplot(temp_data_frame, aes(rowid, cpu_time, fill=build, label= comma(cpu_time)))
	p <- p + geom_line(aes(rowid, cpu_time, color=build))
	p <- p + labs(y='stime+utime', x="----time (sampling ~ 10 secs)) --->")
	p <- p + opts(title=paste("CPU profile: Memcached", sep=''))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
    print(p)
    pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
    popViewport()
    
    #cpu time for beam.smp
    cat("generating beam.smp cpu usage graphs")
	bb <- transform(bb, build=paste(gsub("couchbase-", "", build)))
	bb <- transform(bb, build=paste(gsub("membase-", "", build)))
	bb <- transform(bb,test_time=as.numeric(test_time),unique_col=paste(substring(doc_id, 28, 32), '-', build))
	bb <- transform(bb, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), time_sample=as.numeric(time_sample), rss=as.numeric(rss), unique_col=paste(substring(doc_id, 28, 32), '-', build),cpu_time=as.numeric(cpu_time))
 	bb <- bb[bb$test==tname,]


	data_frame <- data.frame()
 	(temp_data_frame <- data_frame[FALSE, ])
	builds = factor(bb$build)
	for(a_build in levels(builds)) {
		filtered <- bb[bb$build == a_build,]
		max_time <- max(filtered$test_time)
		print(max_time)
		graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == 'beam.smp',]
		temp_data_frame <- rbind(temp_data_frame,  graphed)
		#temp_data_frame[, "cpu_time_diff"] <- c(NA, diff(temp_data_frame$cpu_time))
	}
	p <- ggplot(temp_data_frame, aes(rowid, cpu_time, fill=build, label= comma(cpu_time)))
	p <- p + geom_line(aes(rowid, cpu_time, color=build))
	p <- p + labs(y='stime+utime', x="----time (sampling ~ 10 secs)) --->")
	p <- p + opts(title=paste("CPU profile: beam.smp", sep=''))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
    print(p)    
    
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:6){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
	grid.text(temp_data_frame$test_name[1],gp=gpar(col="black"), x=0.18, y=0.98, check.overlap=TRUE, just="left")
    popViewport()
	
}

dev.off()	