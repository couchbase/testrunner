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
	#tname = "NPP-01-1k.1"
	print(tname)
	cat(paste("Test Case : ", tname),sep="\n")
	pdf(paste(tname,sep="",".pdf"))
	data_frame <- data.frame()
	data_frame <- data.frame(t(rep(NA, 25)))
 	names(data_frame)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'timestamp','ops', 'disk_write_queue', 'memory_used', 'resident_ratio', 'drain_rate', 'active_disk_queue','disk_reads','unique_col')	

	# Does not import the null values if present in json 
	stats <- fromJSON(file=paste("http://ec2-50-16-117-7.compute-1.amazonaws.com:5984/pythonperformance","/_design/ns_server/_view/ops", sep=''))$rows
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
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(timestamp, ops, color=build))
	
	p <- p + opts(title=paste("Operations Per Sec"))
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
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
    popViewport()

	p <- ggplot(temp_data_frame, aes(timestamp, disk_write_queue, fill=build, label=ops)) + labs(x="----time (sec)--->", y="dwq")
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(timestamp, disk_write_queue, color=build))
	
	p <- p + opts(title=paste("Disk Write Queue"))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
    popViewport()

	p <- ggplot(temp_data_frame, aes(timestamp, drain_rate, fill=build, label=ops)) + labs(x="----time (sec)--->", y="drain_rate")
	#p  <-   p + stat_smooth(se = FALSE)
	p <- p + geom_line(aes(timestamp, drain_rate, color=build))
	
	p <- p + opts(title=paste("Drain Rate"))
    p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	print(values)
	print(p)
	pushViewport(viewport())
	y <- 0.75 
	for (row in 1:7){
		grid.text(values[row],gp=gpar(col="black"), x=0.73, y=y, check.overlap=TRUE, just="left")
		y <- y + 0.026	
	}
    popViewport()
	
	
dev.off()	
	
	
}
