# Print out Graphs for each test
# i.e. 
require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
library(methods)


filterBuildsByTestName <- function(data_frame) {
	builds = levels(factor(df$build))
	temp_data_frame <- df[FALSE, ]
	df$test_time = as.numeric(df$test_time)
	tests = levels(factor(df$test))
	for(a_test in tests) {
		for(a_build in builds) {
			filtered = df[df$build==a_build & df$test == a_test,]
			max_time = max(filtered$test_time)
			filtered = filtered[filtered$test_time == max_time,]
			temp_data_frame <- rbind(temp_data_frame,filtered)	
		}
	}
	temp_data_frame
}

createLatencyData <- function(l) {
	result <- matrix(ncol=13)
	for(lr in l) {
		rowId = 1
		for(one_row in lr[3]) { 
			for(latency_data in t(one_row)) {
				aa <- unlist(latency_data)
				aa <- as.numeric(aa)
				a <- union(aa,unlist(lr[2]))
		        b <- union(a, rowId)				
	      		result <- rbind(result, b)
	    	   	rowId = rowId + 1
	    	   	if(rowId > 100) {
	    	   		break
	    	   	}
			}
		}
		cat("...")	
	}
	result
}

pruneBuildNames <- function(bb,builds) {
	bb <- bb[bb$build %in% builds,]
	bb <- transform(bb,test_time=as.numeric(test_time))
	bb
}


createAllProcessesUsageDataFrame <- function(bb) {
 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$build)
	tests = factor(bb$test)
	processes = factor(bb$process)
	for(a_process in levels(processes)) {
		for(a_test in levels(tests)) {
			for(a_build in levels(builds)) {
				filtered <- bb[bb$build == a_build & bb$test == a_test,]
				max_time <- max(filtered$test_time)
				graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$test== a_test & bb$process == a_process,]
			    counterdiff <- diff(graphed$cpu_time)
				graphed[,"cpu_time_diff"] <- append(c(0), counterdiff)		
				temp_data_frame <- rbind(temp_data_frame,  graphed)
			}
		}
	}
	temp_data_frame
}

createProcessUsageDataFrame <- function(bb,process) {
 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$build)
	tests = factor(bb$test)
	for(a_test in levels(tests)) {
		for(a_build in levels(builds)) {
			filtered <- bb[bb$build == a_build & bb$test == a_test,]
			max_time <- max(filtered$test_time)
			graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$process == process & bb$test== a_test,]
		    counterdiff <- diff(graphed$cpu_time)
			graphed[,"cpu_time_diff"] <- append(c(0), counterdiff)		
			temp_data_frame <- rbind(temp_data_frame,  graphed)
		}
	}
	temp_data_frame
}

commaize <- function(x, ...) {
	prettySize(x)
#	format(x, decimal.mark = ",", trim = TRUE, scientific = FALSE, ...)
}

createDataFrame <- function(bb) {
	(temp_data_frame <- data_frame[FALSE, ])
    builds = factor(bb$build)
    tests = factor(bb$test)
    for(a_test in levels(tests)) {
		for(a_build in levels(builds)) {
			filtered <- bb[bb$build == a_build & bb$test == a_test,]
			max_time <- max(filtered$test_time)
			#print(max_time)
			graphed <- bb[bb$build == a_build & bb$test_time == max_time & bb$test == a_test,]
			temp_data_frame <- rbind(temp_data_frame,  graphed)
         }
     }
     temp_data_frame
}

ggplotCpuUsageWithFacets <- function(df,title) {
	p <- ggplot(df, aes(rowid, cpu_time_diff, color=build ,fill=build, label= comma(cpu_time_diff)))
#	p <- p + geom_line(aes(rowid, cpu_time_diff, color=build))
	p <- p + stat_smooth(se = TRUE)
	p <- p + labs(y='stime+utime', x="----time (every 10 secs) --->")
	p <- p + opts(title=paste(title))
	p <- p + scale_y_continuous(formatter="commaize",limits = c(min(temp_data_frame$cpu_time_diff),quantile(temp_data_frame$cpu_time_diff,0.99)))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
	p <- p + facet_wrap(~test, ncol=3, scales='free')
    print(p)	
}

ggplotAllProcessesCpuUsageWithFacets <- function(df,title) {
	
	builds = factor(df$build)
#	for(a_build in levels(builds)) {
		p <- ggplot(df, aes(rowid, cpu_time_diff, color= process ,fill=process, label= comma(cpu_time_diff)))
		p <- ggplot(df, aes(rowid, cpu_time_diff, color= build ,fill=process, label= comma(cpu_time_diff)))
		p <- p + geom_line(aes(rowid, cpu_time_diff, color=build))
	#	p <- p + stat_smooth(se = TRUE)
		p <- p + labs(y='stime+utime', x="----time (every 10 secs) --->")
		p <- p + opts(title=paste(title))
		p <- p + scale_y_continuous(formatter="commaize",expand=c(0,0),limits = c(min(temp_data_frame$cpu_time_diff),quantile(temp_data_frame$cpu_time_diff,0.99)))
		p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
	    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
		p <- p + facet_wrap(~test, ncol=3, scales='free')
	    print(p)
#    }
}

ggplotMemoryUsageWithFacets <- function(df, title){
	p <- ggplot(df, aes(rowid, rss, color=build, fill=build, label=rss))
	p <- p + geom_line(aes(rowid, rss, color=build))
	p <- p + labs(y='Memory (in MB)', x="----time (sampling ~ 10 secs) --->")
	p <- p + opts(title=paste(title))
	p <- p + scale_y_continuous(formatter="comma",limits = c(min(temp_data_frame$rss),max(temp_data_frame$rss)))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
	p <- p + facet_wrap(~test, ncol=3, scales='free')
    print(p)	
}

prettySize <- function(s, fmt="%.2f") {
    sizes <- c('', 'K', 'M', 'G', 'T', 'P', 'E')
    f <- ifelse(s == 0, NA, e <- floor(log(s, 1024)))
    suffix <- ifelse(s == 0, '', sizes[f+1])
	prefix <- ifelse(s == 0, s, sprintf(fmt, s/(1024 ^ floor(e))))
    paste(prefix, suffix, sep="")
}

args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))

#interesting_builds = c("membase-1.7.1r−68−g5d4d0d7","couchbase-2.0.0r−175−gd0c9c65")
interesting_builds = c("1.7.1r-68-g5d4d0d7","2.0.0r-175-gd0c9c65","2.0.0r−199−g50dbc8a","2.0.0r−200−g904d8d8",args[1])
print(paste("comparing only these builds ",interesting_builds))

pdf(file=paste('perf_graphs',sep="",".pdf"),height=14,width=14)

data_frame <- data.frame()
data_frame <- data.frame(t(rep(NA, 25)))
# Does not import the null values if present in json 
cat("generating ops/sec graph\n")
mcstats_json <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/data", sep=''))$rows
mcstats <- plyr::ldply(mcstats_json, unlist)
names(mcstats) <- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time', 'timestamp','ops', 'disk_write_queue', 'memory_used', 'resident_ratio', 'drain_rate', 'active_disk_queue','disk_reads')

mcstats <- pruneBuildNames(mcstats,interesting_builds)
mcstats <- transform(mcstats, timestamp=as.numeric(timestamp), ops=as.numeric(ops), disk_write_queue=as.numeric(disk_write_queue), drain_rate=as.numeric(drain_rate))

    
temp_data_frame <- createDataFrame(mcstats)

p <- ggplot(temp_data_frame, aes(timestamp, ops, color=build ,fill=build, label=ops)) + labs(x="----time (sec)--->", y="OPS")
p <- p + opts(title=paste("Operations Per Sec", sep=""))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(temp_data_frame$ops),max(temp_data_frame$ops)))
p  <-  p + stat_smooth(se = TRUE)
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~test, ncol=3, scales='free')
print(p)
	
	
	cat("generating disk write queue graph\n")
	p <- ggplot(temp_data_frame, aes(timestamp, disk_write_queue, color=build ,fill=build, label= disk_write_queue)) + labs(x="----time (sec)--->", y="dwq")
p  <-  p + stat_smooth(se = TRUE)
p <- p + opts(title=paste("Disk Write Queue", sep=''))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(temp_data_frame$disk_write_queue),max(temp_data_frame$disk_write_queue)))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~test, ncol=3, scales='free')
print(p)



cat("generating disk drain rate graph\n")
p <- ggplot(temp_data_frame, aes(timestamp, drain_rate, color=build, fill=build, label=drain_rate)) 
p <- p + labs(x="----time (sec)--->", y="drain_rate")
p  <-  p + stat_smooth(se = TRUE)
#	p <- p + geom_line(aes(timestamp, drain_rate, color=build))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(temp_data_frame$drain_rate),quantile(temp_data_frame$drain_rate,0.99)))
p <- p + opts(title=paste("Drain Rate", sep=''))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~test, ncol=3, scales='free_y')
print(p)

# Does not import the null values if present in json
systemstats <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/systemstats", sep=''))$rows
systemstats <- plyr::ldply(systemstats, unlist)
names(systemstats)<- c('id', 'build', 'ram', 'os', 'doc_id', 'test', 'test_time', 'test_name', 'max_items', 'min_value_size', 'total_gets', 'total_creates', 'total_sets', 'total_items', 'total_misses', 'run_time','rowid', 'time_sample', 'rss', 'process','cpu_time')
	
systemstats <- pruneBuildNames(systemstats, interesting_builds)
systemstats <- transform(systemstats, rowid=as.numeric(rowid), ram=as.numeric(ram), test_time=as.numeric(test_time), time_sample=as.numeric(time_sample), rss=as.numeric(rss),cpu_time=as.numeric(cpu_time))
# Memory Usage
cat("Generating Memory/CPU usage for beam.smp and memcached\n")
temp_data_frame  = createProcessUsageDataFrame(systemstats, "beam.smp")	
ggplotCpuUsageWithFacets(temp_data_frame,"beam.smp cpu ticks")
ggplotMemoryUsageWithFacets(temp_data_frame,"beam.smp memory profile")
temp_data_frame  = createProcessUsageDataFrame(systemstats, "memcached")
ggplotCpuUsageWithFacets(temp_data_frame,"memcached cpu ticks")
ggplotMemoryUsageWithFacets(temp_data_frame,"memcached memory profile")
temp_data_frame  = createAllProcessesUsageDataFrame(systemstats)
ggplotAllProcessesCpuUsageWithFacets(temp_data_frame,"cpu ticks")


latency_json <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/latency-get", sep=''))$rows
l <- latency_json
result <- createLatencyData(l)
print(nrow(result))
print(ncol(result))
df <- data.frame(result,stringsAsFactors = FALSE)
df <- df[2:nrow(df),]
names(df) <- c('percentile_90', 'latency_90', 'percentile_99','latency_99','build','ram','os', 'doc_id', 'test', 'test_time', 'test_name','item_value_size','row')

xf <- filterBuildsByTestName(df)


xf$row <- as.numeric(factor(xf$row))
xf$latency_90 = as.numeric(xf$latency_90) * 1000
xf$latency_99 = as.numeric(xf$latency_99) * 1000

p <- ggplot(xf, aes(row, latency_99, color=build ,fill=build, label= latency_99))
p <- p + opts(title=paste("Get Operations - Latency 99th Percentile", sep=''))
p <- p + facet_wrap(~test, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

p <- ggplot(xf, aes(row, latency_90, color=build ,fill=build, label= latency_90))
p <- p + opts(title=paste("Get Operations - Latency 90th Percentile", sep=''))
p <- p + facet_wrap(~test, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

latency_json <- fromJSON(file=paste("http://couchdb2.couchbaseqe.com:5984/experimental","/_design/data/_view/latency-set", sep=''))$rows
l <- latency_json
result <- createLatencyData(l)
print(nrow(result))
print(ncol(result))
df <- data.frame(result,stringsAsFactors = FALSE)
df <- df[2:nrow(df),]
names(df) <- c('percentile_90', 'latency_90', 'percentile_99','latency_99','build','ram','os', 'doc_id', 'test', 'test_time', 'test_name','item_value_size','row')

xf <- filterBuildsByTestName(df)


xf$row <- as.numeric(factor(xf$row))
xf$latency_90 = as.numeric(xf$latency_90) * 1000
xf$latency_99 = as.numeric(xf$latency_99) * 1000

p <- ggplot(xf, aes(row, latency_99, color=build ,fill=build, label= latency_99))
p <- p + opts(title=paste("Set Operations - Latency 99th Percentile", sep=''))
p <- p + facet_wrap(~test, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

p <- ggplot(xf, aes(row, latency_90, color=build ,fill=build, label= latency_90))
p <- p + opts(title=paste("Set Operations - Latency 90th Percentile", sep=''))
p <- p + facet_wrap(~test, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)
		
dev.off()

