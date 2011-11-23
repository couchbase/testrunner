require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
library(methods)

filterBuildsByTestName <- function(data_frame) {
	builds = levels(factor(df$test_build))
	temp_data_frame <- df[FALSE, ]
	df$test_time = as.numeric(df$test_time)
	tests = levels(factor(df$test_name))
	for(a_test in tests) {
		for(a_build in builds) {
			filtered = df[df$test_build==a_build & df$test_name == a_test,]
			max_time = max(filtered$test_time)
			filtered = filtered[filtered$test_time == max_time,]
			temp_data_frame <- rbind(temp_data_frame,filtered)	
		}
	}
	temp_data_frame
}

createAllProcessesUsageDataFrame <- function(bb) {
 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$buildinfo.version)
	tests = factor(bb$name)
	processes = factor(bb$comm)
	for(a_process in levels(processes)) {
		for(a_test in levels(tests)) {
			for(a_build in levels(builds)) {
				filtered <- bb[bb$buildinfo.version == a_build & bb$name == a_test,]
				max_time <- max(filtered$info.test_time)
				graphed <- bb[bb$buildinfo.version == a_build & bb$info.test_time == max_time & bb$name== a_test & bb$comm == a_process,]
				graphed <- transform(graphed,cpu_time = as.numeric(utime) + as.numeric(stime))
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
	builds = factor(bb$buildinfo.version)
	tests = factor(bb$name)
	ips = factor(bb$ip)
	for(a_test in levels(tests)) {
		for(a_build in levels(builds)) {
			for(ip in levels(ips)) {
				filtered <- bb[bb$buildinfo.version == a_build & bb$name == a_test & bb$ip == ip,]
				graphed <- bb[bb$buildinfo.version == a_build & bb$comm == process & bb$name == a_test & bb$ip == ip,]
				graphed <- transform(graphed,cpu_time = as.numeric(utime) + as.numeric(stime))
			    counterdiff <- diff(graphed$cpu_time)
				graphed[,"cpu_time_diff"] <- append(c(0), counterdiff)		
				temp_data_frame <- rbind(temp_data_frame,  graphed)
			}
		}
	}
	temp_data_frame
}

commaize <- function(x, ...) {
	prettySize(x)
#	format(x, decimal.mark = ",", trim = TRUE, scientific = FALSE, ...)
}

ggplotCpuUsageWithFacets <- function(df,title) {
	names = factor(df$name)
	for(name in levels(names)) {
		p <- ggplot(df[df$name==name,], aes(row, cpu_time_diff, color=buildinfo.version ,fill=buildinfo.version, label= comma(cpu_time_diff)))
		p <- p + geom_line(aes(row, cpu_time, color=buildinfo.version))
#		p <- p + stat_smooth(se = TRUE)
		p <- p + labs(y='stime+utime', x="----time (every 10 secs) --->")
		p <- p + opts(title=paste(name))
#		p <- p + scale_y_continuous(formatter="commaize",limits = c(min(df$cpu_time_diff),quantile(df$cpu_time_diff,0.99)))
		p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
	    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
		p <- p + facet_wrap(~ip, ncol=3, scales='free')
	    print(p)
	}	
}

ggplotAllProcessesCpuUsageWithFacets <- function(df,title) {
	
	builds = factor(df$buildinfo.version)
	names = factor(df$name)
	for(name in levels(names)) {
#	for(a_build in levels(builds)) {
		p <- ggplot(df[df$name==name,], aes(row, cpu_time_diff, color= buildinfo.version ,fill=comm, label=comma(cpu_time_diff)))
		p <- ggplot(df, aes(row, cpu_time_diff, color= buildinfo.version ,fill=comm, label= comma(cpu_time_diff)))
#		p <- p + geom_line(aes(row, cpu_time_diff, color=buildinfo.version))
		p <- p + stat_smooth(se = TRUE)
		p <- p + labs(y='stime+utime', x="----time (every 10 secs) --->")
		p <- p + opts(title=paste(title))
		p <- p + scale_y_continuous(formatter="commaize",expand=c(0,0),limits = c(min(temp_data_frame$cpu_time_diff),quantile(temp_data_frame$cpu_time_diff,0.99)))
		p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
	    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))	
		p <- p + facet_wrap(~buildinfo.version, ncol=3, scales='free')
	    print(p)
#    }
	}
}

ggplotMemoryUsageWithFacets <- function(df, title) {	
	names = factor(df$name)
		p <- ggplot(df, aes(ip, rss, color= buildinfo.version, fill=buildinfo.version, label=commaize(rss)))
		p <- p + geom_boxplot()
	#	p <- p + geom_line(aes(row, rss, color=buildinfo.version))
		p <- p + labs(y='Memory (in MB)', x="----time (sampling ~ 10 secs) --->")
		p <- p + opts(title=paste(title))
		#	p <- p + scale_y_continuous(formatter="comma",limits = c(min(temp_data_frame$rss),max(temp_data_frame$rss)))
		p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
	    p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
	    p <- p + coord_flip()
	    print(p)	
#	}
}

prettySize <- function(s, fmt="%.2f") {
    sizes <- c('', 'K', 'M', 'G', 'T', 'P', 'E')
    f <- ifelse(s == 0, NA, e <- floor(log(s, 1024)))
    suffix <- ifelse(s == 0, '', sizes[f+1])
	prefix <- ifelse(s == 0, s, sprintf(fmt, s/(1024 ^ floor(e))))
    paste(prefix, suffix, sep="")
}


#interesting_builds = c("1.7.1r-68-g5d4d0d7","2.0.0r-175-gd0c9c65","2.0.0r−199−g50dbc8a","2.0.0r−200−g904d8d8",args[1])
#print(paste("comparing only these builds ",interesting_builds))

args <- commandArgs(TRUE)
cat(paste("args : ",args,""),sep="\n")
args <- unlist(strsplit(args," "))

new_build = args[1]
dbip = args[2]
dbname = args[3]
pdfname = args[4]

# new_build= "2.0.0r-243-g9f06011"
#new_build = "2.0.0r-225-gee14e50"
#dbip = "localhost"
#dbname= "ep"
#pdfname = "ep.pdf"



#pdf(file=paste(pdfname,sep="",".pdf"),height=14,width=14)
pdf(file=paste(pdfname,sep="",".pdf"),height=7,width=7)

builds_json <- fromJSON(file=paste("http://",dbip,":5984/",dbname,"/","/_design/data/_view/by_test_time", sep=''))$rows
builds_list <- plyr::ldply(builds_json, unlist)

names(builds_list) <- c('id', 'build', 'test_name', 'test_spec_name','runtime','test_time')

# (fbl <- builds_list[FALSE, ])
# for(name in levels(factor(builds_list$test_name))) {
	# for(a_build in levels(factor(builds_list[builds_list$test_name == name,]$build))) {
		# filtered = builds_list[builds_list$build==a_build & builds_list$test_name == name,]
		# max_time = max(filtered$test_time)
		# filtered = filtered[filtered $test_time == max_time,]
		# # print(filtered)
		# fbl <- rbind(fbl,filtered)	
	# }
# }

# builds_list <- fbl
# i_builds = c("1.7.1r-68-g5d4d0d7","2.0.0r-238-ge864d8b",args[1])
i_builds = c("1.7.1r-68-g5d4d0d7", "2.0.0r-231-g370366e","2.0.0r-233-gb6536ea",new_build)
builds_list <- builds_list[builds_list$build %in% i_builds,]


# #now download the membase stats 
# builds = factor(builds_list $build)
# len = length(levels(builds))
# build_rows = array(dim=c(len,5))

cat("comparing the test runtime")
builds_list$runtime = as.numeric(builds_list$runtime)
p <- ggplot(builds_list, aes(paste(test_name,build,sep="-"), runtime, color= build , label= test_time)) + labs(x="----time (sec)--->", y="ops/sec")
p <- p + geom_bar(fill="white",colour="darkgreen",position="stack")
p <- p + opts(title=paste("scenario runtime", sep=''))
p <- p + opts(panel.background = theme_rect(colour = 'red', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + coord_flip()
print(p)

cat("generating system stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","systemstats", sep='')
	cat(paste(url,"\n"))
	doc_json <- fromJSON(file=url)
	# cat(paste(builds_list[index,]$id,"\n"))
	unlisted <- plyr::ldply(doc_json, unlist)
	print(ncol(unlisted))
	result <- rbind(result,unlisted)
}
cat("generated system stats data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))
system_stats <- result
system_stats = transform(system_stats,utime=as.numeric(utime),stime=as.numeric(stime),rss=(as.numeric(rss) * 4096) / (1024 * 1024))
system_stats$row <- as.numeric(factor(system_stats$row))


cat("Generating Memory/CPU usage for beam.smp and memcached\n")
temp_data_frame  = createProcessUsageDataFrame(system_stats, "(beam.smp)")
ggplotCpuUsageWithFacets(temp_data_frame,"beam.smp cpu ticks")
ggplotMemoryUsageWithFacets(temp_data_frame,"beam.smp memory profile")

cat("generated Memory/CPU usage for beam.smp and memcached\n")

temp_data_frame  = createProcessUsageDataFrame(system_stats, "(memcached)")
ggplotCpuUsageWithFacets(temp_data_frame,"memcached cpu ticks")
ggplotMemoryUsageWithFacets(temp_data_frame,"memcached memory profile")
temp_data_frame  = createAllProcessesUsageDataFrame(system_stats)
#ggplotAllProcessesCpuUsageWithFacets(temp_data_frame,"cpu ticks")


cat("generating disk usage over time")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
#	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","data-size", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		print(ncol(unlisted))
		result <- rbind(result,unlisted)
#	},error=function(e)e, finally=print("Hello"))
}
disk_data <- result
disk_data$row <- as.numeric(disk_data$row)
disk_data$size <- as.numeric(disk_data$size)
disk_data$time <- as.numeric(disk_data$time)
disk_data$row <- as.numeric(factor(disk_data$row))

files = factor(disk_data$file)
for(file in levels(files)) {
	p <- ggplot(disk_data[disk_data$file==file,], aes(row, size, color=buildinfo.version ,fill=buildinfo.version, label=size)) + labs(x="----time (sec)--->", y="file size")
	p <- p + geom_point()
	# p <- p + geom_line(aes(row, size, color=buildinfo.version))
	p <- p + opts(title=paste("file size for file : ", file ,sep=''))
	p <- p + scale_y_continuous(formatter="commaize",limits = c(min(disk_data$size),max(disk_data$size)))
	p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
	p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
	p <- p + facet_wrap(~buildinfo.version, ncol=3, scales='free')
	print(p)
}



cat("generating ns_server_data ")

result <- data.frame()
for(index in 1:nrow(builds_list)) {
	url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ns_server_data", sep='')
	# cat(paste(url,"\n"))
	doc_json <- fromJSON(file=url)
	# cat(paste(builds_list[index,]$id,"\n"))
	unlisted <- plyr::ldply(doc_json, unlist)
	# print(ncol(unlisted))
	# print(nrow(unlisted))
	# print(unlisted[1,])
	result <- rbind(result,unlisted)
}
cat("generated ns_server_data data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))


ns_server_data <- result
ns_server_data $row <- as.numeric(ns_server_data $row)
ns_server_data $ep_queue_size <- as.numeric(ns_server_data $ep_queue_size)
ns_server_data $ep_diskqueue_drain <- as.numeric(ns_server_data $ep_diskqueue_drain)
ns_server_data $ops <- as.numeric(ns_server_data $ops)



cat("generating ops/per second \n")
p <- ggplot(ns_server_data, aes(row, ops, color=buildinfo.version ,fill= buildinfo.version, label=ops)) + labs(x="----time (sec)--->", y="ops/sec")
p  <-  p + stat_smooth(se = TRUE)
p <- p + opts(title=paste("ops/per second", sep=''))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(ns_server_data$ops),max(ns_server_data$ops)))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~name, ncol=3, scales='free')
print(p)

cat("generating disk write queue graph\n")
p <- ggplot(ns_server_data, aes(row, ep_queue_size, color=buildinfo.version ,fill=buildinfo.version, label=ep_queue_size)) + labs(x="----time (sec)--->", y="dwq")
p  <-  p + stat_smooth(se = TRUE)
p <- p + opts(title=paste("ep_queue_size", sep=''))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(ns_server_data$ep_queue_size),max(ns_server_data$ep_queue_size)))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~name, ncol=3, scales='free')
print(p)

cat("generating disk drain rate graph\n")
p <- ggplot(ns_server_data, aes(row, ep_diskqueue_drain, color=buildinfo.version, fill=buildinfo.version, label=ep_diskqueue_drain)) 
p <- p + labs(x="----time (sec)--->", y="drain_rate")
p  <-  p + stat_smooth(se = TRUE)
#	p <- p + geom_line(aes(timestamp, drain_rate, color=build))
p <- p + scale_y_continuous(formatter="commaize",limits = c(min(ns_server_data$ep_diskqueue_drain),quantile(ns_server_data$ep_diskqueue_drain,0.999)))
p <- p + opts(title=paste("Drain Rate", sep=''))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
p <- p + facet_wrap(~name, ncol=3, scales='free_y')
print(p)


result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-get", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Hello"))
}
latency_get <- result
latency_get$row <- as.numeric(latency_get$row)
latency_get$percentile_99th <- as.numeric(latency_get$percentile_99th) * 1000
latency_get$percentile_95th <- as.numeric(latency_get$percentile_95th) * 1000
latency_get$percentile_90th <- as.numeric(latency_get$percentile_90th) * 1000

p <- ggplot(latency_get, aes(row, percentile_99th, color=buildinfo.version ,fill= buildinfo.version, label=percentile_99th))
p <- p + opts(title=paste("Get Operations - Latency 99th Percentile", sep=''))
p <- p + facet_wrap(~name, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

p <- ggplot(latency_get, aes(row, percentile_90th, color=buildinfo.version ,fill=buildinfo.version, label=percentile_90th))
p <- p + opts(title=paste("Get Operations - Latency 90th Percentile", sep=''))
p <- p + facet_wrap(~name, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)


result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-set", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Hello"))
}
latency_get <- result
latency_get$row <- as.numeric(latency_get$row)
latency_get$percentile_99th <- as.numeric(latency_get$percentile_99th) * 1000
latency_get$percentile_95th <- as.numeric(latency_get$percentile_95th) * 1000
latency_get$percentile_90th <- as.numeric(latency_get$percentile_90th) * 1000


p <- ggplot(latency_get, aes(row, percentile_99th, color=buildinfo.version ,fill= buildinfo.version, label=percentile_99th))
p <- p + opts(title=paste("Set Operations - Latency 99th Percentile", sep=''))
p <- p + facet_wrap(~name, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

p <- ggplot(latency_get, aes(row, percentile_90th, color=buildinfo.version ,fill=buildinfo.version, label=percentile_90th))
p <- p + opts(title=paste("Set Operations - Latency 90th Percentile", sep=''))
p <- p + facet_wrap(~name, ncol=3, scales='free')
p <- p + geom_point()
p <- p + labs(x="----time (sampled every 100k ops)--->", y="latency(msec)")
print(p)

dev.off()