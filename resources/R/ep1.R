source("executive.R")

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)

library(plotrix)
library(methods)

#Rscript ep1.R 1.8.0r-55-g80f24f2-community 2.0.0r-552-gd0bac7a-community EPT-READ-original couchdb2.couchbaseqe.com eperf ept-read-#nonjson-180r55-200r552

args <- commandArgs(TRUE)
args <- unlist(strsplit(args," "))
# ep1.R 1.8.0r-55-g80f24f2-community 2.0.0r-452-gf1c60e1-community EPT-WRITE-original couchdb2.couchbaseqe.com eperf ept-write-nonjson-180-200r452
baseline_build = args[1]
new_build = args[2]
test_name = args[3]
dbip = args[4]
dbname = args[5]
pdfname = args[6]
# pdfname = "ept-write-nonjson-180-200r452"

pdf(file=paste(pdfname,sep="",".pdf"),height=8,width=10,paper='USr')
# baseline_build="1.8.0r-55-g80f24f2-community"
# new_build = "2.0.0r-452-gf1c60e1-community"
# test_name = "EPT-WRITE-original"
# dbip = "couchdb2.couchbaseqe.com"
# dbname= "eperf"
# pdfname = "ept-write-nonjson-180-200r452.pdf"
#i_builds = c("1.7.2r-22-geaf53ef", new_build)
i_builds = c(baseline_build, new_build)

cat(paste("args : ",args,""),sep="\n")

commaize <- function(x, ...) {		
	prettySize(x)
#	format(x, decimal.mark = ",", trim = TRUE, scientific = FALSE, ...)
}

makeFootnote <- function(footnoteText=
                         format(Sys.time(), "%d %b %Y"),
                         size= .7, color= grey(.5))
{
   require(grid)
   pushViewport(viewport())
   grid.text(label= footnoteText ,
             x = unit(1,"npc") - unit(2, "mm"),
             y= unit(2, "mm"),
             just=c("right", "bottom"),
             gp=gpar(cex= size, col=color))
   popViewport()
}

addopts <- function(aplot,atitle) {
	aplot <- aplot + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = ))
	aplot <- aplot + opts(axis.ticks = theme_segment(colour = 'red', size = 1))
	aplot <- aplot + opts(title=paste(atitle, sep=''))
	aplot <- aplot + scale_color_brewer() + scale_y_continuous(formatter="commaize")
}

prettySize <- function(s, fmt="%.2f") {
    sizes <- c('', 'K', 'M', 'G', 'T', 'P', 'E')
    f <- ifelse(s == 0, NA, e <- floor(log(s, 1024)))
    suffix <- ifelse(s == 0, '', sizes[f+1])
	prefix <- ifelse(s == 0, s, sprintf(fmt, s/(1024 ^ floor(e))))
    paste(prefix, suffix, sep="")
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


ggplotAllProcessesCpuUsageWithFacets <- function(df,title) {

        builds = factor(df$buildinfo.version)
#       for(a_build in levels(builds)) {
                p <- ggplot(df, aes(row, cpu_time_diff, color= buildinfo.version ,fill=comm, label=comma(cpu_time_diff)))
                p <- ggplot(df, aes(row, cpu_time_diff, color= buildinfo.version ,fill=comm, label= comma(cpu_time_diff)))
#               p <- p + geom_line(aes(row, cpu_time_diff, color=buildinfo.version))
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

createProcessUsageDataFrame <- function(bb,process) {
 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$buildinfo.version)
	print(builds)
	tests = factor(bb$name)
	ips = factor(bb$ip)
	id <- unique(bb[bb$buildinfo.version==build,]$unique_id)[1]
	print(id)
	for(a_test in levels(tests)) {
		for(a_build in levels(builds)) {
				filtered <- bb[bb$buildinfo.version == a_build & bb$name == a_test & bb$unique_id ==id,]
				print(unique(filtered$unique_id))
				graphed <- bb[bb$buildinfo.version == a_build & bb$comm == process & bb$name == a_test & bb$unique_id ==id,]
                print(unique(graphed$unique_id))
				graphed <- transform(graphed,cpu_time = as.numeric(utime) + as.numeric(stime))
			    counterdiff <- diff(graphed$cpu_time)
				graphed[,"cpu_time_diff"] <- append(c(0), counterdiff)		
				temp_data_frame <- rbind(temp_data_frame,  graphed)
		}
	}
	temp_data_frame
}


builds_json <- fromJSON(file=paste("http://",dbip,":5984/",dbname,"/","/_design/data/_view/by_test_time", sep=''))$rows
builds_list <- plyr::ldply(builds_json, unlist)

names(builds_list) <- c('id', 'build', 'test_name', 'test_spec_name','runtime','is_json','test_time')

 # Pick the latest stats doc for a given test on a given build
 (fbl <- builds_list[FALSE, ])
 for(name in levels(factor(builds_list$test_name))) {
	 for(a_build in levels(factor(builds_list[builds_list$test_name == name,]$build))) {
		 filtered = builds_list[builds_list$build==a_build & builds_list$test_name == name,]
		 max_time = max(filtered$test_time)
		 filtered = filtered[filtered $test_time == max_time,]
		 # print(filtered)
		 fbl <- rbind(fbl,filtered)
	 }
 }

builds_list <- fbl
builds_list <- builds_list[builds_list$build %in% i_builds & builds_list$test_name == test_name & builds_list$is_json=='0',]
print(builds_list)
# Following metrics are to be fetch from CouchDB and plotted
metric_list = c('ns_server_data', 'systemstats', 'latency-get','latency-set')

# Get ns_server_data

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
	if (ncol(result) > 0 & ncol(result) != ncol(unlisted)) {
		#rbind.fill does not work if arg1 or arg2 is empty
		result <- rbind.fill(result,unlisted)
	} else {
		result <- rbind(result,unlisted)
	}
}
cat("generated ns_server_data data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))

ns_server_data <- result
ns_server_data $row <- as.numeric(ns_server_data $row)
ns_server_data $ep_queue_size <- as.numeric(ns_server_data $ep_queue_size)
ns_server_data $ep_diskqueue_drain <- as.numeric(ns_server_data $ep_diskqueue_drain)
ns_server_data $ops <- as.numeric(ns_server_data $ops)
ns_server_data $ep_bg_fetched <- as.numeric(ns_server_data $ep_bg_fetched)
ns_server_data $ep_tmp_oom_errors <- as.numeric(ns_server_data $ep_tmp_oom_errors)
ns_server_data $vb_active_resident_items_ratio <- as.numeric(ns_server_data $vb_active_resident_items_ratio)
ns_server_data $vb_active_eject <- as.numeric(ns_server_data $vb_active_eject)
ns_server_data $vb_replica_eject <- as.numeric(ns_server_data $vb_replica_eject)
ns_server_data $ep_tap_replica_queue_backoff <- as.numeric(ns_server_data $ep_tap_replica_queue_backoff)
ns_server_data $mem_used <- as.numeric(ns_server_data$mem_used)
ns_server_data $curr_items_tot <- as.numeric(ns_server_data$curr_items_tot)
ns_server_data $cmd_get <- as.numeric(ns_server_data$cmd_get)
ns_server_data $cmd_set <- as.numeric(ns_server_data$cmd_set)
ns_server_data $get_misses <- as.numeric(ns_server_data$get_misses)
ns_server_data $get_hits <- as.numeric(ns_server_data$get_hits)
ns_server_data <- transform(ns_server_data , cache_miss=ifelse(ns_server_data$cmd_get <= ns_server_data$ep_bg_fetched,0,ns_server_data$ep_bg_fetched/ns_server_data$cmd_get))
ns_server_data$cache_miss <- ns_server_data$cache_miss*100
all_builds = factor(ns_server_data$buildinfo.version)
result_tmp <- data.frame()
for(a_build in levels(all_builds)) {
	tt <- ns_server_data[ns_server_data $buildinfo.version==a_build,]
	tt$timestamp <- as.numeric(tt$timestamp)
	min_timestamp = min(tt$timestamp)
	filtered = transform(tt,row=timestamp-min_timestamp)
	result_tmp <- rbind(result_tmp,filtered)
}
ns_server_data <- result_tmp
ns_server_data$row = ns_server_data $row/1000


cat("generating System stats from ns_server_data_system ")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
		tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ns_server_data_system", sep='')
		#cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
        ncol(unlisted)
		result <- rbind(result,unlisted)
		print(nrow(result))
	},error=function(e)e, finally=print("Error getting system stats from ns_server_data"))
}
cat("generated ns_server_data_system data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))

ns_server_data_system <- result
ns_server_data_system $row <- as.numeric(ns_server_data_system $row)
ns_server_data_system $cpu_util <- as.numeric(ns_server_data_system $cpu_util)
ns_server_data_system $swap_used <- as.numeric(ns_server_data_system $swap_used




# Get systemstats
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
system_stats$nswap <- as.numeric(system_stats$nswap)
system_stats$cnswap <- as.numeric(system_stats$cnswap)


# Get Latency-get
cat("generating latency-get\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-get", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-get"))
}
latency_get <- result
latency_get$row <- as.numeric(latency_get$row)
latency_get$mystery <- as.numeric(latency_get$mystery)
latency_get$percentile_99th <- as.numeric(latency_get$percentile_99th) * 1000
latency_get$percentile_95th <- as.numeric(latency_get$percentile_95th) * 1000
latency_get$percentile_90th <- as.numeric(latency_get$percentile_90th) * 1000


all_builds = factor(latency_get$buildinfo.version)
result <- data.frame()
for(a_build in levels(all_builds)) {
	tt <- latency_get[latency_get$buildinfo.version==a_build,]
	tt$mystery <- as.numeric(tt$mystery)
	min_myst = min(tt$mystery)
	filtered = transform(tt,row=mystery-min_myst)
	result <- rbind(result,filtered)
}
latency_get <- result



#factor by build number and fix _timestamp

# Get Latency-set
cat("generating latency-set\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-set", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-set"))
}
latency_set <- result
latency_set$row <- as.numeric(latency_set$row)
latency_set$mystery <- as.numeric(latency_set$mystery)
latency_set$percentile_99th <- as.numeric(latency_set$percentile_99th) * 1000
latency_set$percentile_95th <- as.numeric(latency_set$percentile_95th) * 1000
latency_set$percentile_90th <- as.numeric(latency_set$percentile_90th) * 1000

all_builds = factor(latency_set$buildinfo.version)
result <- data.frame()
for(a_build in levels(all_builds)) {
	tt <- latency_set[latency_set $buildinfo.version==a_build,]
	tt$mystery <- as.numeric(tt$mystery)
	min_myst = min(tt$mystery)
	filtered = transform(tt,row=mystery-min_myst)
	result <- rbind(result,filtered)
}
latency_set <- result

# Get Data size on disk
cat("generating disk usage over time")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","bucket-size", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		print(ncol(unlisted))
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting bucket disk size"))
}
disk_data <- result
disk_data$row <- as.numeric(disk_data$row)
disk_data$size <- as.numeric(disk_data$size)
disk_data$row <- as.numeric(factor(disk_data$row))


builds_list$runtime = as.numeric(builds_list$runtime)
#baseline= c("1.7.2r-22-geaf53ef")
baseline <- baseline_build
first_row <- c("system","test","value")
combined <- data.frame(t(rep(NA,3)))
names(combined)<- c("system","test","value")
p <- combined[2:nrow(combined), ]

# Test runtime is present in builds_list
builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	
	fi <-builds_list[builds_list$build==build, ]
	d <- fi$runtime
	print(d)	
	if(build == baseline){
		row <-c ("baseline", "runtime", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "runtime", as.numeric(d))
		combined <- rbind(combined, row)	
	}

}
builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$ep_diskqueue_drain!=0, ]
	d <- mean(fi$ep_diskqueue_drain)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "drain rate", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "drain rate", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}


builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_get[latency_get$buildinfo.version==build & latency_get$client_id ==0, ]
	d <- mean(fi$percentile_95th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-get (95th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-get (95th)", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}

builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_set[latency_set$buildinfo.version==build & latency_set$client_id ==0, ]	
	d <- mean(fi$percentile_95th)

	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-set (95th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-set (95th)", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}

builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-disk_data[disk_data$buildinfo.version==build, ]
	d <- max(fi$size)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "peak disk", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "peak disk", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}


builds = factor(system_stats$buildinfo.version)
for(build in levels(builds)) {	
    id <- unique(system_stats[system_stats$buildinfo.version==build,]$unique_id)[1]
	fi_memcached <-system_stats[system_stats$buildinfo.version==build & system_stats$comm=="(memcached)" & system_stats$unique_id == id, ]
	fi_beam <-system_stats[system_stats$buildinfo.version==build & system_stats$comm=="(beam.smp)" & system_stats$unique_id == id, ]

	print("here")
	d <- max(fi_memcached$rss + fi_beam$rss)
	
	print(d)
	if(build == baseline){
		row <-c ("baseline", "peak mem", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "peak mem", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}


p <- combined[2:nrow(combined), ]
p$value <- as.numeric(p$value)
df <- fixupData(buildComparison(p , 'system', 'baseline'))

ggplot(data=df[df$system != 'baseline',], aes(test, position, fill=color)) +
  geom_hline(yintercept=0, lwd=1, col='#777777') +
geom_bar(stat='identity', position='dodge') +
  scale_fill_manual('Result', values=colors, legend=FALSE) +
  geom_hline(yintercept=.10, lty=3) +
  geom_hline(yintercept=-.10, lty=3) +
  scale_y_continuous(limits=c(-1 * (magnitude_limit - 1), magnitude_limit - 1),
                     formatter=function(n) sprintf("%.1fx", abs(n) + 1)) +
  opts(title=paste(builds_list$test_name,':', baseline_build, ':', new_build )) +
  labs(y='(righter is better)', x='') +
  geom_text(aes(x=test, y=ifelse(abs(position) < .5, .5, sign(position) * -.5),
                label=sprintf("%.02fx", abs(offpercent))),
            size=4, colour="#999999") +
coord_flip() +
  theme_bw()
footnote <- paste(builds_list$test_name, baseline_build, new_build, format(Sys.time(), "%d %b %Y"), sep=" / ")

makeFootnote(footnote)

builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$ops !=0, ]			         
	print("here")
	d <- mean(fi$ops)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "ops", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "ops", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
	
}

builds = factor(system_stats$buildinfo.version)
for(build in levels(builds)) {
	id <- unique(system_stats[system_stats$buildinfo.version==build,]$unique_id)[1]
    print(id)	
	fi_memcached <-system_stats[system_stats$buildinfo.version==build & system_stats$comm=="(memcached)" & system_stats$unique_id==id, ]

	print("here")
	d <- mean(fi_memcached$rss)
	
	print(d)
	if(build == baseline){
		row <-c ("baseline", "Avg. mem memcached", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "Avg. mem memcached", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}

builds = factor(system_stats$buildinfo.version)
for(build in levels(builds)) {	
    id <- unique(system_stats[system_stats$buildinfo.version==build,]$unique_id)[1]
    print(id)
	fi_beam <-system_stats[system_stats$buildinfo.version==build & system_stats$comm=="(beam.smp)" & system_stats$unique_id ==id, ]

	print("here")
	d <- mean(fi_beam$rss)
	
	print(d)
	if(build == baseline){
		row <-c ("baseline", "Avg. mem beam.smp", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "Avg. mem beam.smp", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}

builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_get[latency_get$buildinfo.version==build & latency_get$client_id ==0, ]
	d <- mean(fi$percentile_90th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-get (90th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-get (90th)", as.numeric(d))
		combined <- rbind(combined, row)	
	}
	
}
builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_get[latency_get$buildinfo.version==build & latency_get$client_id ==0, ]
	d1 <- mean(fi$percentile_99th)
	print(d)
	print(d1)
	if(build == baseline){
		row <-c ("baseline", "latency-get (99th)", as.numeric(d1))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-get (99th)", as.numeric(d1))
		combined <- rbind(combined, row)	
	}
	
}
builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_set[latency_set$buildinfo.version==build & latency_set$client_id ==0, ]
	d <- mean(fi$percentile_90th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-set (90th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-set (90th)", as.numeric(d))
		combined <- rbind(combined, row)	
	}
}


builds = factor(ns_server_data$buildinfo.version)
for(build in levels(builds)) {	

	fi <-latency_set[latency_set$buildinfo.version==build & latency_set$client_id ==0, ]
	d1 <- mean(fi$percentile_99th)
	print(d1)
	if(build == baseline){
		row <-c ("baseline", "latency-set (99th)", as.numeric(d1))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-set (99th)", as.numeric(d1))
		combined <- rbind(combined, row)	
	}
}



p <- combined[2:nrow(combined), ]
p$value <- as.numeric(p$value)

MB1 <- p[p[,'system'] == 'baseline',]$value
MB1 <- as.numeric(sprintf("%.2f", MB1))
CB1 <- p[p[,'system'] != 'baseline',]$value
CB1 <- as.numeric(sprintf("%.2f", CB1))
MB <- c()
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[1]/3600)))
MB <- append(MB, commaize(MB1[2]))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[5]/1024)))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[6]/1024)))
MB <- append(MB, commaize(MB1[7]))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[8]/1024)))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[9])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[10])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[3])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[11])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[12])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[4])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[13])))


CB <- c()
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[1]/3600)))
CB <- append(CB, commaize(CB1[2]))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[5]/1024)))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[6]/1024)))
CB <- append(CB, commaize(CB1[7]))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[8]/1024)))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[9])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[10])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[3])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[11])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[12])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[4])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[13])))


testdf <- data.frame(MB,CB)
rownames(testdf)<-c("Runtime (in hr)","Avg. Drain Rate","Peak Disk (GB)","Peak Memory (GB)", "Avg. OPS", "Avg. mem memcached (GB)", "Avg. mem beam.smp (MB)","Latency-get (90th) (ms)", "Latency-get (95th) (ms)","Latency-get (99th) (ms)","Latency-set (90th) (ms)","Latency-set (95th) (ms)","Latency-set (99th) (ms)")
plot.new()
col1 <- paste(unlist(strsplit(baseline_build, "-"))[1],"-",unlist(strsplit(baseline_build, "-"))[2])
col2 <- paste(unlist(strsplit(new_build, "-"))[1],"-",unlist(strsplit(new_build, "-"))[2]) 
grid.table(testdf, h.even.alpha=1, h.odd.alpha=1,  v.even.alpha=0.5, v.odd.alpha=1,cols=c(col1, col2))
makeFootnote(footnote)


cat("generating ops/sec \n")
p <- ggplot(ns_server_data, aes(row, ops, color=buildinfo.version , label= prettySize(ops))) + labs(x="----time (sec)--->", y="ops/sec")
p <- p + geom_point()
p <- addopts(p,"ops/sec")
print(p)
makeFootnote(footnote)

cat("generating disk write queue \n")
p <- ggplot(ns_server_data, aes(row, ep_queue_size, color=buildinfo.version , label= prettySize(ep_queue_size))) + labs(x="----time (sec)--->", y="ep_queue_size")
p <- p + geom_point()
p <- addopts(p,"disk write queue")
print(p)
makeFootnote(footnote)




cat("generating ep_diskqueue_drain \n")
p <- ggplot(ns_server_data, aes(row, ep_diskqueue_drain, color=buildinfo.version , label= prettySize(ep_diskqueue_drain))) + labs(x="----time (sec)--->", y="ep_diskqueue_drain")
p <- p + geom_point()
p <- addopts(p,"ep_diskqueue_drain")
print(p)
makeFootnote(footnote)


cat("generating ep_bg_fetched \n")
p <- ggplot(ns_server_data, aes(row, ep_bg_fetched, color=buildinfo.version , label= prettySize(ep_bg_fetched))) + labs(x="----time (sec)--->", y="ep_bg_fetched")
p <- p + geom_point()
p <- addopts(p,"ep_bg_fetched ops/sec")
print(p)
makeFootnote(footnote)


cat("generating tmp_oom \n")
p <- ggplot(ns_server_data, aes(row, ep_tmp_oom_errors, color=buildinfo.version , label= prettySize(ep_tmp_oom_errors))) + labs(x="----time (sec)--->", y="tmp_oom")
p <- p + geom_point()
p <- addopts(p,"tmp_oom ops/sec")
print(p)
makeFootnote(footnote)



ns_server_data $vb_replica_eject <- as.numeric(ns_server_data $vb_replica_eject)
ns_server_data $ep_tap_replica_queue_backoff <- as.numeric(ns_server_data $ep_tap_replica_queue_backoff)


cat("generating vb_active_eject \n")
p <- ggplot(ns_server_data, aes(row, vb_active_eject, color=buildinfo.version , label= prettySize(vb_active_eject))) + labs(x="----time (sec)--->", y="vb_active_eject")
p <- p + geom_point()
p <- addopts(p,"vb_active_eject/sec")
print(p)
makeFootnote(footnote)



cat("generating vb_replica_eject \n")
p <- ggplot(ns_server_data, aes(row, vb_replica_eject, color=buildinfo.version , label= prettySize(vb_replica_eject))) + labs(x="----time (sec)--->", y="vb_replica_eject")
p <- p + geom_point()
p <- addopts(p,"vb_replica_eject/sec")
print(p)
makeFootnote(footnote)


cat("generating ep_tap_replica_queue_backoff \n")
p <- ggplot(ns_server_data, aes(row, ep_tap_replica_queue_backoff, color=buildinfo.version , label= prettySize(ep_tap_replica_queue_backoff))) + labs(x="----time (sec)--->", y="ep_tap_replica_queue_backoff")
p <- p + geom_point()
p <- addopts(p,"ep_tap_replica_queue_backoff/sec")
print(p)
makeFootnote(footnote)



cat("generating vb_active_resident_items_ratio \n")
p <- ggplot(ns_server_data, aes(row, vb_active_resident_items_ratio, color=buildinfo.version , label= prettySize(vb_active_resident_items_ratio))) + labs(x="----time (sec)--->", y="vb_active_resident_items_ratio")
p <- p + geom_point()
p <- addopts(p,"vb_active_resident_items_ratio")
print(p)
makeFootnote(footnote)



cat("generating cur_items_total \n")
p <- ggplot(ns_server_data, aes(row, curr_items_tot, color=buildinfo.version , label= prettySize(curr_items_tot))) + labs(x="----time (sec)--->", y="curr_items_tot")
p <- p + geom_point()
p <- addopts(p,"cur_items_total")
print(p)
makeFootnote(footnote)

cat("generating mem_used \n")
p <- ggplot(ns_server_data, aes(row, mem_used, color=buildinfo.version , label= mem_used)) + labs(x="----time (sec)--->", y="ops/sec")
p <- p + geom_point()
p <- addopts(p,"mem_used")
print(p)
makeFootnote(footnote)



cat("generating data disk size\n")
p <- ggplot(disk_data, aes(row,size, color=buildinfo.version, label=size)) + labs(x="----time (sec)--->", y="size (MB)")
p <- p + geom_point()
p <- addopts(p,"data disk size")
print(p)
makeFootnote(footnote)



cat("generating cmd_set \n")
p <- ggplot(ns_server_data, aes(row, cmd_set, color=buildinfo.version , label= cmd_set)) + labs(x="----time (sec)--->", y="ops/sec")
p <- p + geom_point()
p <- addopts(p,"cmd_set ops/sec")
print(p)
makeFootnote(footnote)


cat("generating cmd_get \n")
p <- ggplot(ns_server_data, aes(row, cmd_get, color=buildinfo.version , label= cmd_get)) + labs(x="----time (sec)--->", y="ops/sec")
p <- p + geom_point()
p <- addopts(p,"cmd_get ops/sec")
print(p)
makeFootnote(footnote)


cat("generating get misses \n")
p <- ggplot(ns_server_data, aes(row, get_misses, color=buildinfo.version , label= get_misses)) + labs(x="----time (sec)--->", y="# of get misses")
p <- p + geom_point()
p <- addopts(p,"# of get misses")
print(p)
makeFootnote(footnote)

cat("generating get hits \n")
p <- ggplot(ns_server_data, aes(row, get_hits, color=buildinfo.version , label= get_hits)) + labs(x="----time (sec)--->", y="# of get hits")
p <- p + geom_point()
p <- addopts(p,"# of get hits")
print(p)
makeFootnote(footnote)


cat("generating cache_miss \n")
p <- ggplot(ns_server_data, aes(row, cache_miss, color=buildinfo.version , label= cache_miss)) + labs(x="----time (sec)--->", y="cache_miss percentage")
p <- p + geom_point()
p <- addopts(p,"cache_miss percentage")
print(p)
makeFootnote(footnote)


cat("Latency-get 90th\n")
temp <- latency_get[latency_get$client_id ==0,]
p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_90th, linetype=buildinfo.version)) + labs(x="----time (sec)--->", y="ms")
#p  <-  p + stat_smooth(se = TRUE)
p <- p + geom_point()
p <- p + opts(title=paste("Latency-get 90th  percentile", sep=''))
p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'solid'))
#p <- p + facet_wrap(~name, ncol=3, scales='free')
print(p)
makeFootnote(footnote)



cat("Latency-get 95th\n")
temp <- latency_get[latency_get$client_id ==0,]
p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="ms")
p <- p + geom_point()
p <- addopts(p,"Latency-get 95th  percentile")
print(p)
makeFootnote(footnote)
cat("Latency-get 99th\n")
p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="ms")
p <- p + geom_point()
p <- addopts(p,"Latency-get 99th  percentile")
print(p)
makeFootnote(footnote)


cat("Latency-set 90th\n")
temp <- latency_set[latency_set$client_id ==0,]
p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="ms")
p <- p + geom_point()
p <- addopts(p,"Latency-set 90th  percentile")
print(p)
makeFootnote(footnote)
cat("Latency-set 95th\n")
p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="ms")
p <- p + geom_point()
p <- addopts(p,"Latency-set 95th  percentile")
print(p)
makeFootnote(footnote)
cat("Latency-set 99th\n")
p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="ms")
p <- p + geom_point()
p <- addopts(p,"Latency-set 99th  percentile")
print(p)
makeFootnote(footnote)



cat("generating cpu_util \n")
p <- ggplot(ns_server_data_system, aes(row, cpu_util, color=buildinfo.version , label= cpu_util)) + labs(x="----time (sec)--->", y="cpu_util")
p <- p + geom_point()
p <- addopts(p,"cpu_util")
print(p)
makeFootnote(footnote)

cat("generating swap_used \n")
p <- ggplot(ns_server_data_system, aes(row, swap_used, color=buildinfo.version , label= swap_used)) + labs(x="----time (sec)--->", y="swap_used")
p <- p + geom_point()
p <- addopts(p,"swap_used")
print(p)
makeFootnote(footnote)

#cat("Generating Memory/CPU usage for beam.smp and memcached\n")
#temp_data_frame  = createProcessUsageDataFrame(system_stats, "(beam.smp)")
#ggplotCpuUsageWithFacets(temp_data_frame,"beam.smp cpu ticks")
#ggplotMemoryUsageWithFacets(temp_data_frame,"beam.smp memory profile")
#makeFootnote(footnote)


dev.off()

