source("executive.R")

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
require(gplots, quietly=TRUE)

library(plotrix)
library(methods)

#Rscript ep1.R 1.8.0r-55-g80f24f2-community 2.0.0r-552-gd0bac7a-community EPT-READ-original couchdb2.couchbaseqe.com eperf ept-read-#nonjson-180r55-200r552
#Rscript ep1.R 1.8.0r-55-g80f24f2-enterprise 2.0.0r-710-toy-community EPT-WRITE-original couchdb2.couchbaseqe.com eperf EPT-SCALED-DOWN-WRITE.1-2.0.0r-709-toy-community-1.8.0r-55-g80f24f2-enterprise
args <- commandArgs(TRUE)
args <- unlist(strsplit(args," "))
# ep1.R 1.8.0r-55-g80f24f2-community 2.0.0r-452-gf1c60e1-community EPT-WRITE-original couchdb2.couchbaseqe.com eperf ept-write-nonjson-180-200r452
baseline_build = args[1]
new_build = args[2]
test_name = args[3]
dbip = args[4]
dbname = args[5]
run_id=args[6]
pdfname = args[7]

# baseline_build="1.8.0r-55-g80f24f2-enterprise"
# new_build = "1.8.1-852-rel-enterprise"
# test_name = "mixed-suv-4-10.loop"
# dbip = "couchdb2.couchbaseqe.com"
# dbname= "eperf"
# pdfname = "mixed-suv-4-10-1.8.0vs1.8.1-852"

if (is.na(pdfname) | length(pdfname) == 0) {
    if (is.na(run_id) | length(run_id) == 0) {
        run_id = "*"
    }
    pdfname = paste(test_name,
                    baseline_build,
                    new_build,
                    run_id,
                    format(Sys.time(), "%b-%d-%Y_%H:%M:%S"),
                    sep="_")
    print(paste("pdfname:", pdfname))
}

pdf(file=paste(pdfname,sep="",".pdf"),height=8,width=10,paper='USr')

i_builds = c(baseline_build, new_build)

cat(paste("args : ",args,""),sep="\n")

dumpTextFile <- function(filename=NULL) {
    # dump and plot text file as it is

    if (is.null(filename)) {
        return(NULL)
    }

    tryCatch({
        data <- readLines(filename, -1)
    }, error = function(e) {
        print(paste("unable to open file: ", filename))
        print(e)
        return(NULL)
    })

    if (is.null(data)  == FALSE) {
        simple_name = tail(unlist(strsplit(filename, "/")), 1)
        data = c(simple_name, unlist(data))
        textplot(data)
    }

    return(data)
}

buildPath <- function(name=NULL,
                      prefix="",
                      suffix="") {
    # build relative file path

    if (is.null(name)) {
        return(NULL)
    }

    path = paste(prefix,
                 unlist(strsplit(name, "\\."))[1],
                 suffix,
                 sep="")
    print(path)
    return(path)
}

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

makeMetricDef <- function(defText="unknown metric",
                          size=.7,
                          color="blue")
{
    pushViewport(viewport())
    grid.text(label=defText,
              x=unit(0.75, "npc"),
              y=unit(0.3, "npc"),
              gp=gpar(cex=size, col=color, fontface="italic"),
              check.overlap=TRUE,
              just=0)
    popViewport()
}

addopts <- function(aplot,atitle) {
	aplot <- aplot + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = ))
	aplot <- aplot + opts(axis.ticks = theme_segment(colour = 'red', size = 1))
	aplot <- aplot + opts(title=paste(atitle, sep=''))
	aplot <- aplot + scale_y_continuous(labels=commaize)
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
                p <- p + scale_y_continuous(labels="commaize",expand=c(0,0),limits = c(min(temp_data_frame$cpu_time_diff),quantile(temp_data_frame$cpu_time_diff,0.99)))
                p <- p + opts(panel.background = theme_rect(colour = 'black', fill = 'white', size = 1, linetype='solid'))
            p <- p + opts(axis.ticks = theme_segment(colour = 'red', size = 1, linetype = 'dashed'))
                p <- p + facet_wrap(~buildinfo.version, ncol=3, scales='free')
            print(p)
#    }
}

createProcessUsageDataFrame <- function(bb,process) {
 	(temp_data_frame <- bb[FALSE, ])
	builds = factor(bb$buildinfo.version)
	for(a_build in levels(builds)) {
			filtered <- bb[bb$buildinfo.version == a_build,]
			print(unique(filtered$unique_id))
			graphed <- bb[bb$buildinfo.version == a_build & bb$comm == process,]
            print(unique(graphed$unique_id))
			graphed <- transform(graphed,cpu_time = as.numeric(utime) + as.numeric(stime))
		    counterdiff <- diff(graphed$cpu_time)
			graphed[,"cpu_time_diff"] <- append(c(0), counterdiff)
			temp_data_frame <- rbind(temp_data_frame,  graphed)
	}
	temp_data_frame
}

builds_json <- fromJSON(file=paste("http://",dbip,":5984/",dbname,"/","/_design/data/_view/by_test_time", sep=''))$rows
builds_list <- plyr::ldply(builds_json, unlist)

names(builds_list) <- c('id', 'build', 'test_name', 'test_spec_name','runtime','is_json', 'reb_dur',  'testrunner', 'cluster_name', 'test_time')

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
builds_list <- builds_list[builds_list$build %in% i_builds & builds_list$test_name == test_name,]
print(builds_list)
# Following metrics are to be fetch from CouchDB and plotted
metric_list = c('ns_server_data', 'systemstats', 'latency-get','latency-set') #unused

# Get ns_server_data

cat("generating ns_server_data ")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ns_server_data", sep='')
    # cat(paste(url,"\n"))
    # cat(paste(builds_list[index,]$id,"\n"))
    tryCatch({
            doc_json <- fromJSON(file=url)
            unlisted <- plyr::ldply(doc_json, unlist)
        if (ncol(result) > 0 & ncol(result) != ncol(unlisted)) {
            #rbind.fill does not work if arg1 or arg2 is empty
            result <- rbind.fill(result,unlisted)
        } else {
            result <- rbind(result,unlisted)
        }
    }, error=function(e) {
        print("cannot generate ns server data")
        print(e)
    })
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
ns_server_data $vb_replica_resident_items_ratio <- as.numeric(ns_server_data $vb_replica_resident_items_ratio)
ns_server_data $vb_active_eject <- as.numeric(ns_server_data $vb_active_eject)
ns_server_data $vb_replica_eject <- as.numeric(ns_server_data $vb_replica_eject)
ns_server_data $ep_tap_replica_queue_backoff <- as.numeric(ns_server_data $ep_tap_replica_queue_backoff)
ns_server_data $mem_used <- as.numeric(ns_server_data$mem_used)
tryCatch({
    ns_server_data $docs_data_size <- as.numeric(ns_server_data$couch_docs_data_size)
    ns_server_data $docs_disk_size <- as.numeric(ns_server_data$couch_docs_disk_size)
    ns_server_data $docs_actual_disk_size <- as.numeric(ns_server_data$couch_docs_actual_disk_size)

    ns_server_data $views_data_size <- as.numeric(ns_server_data$couch_views_data_size)
    ns_server_data $views_disk_size <- as.numeric(ns_server_data$couch_views_disk_size)
    ns_server_data $views_actual_disk_size <- as.numeric(ns_server_data$couch_views_actual_disk_size)

    ns_server_data $total_disk_size <- as.numeric(ns_server_data$couch_total_disk_size)
}, error=function(e) {
    print("Cannot find detailed disk stats, they are only available in 2.0")
})
tryCatch({
    ns_server_data $docs_fragmentation <- as.numeric(ns_server_data$couch_docs_fragmentation)
    ns_server_data $views_fragmentation <- as.numeric(ns_server_data$couch_views_fragmentation)
}, error=function(e) {
    print("Cannot find fragmentation stats, they are only available in 2.0")
})

ns_server_data $curr_items <- as.numeric(ns_server_data$curr_items)
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

cat("generating memcached stats ")
memcached_stats <- data.frame()
for(index in 1:nrow(builds_list)) {
       tryCatch({
                url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","membasestats", sep='')
                cat(paste(url,"\n"))
                doc_json <- fromJSON(file=url)
				for(index in 1:length(doc_json)) {
					unlisted <- plyr::ldply(doc_json[index], unlist)
                    if(unlisted$ep_warmup_thread == "complete") {
                        unlisted <- unlisted[c('row', 'ep_warmup_thread', 'ip', 'ep_warmup_time', 'buildinfo.version')]
                        memcached_stats <- rbind.fill(memcached_stats, unlisted)
					}
				}
       },error=function(e) {
                print("Error getting system stats from memcached")
       })
}

memcached_stats$row <- as.numeric(memcached_stats$row)
memcached_stats$ep_warmup_time <- as.numeric(memcached_stats$ep_warmup_time)

# memcached_stats $ep_bg_wait_avg <- as.numeric(memcached_stats $ep_bg_wait_avg)
# memcached_stats $ep_tap_bg_fetched <- as.numeric(memcached_stats $ep_tap_bg_fetched)
# memcached_stats $ep_bg_max_load <- as.numeric(memcached_stats $ep_bg_max_load)
# memcached_stats $ep_bg_load <- as.numeric(memcached_stats $ep_bg_load)

cat("generating System stats from ns_server_data_system ")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ns_server_data_system", sep='')
        doc_json <- fromJSON(file=url)
        unlisted <- plyr::ldply(doc_json, unlist)
        result <- rbind(result,unlisted)
    }, error=function(e) e)
}
cat("generated ns_server_data_system data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))

ns_server_data_system <- result
ns_server_data_system $row <- as.numeric(ns_server_data_system $row)
ns_server_data_system $cpu_util <- as.numeric(ns_server_data_system $cpu_util)
# ns_server_data_system $swap_used <- as.numeric(ns_server_data_system $swap_used)

cat("generating system stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","systemstats", sep='')
	#cat(paste(url,"\n"))
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

# Get Latency-query
cat("generating latency-query\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-query", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-query"))
}
latency_query <- result
if (nrow(latency_query) > 0) {

    latency_query$row <- as.numeric(latency_query$row)
    latency_query$mystery <- as.numeric(latency_query$mystery)
    if (length(latency_query$percentile_999th)) {
        latency_query$percentile_999th <- as.numeric(latency_query$percentile_999th) * 1000
    }
    latency_query$percentile_99th <- as.numeric(latency_query$percentile_99th) * 1000
    latency_query$percentile_95th <- as.numeric(latency_query$percentile_95th) * 1000
    latency_query$percentile_90th <- as.numeric(latency_query$percentile_90th) * 1000
    if (length(latency_query$percentile_80th)) {
        latency_query$percentile_80th <- as.numeric(latency_query$percentile_80th) * 1000
    }

    all_builds = factor(latency_query$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_query[latency_query $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_query <- result
}

# Get average query throughput
cat("generating average query throughput\n")
avg_qps <- c()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","qps", sep='')
        doc_json <- fromJSON(file=url)
        avg_qps <- c(avg_qps, doc_json$buildinfo.version, doc_json$average)
    }, error=function(e) e)
}
if (length(c) > 0) {
    avg_qps <- array(avg_qps, c(2, nrow(builds_list)))
}

# Get query throughput
cat("generating query throughput\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ops", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	}, error=function(e) e)
}

throughput_query <- result
if (nrow(throughput_query) > 0) {
    throughput_query$row <- as.numeric(throughput_query$row)
    throughput_query$queries_per_sec <- as.numeric(throughput_query$queriesPerSec)
    throughput_query$buildinfo.version <- throughput_query$buildinfo.version
    throughput_query$timestamp <- as.numeric(throughput_query$startTime)

    all_builds = factor(throughput_query$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- throughput_query[throughput_query $buildinfo.version==a_build,]
	    tt$timestamp <- as.numeric(tt$timestamp)
	    min_timestamp = min(tt$timestamp)
	    filtered = transform(tt, row=timestamp-min_timestamp)
        result <- rbind(result, filtered)
    }
    throughput_query <- result
}

# Get Latency-get histogram
cat("generating latency-get histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-get-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-get histogram"))
}
latency_get_histo <- result
latency_get_histo$time <- as.numeric(latency_get_histo$time) * 1000
latency_get_histo$count <- as.numeric(latency_get_histo$count)


# Get Latency-set histogram
cat("generating latency-set histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-set-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-set histogram"))
}
latency_set_histo <- result
latency_set_histo$time <- as.numeric(latency_set_histo$time) * 1000
latency_set_histo$count <- as.numeric(latency_set_histo$count)

# Get Latency-query histogram
cat("generating latency-query histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-query-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-query histogram"))
}
latency_query_histo <- result
latency_query_histo$time <- as.numeric(latency_query_histo$time) * 1000
latency_query_histo$count <- as.numeric(latency_query_histo$count)

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
disk_data$row <- as.numeric(factor(disk_data$row)) * 10


builds_list$runtime = as.numeric(builds_list$runtime)
#baseline= c("1.7.2r-22-geaf53ef")
baseline <- baseline_build
first_row <- c("system","test","value")
combined <- data.frame(t(rep(NA,3)))
names(combined)<- c("system","test","value")
p <- combined[2:nrow(combined), ]

builds = factor(builds_list$build)
# Test runtime is present in builds_list
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

for(build in levels(builds)) {

    if (length(ns_server_data) == 0 | nrow(ns_server_data) == 0) {
        if(build == baseline){
            row <-c ("baseline", "drain rate", "NA")
        } else {
            row <-c (build, "drain rate", "NA")
        }
        combined <- rbind(combined, row)
        next;
    }

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

for(build in levels(builds)) {
    fi <-disk_data[disk_data$buildinfo.version==build, ]
    d <- max(fi$size)

    if (build == baseline) {
        row <-c ("baseline", "peak disk", as.numeric(d))
        combined <- rbind(combined, row)
    }
    else {
        row <-c (build, "peak disk", as.numeric(d))
        combined <- rbind(combined, row)
    }
}

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

footnote <- paste(builds_list$test_name, baseline_build, new_build, format(Sys.time(), "%d %b %Y"), sep=" / ")
p <- combined[2:nrow(combined), ]
p$value <- as.numeric(p$value)
df <- fixupData(buildComparison(p , 'system', 'baseline'))

if (length(unique(df$system)) == 2) {
   p <- ggplot(data=df[df$system != 'baseline',], aes(test, position, fill=color)) +
        geom_hline(yintercept=0, lwd=1, col='#777777') +
        geom_bar(stat='identity', position='dodge') +
        scale_fill_manual('Result', values=colors) +
        geom_hline(yintercept=.10, lty=3) +
        geom_hline(yintercept=-.10, lty=3) +
        scale_y_continuous(limits=c(-1 * (magnitude_limit - 1), magnitude_limit - 1),
                           labels=function(n) sprintf("%.1fx", abs(n) + 1)) +
        opts(title=paste(builds_list$test_name,':', baseline_build, ':', new_build )) +
        labs(y='(righter is better)', x='') +
        geom_text(aes(x=test, y=ifelse(abs(position) < .5, .5, sign(position) * -.5),
                  label=sprintf("%.02fx", abs(offpercent))),
                  size=4, colour="#999999") +
        coord_flip() +
        theme_bw()

    print(p)
    makeFootnote(footnote)
}

for(build in levels(builds)) {

    if (length(ns_server_data) == 0 | nrow(ns_server_data) == 0) {
        if(build == baseline){
            row <-c ("baseline", "ops", "NA")
        } else {
        row <-c (build, "ops", "NA")
        }

        combined <- rbind(combined, row)
        next;
    }

    fi <-ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$ops !=0, ]
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

for(build in levels(builds)) {

	fi <-latency_query[latency_query$buildinfo.version==build & latency_query$client_id ==0, ]
	d <- mean(fi$percentile_80th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-query (80th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-query (80th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
}

for(build in levels(builds)) {

	fi <-latency_query[latency_query$buildinfo.version==build & latency_query$client_id ==0, ]
	d <- mean(fi$percentile_90th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-query (90th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-query (90th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
}

for(build in levels(builds)) {

	fi <-latency_query[latency_query$buildinfo.version==build & latency_query$client_id ==0, ]
	d <- mean(fi$percentile_95th)

	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency_query (95th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency_query (95th)", as.numeric(d))
		combined <- rbind(combined, row)
	}

}

for(build in levels(builds)) {

	fi <-latency_query[latency_query$buildinfo.version==build & latency_query$client_id ==0, ]
	d1 <- mean(fi$percentile_99th)
	print(d1)
	if(build == baseline){
		row <-c ("baseline", "latency-query (99th)", as.numeric(d1))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency_query (99th)", as.numeric(d1))
		combined <- rbind(combined, row)
	}
}

for(build in levels(builds)) {

	fi <-latency_query[latency_query$buildinfo.version==build & latency_query$client_id ==0, ]
	d <- mean(fi$percentile_999th)
	print(d)
	if(build == baseline){
		row <-c ("baseline", "latency-query (99.9th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "latency-query (99.9th)", as.numeric(d))
		combined <- rbind(combined, row)
	}
}

if(ncol(avg_qps) > 0) {
    for(i in 1:ncol(avg_qps)) {
        build <- avg_qps[1, i]
        average <- as.numeric(avg_qps[2, i])

        if(build == baseline){
            row <- c("baseline", "query throughput", average)
            combined <- rbind(combined, row)
        }
        else{
            row <- c(build, "query throughput", average)
            combined <- rbind(combined, row)
        }
    }
}

for(build in levels(builds)) {
    fi <-builds_list[builds_list$build==build, ]
    d <- fi$reb_dur

    if(build == baseline){
        row <-c ("baseline", "reb duration", as.numeric(d))
        combined <- rbind(combined, row)
    }
    else{
        row <-c (build, "reb duration", as.numeric(d))
        combined <- rbind(combined, row)
    }
}

for(build in levels(builds)) {
    fi <-builds_list[builds_list$build==build, ]
    d <- fi$testrunner
    print(d)
    if(build == baseline){
        row <-c ("baseline", "testrunner version", d)
        combined <- rbind(combined, row)
    }
    else{
        row <-c (build, "testrunner version", d)
        combined <- rbind(combined, row)
    }
}

for(build in levels(builds)) {
    fi <- memcached_stats[memcached_stats$buildinfo.version == build, ]
	d <- fi$ep_warmup_time[1] / 1000000

    if(build == baseline){
		row <-c ("baseline", "warmup time (with os flush)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "warmup time (with os flush)", as.numeric(d))
		combined <- rbind(combined, row)
	}
}

for(build in levels(builds)) {
    fi <- memcached_stats[memcached_stats$buildinfo.version == build, ]
	d <- fi$ep_warmup_time[2] / 1000000

    if(build == baseline){
		row <-c ("baseline", "warmup time (no os flush)", as.numeric(d))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "warmup time (no os flush)", as.numeric(d))
		combined <- rbind(combined, row)
	}
}

# TODO rewrite (benchmark table)
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
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[14])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[15])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[16])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[17])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[18])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[19])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[20])))
MB <- append(MB,combined[combined[,'system'] == 'baseline',]$value[22])

if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
    MB <- append(MB,as.numeric(sprintf("%.2f",MB1[22])))
    MB <- append(MB,as.numeric(sprintf("%.2f",MB1[23])))
}

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
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[14])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[15])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[16])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[17])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[18])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[19])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[20])))
CB <- append(CB,combined[combined[,'system'] != 'baseline',]$value[22])

if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
    CB <- append(CB,as.numeric(sprintf("%.2f",CB1[22])))
    CB <- append(CB,as.numeric(sprintf("%.2f",CB1[23])))
}

testdf <- data.frame(MB,CB)

row_names <- c("Runtime (in hr)","Avg. Drain Rate","Peak Disk (GB)","Peak Memory (GB)", "Avg. OPS", "Avg. mem memcached (GB)", "Avg. mem beam.smp (MB)","Latency-get (90th) (ms)", "Latency-get (95th) (ms)","Latency-get (99th) (ms)","Latency-set (90th) (ms)","Latency-set (95th) (ms)","Latency-set (99th) (ms)","Latency-query (80th) (ms)","Latency-query (90th) (ms)","Latency-query (95th) (ms)","Latency-query (99th) (ms)", "Latency-query (99.9th) (ms)", "Avg. QPS", "Rebalance Time (sec)", "Testrunner Version")

if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
	row_names <- c(row_names, c("Warmup Time - flush (sec)", "Warmup Time - no flush (sec)"))
}

rownames(testdf) <- row_names
plot.new()
col1 <- paste(unlist(strsplit(baseline_build, "-"))[1],"-",unlist(strsplit(baseline_build, "-"))[2])
col2 <- paste(unlist(strsplit(new_build, "-"))[1],"-",unlist(strsplit(new_build, "-"))[2])
grid.table(testdf, h.even.alpha=1, h.odd.alpha=1,  v.even.alpha=0.5, v.odd.alpha=1,cols=c(col1, col2))
makeFootnote(footnote)

if (nrow(ns_server_data) > 0) {

    cat("generating ops/sec \n")
    p <- ggplot(ns_server_data, aes(row, ops, color=buildinfo.version , label= prettySize(ops))) + labs(x="----time (sec)--->", y="ops/sec")
    p <- p + geom_point()
    p <- addopts(p,"ops/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of ops per second")

    cat("generating ep queue size \n")
    p <- ggplot(ns_server_data, aes(row, ep_queue_size, color=buildinfo.version , label= prettySize(ep_queue_size))) + labs(x="----time (sec)--->", y="ep_queue_size")
    p <- p + geom_point()
    p <- addopts(p,"ep queue size")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of items queued for storage")

    cat("generating ep_diskqueue_drain \n")
    p <- ggplot(ns_server_data, aes(row, ep_diskqueue_drain, color=buildinfo.version , label= prettySize(ep_diskqueue_drain))) + labs(x="----time (sec)--->", y="ep_diskqueue_drain")
    p <- p + geom_point()
    p <- addopts(p,"ep_diskqueue_drain")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Total drained items on disk queue")

    cat("generating ep_bg_fetched \n")
    p <- ggplot(ns_server_data, aes(row, ep_bg_fetched, color=buildinfo.version , label= prettySize(ep_bg_fetched))) + labs(x="----time (sec)--->", y="ep_bg_fetched")
    p <- p + geom_point()
    p <- addopts(p,"ep_bg_fetched ops/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of items fetched from disk")

    cat("generating tmp_oom \n")
    p <- ggplot(ns_server_data, aes(row, ep_tmp_oom_errors, color=buildinfo.version , label= prettySize(ep_tmp_oom_errors))) + labs(x="----time (sec)--->", y="tmp_oom")
    p <- p + geom_point()
    p <- addopts(p,"tmp_oom ops/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of times temporary OOMs")

    ns_server_data $vb_replica_eject <- as.numeric(ns_server_data $vb_replica_eject)
    ns_server_data $ep_tap_replica_queue_backoff <- as.numeric(ns_server_data $ep_tap_replica_queue_backoff)

    cat("generating vb_active_eject \n")
    p <- ggplot(ns_server_data, aes(row, vb_active_eject, color=buildinfo.version , label= prettySize(vb_active_eject))) + labs(x="----time (sec)--->", y="vb_active_eject")
    p <- p + geom_point()
    p <- addopts(p,"vb_active_eject/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of times item values got ejected")

    cat("generating vb_replica_eject \n")
    p <- ggplot(ns_server_data, aes(row, vb_replica_eject, color=buildinfo.version , label= prettySize(vb_replica_eject))) + labs(x="----time (sec)--->", y="vb_replica_eject")
    p <- p + geom_point()
    p <- addopts(p,"vb_replica_eject/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of times item values got ejected")

    cat("generating ep_tap_replica_queue_backoff \n")
    p <- ggplot(ns_server_data, aes(row, ep_tap_replica_queue_backoff, color=buildinfo.version , label= prettySize(ep_tap_replica_queue_backoff))) + labs(x="----time (sec)--->", y="ep_tap_replica_queue_backoff")
    p <- p + geom_point()
    p <- addopts(p,"ep_tap_replica_queue_backoff/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Number of back-offs received per second ",
                        "while sending data over replication ",
                         "TAP connections",
                         sep="\n"))

    cat("generating vb_active_resident_items_ratio \n")
    ns_server_data_filtered = ns_server_data[ns_server_data$vb_active_resident_items_ratio > 0, ]
    p <- ggplot(ns_server_data_filtered, aes(row, vb_active_resident_items_ratio, color=buildinfo.version , label= prettySize(vb_active_resident_items_ratio))) + labs(x="----time (sec)--->", y="vb_active_resident_items_ratio")
    p <- p + geom_point()
    p <- addopts(p,"vb_active_resident_items_ratio")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Percentage of active items cached in RAM")

    cat("generating vb_replica_resident_items_ratio \n")
    p <- ggplot(ns_server_data, aes(row, vb_replica_resident_items_ratio, color=buildinfo.version , label= prettySize(vb_replica_resident_items_ratio))) + labs(x="----time (sec)--->", y="vb_replica_resident_items_ratio")
    p <- p + geom_point()
    p <- addopts(p,"vb_replica_resident_items_ratio")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Percentage of replica items cached in RAM")

    cat("generating curr_items \n")
    p <- ggplot(ns_server_data, aes(row, curr_items, color=buildinfo.version , label= prettySize(curr_items))) + labs(x="----time (sec)--->", y="curr_items")
    p <- p + geom_point()
    p <- addopts(p,"curr_items")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of items in bucket")

    cat("generating cur_items_total \n")
    p <- ggplot(ns_server_data, aes(row, curr_items_tot, color=buildinfo.version , label= prettySize(curr_items_tot))) + labs(x="----time (sec)--->", y="curr_items_tot")
    p <- p + geom_point()
    p <- addopts(p,"cur_items_total")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Total number of items")

    cat("generating mem_used \n")
    p <- ggplot(ns_server_data, aes(row, mem_used, color=buildinfo.version , label= mem_used)) + labs(x="----time (sec)--->", y="bytes")
    p <- p + geom_point()
    p <- addopts(p,"mem_used")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Engine's total memory usage")

    if(!is.null(ns_server_data$total_disk_size)) {
        cat("generating couch_data_size \n")
        p <- ggplot(ns_server_data, aes(row, docs_data_size, color=buildinfo.version , label=docs_data_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Docs data size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of active data in bucket (2.0 only)",
                            sep="\n"))

        cat("generating couch_docs_disk_size \n")
        p <- ggplot(ns_server_data, aes(row, docs_disk_size, color=buildinfo.version , label=docs_disk_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Docs disk size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of all data files for this bucket, ",
                            "including the data itself, meta data ",
                            "and temporary files (2.0 only)",
                            sep="\n"))

        cat("generating couch_docs_actual_disk_size \n")
        p <- ggplot(ns_server_data, aes(row, docs_actual_disk_size, color=buildinfo.version , label=docs_actual_disk_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Docs actual disk size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of all data files for this bucket, ",
                            "including the data itself, meta data ",
                            "and temporary files (2.0 only)",
                            sep="\n"))

        cat("generating couch_views_data_size \n")
        p <- ggplot(ns_server_data, aes(row, views_data_size, color=buildinfo.version , label=views_data_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Views data size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of all active data on for all the",
                            "indexes in this bucket (2.0 only)",
                            sep="\n"))

        cat("generating couch_views_disk_size \n")
        p <- ggplot(ns_server_data, aes(row, views_disk_size, color=buildinfo.version , label=views_disk_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Views disk size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of all active items ",
                            "in all the indexes on disk (2.0 only)",
                            sep="\n"))

        cat("generating couch_views_actual_disk_size \n")
        p <- ggplot(ns_server_data, aes(row, views_actual_disk_size, color=buildinfo.version , label=views_actual_disk_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Views actual disk size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The size of all active items ",
                            "in all the indexes on disk (2.0 only)",
                            sep="\n"))

        cat("generating couch_total_disk_size \n")
        p <- ggplot(ns_server_data, aes(row, total_disk_size, color=buildinfo.version , label=total_disk_size)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Total disk size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The total size on disk of all data ",
                            "and view files. (2.0 only)",
                            sep="\n"))
    }

    if(!is.null(ns_server_data$couch_views_fragmentation)) {
        cat("generating couch_docs_fragmentation \n")
        p <- ggplot(ns_server_data, aes(row, docs_fragmentation, color=buildinfo.version, label=docs_fragmentation)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Docs fragmentation")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("How much fragmented data there is to be ",
                            "compacted compared to real data for the ",
                            "data files in this bucket (2.0 only)",
                            sep="\n"))

        cat("generating couch_views_fragmentation \n")
        p <- ggplot(ns_server_data, aes(row, views_fragmentation, color=buildinfo.version, label=views_fragmentation)) + labs(x="----time (sec)--->", y="Bytes")
        p <- p + geom_point()
        p <- addopts(p,"Views fragmentation")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("How much fragmented data there is to be ",
                            "compacted compared to real data for the ",
                            "view index files in this bucket (2.0 only)",
                            sep="\n"))
    }

    cat("generating cmd_get \n")
    p <- ggplot(ns_server_data, aes(row, cmd_get, color=buildinfo.version , label= cmd_get)) + labs(x="----time (sec)--->", y="ops/sec")
    p <- p + geom_point()
    p <- addopts(p,"cmd_get ops/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Cumulative number of retrieval reqs")

    cat("generating cmd_set \n")
    p <- ggplot(ns_server_data, aes(row, cmd_set, color=buildinfo.version , label= cmd_set)) + labs(x="----time (sec)--->", y="ops/sec")
    p <- p + geom_point()
    p <- addopts(p,"cmd_set ops/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Cumulative number of set reqs")

    cat("generating get misses \n")
    p <- ggplot(ns_server_data, aes(row, get_misses, color=buildinfo.version , label= get_misses)) + labs(x="----time (sec)--->", y="# of get misses")
    p <- p + geom_point()
    p <- addopts(p,"# of get misses")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Number of get operations per second ",
                        "for data that the bucket does not contain",
                        sep="\n"))

    cat("generating get hits \n")
    p <- ggplot(ns_server_data, aes(row, get_hits, color=buildinfo.version , label= get_hits)) + labs(x="----time (sec)--->", y="# of get hits")
    p <- p + geom_point()
    p <- addopts(p,"# of get hits")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Number of get operations per second ",
                        "for data that the bucket contains",
                        sep="\n"))

    cat("generating cache_miss \n")
    p <- ggplot(ns_server_data, aes(row, cache_miss, color=buildinfo.version , label= cache_miss)) + labs(x="----time (sec)--->", y="cache_miss percentage")
    p <- p + geom_point()
    p <- addopts(p,"cache_miss percentage")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Percentage of reads per second ",
                        "from disk as opposed to RAM",
                        sep="\n"))

    cat("generating cache_miss ( between 0-5 ) \n")
    ns_server_data$cache_miss_0_5 <- ifelse(ns_server_data $cache_miss>5,5, ns_server_data$cache_miss)
    p <- ggplot(ns_server_data, aes(row, cache_miss_0_5, color=buildinfo.version , label= cache_miss_0_5)) + labs(x="----time (sec)--->", y="cache_miss percentage")
    p <- p + geom_point()
    p <- addopts(p,"cache_miss percentage 0-5")
    print(p)
    makeFootnote(footnote)
}

cat("generating cpu_util \n")
nodes = factor(ns_server_data_system$node)
for(ns_node in levels(nodes)) {
    node_system_stats = subset(ns_server_data_system, node==ns_node)
    p <- ggplot(node_system_stats, aes(row, cpu_util, color=buildinfo.version, label=cpu_util))
    p <- p + labs(x="----time (sec)--->", y="%")
    p <- p + coord_cartesian(ylim = c(0, 100))
    p <- p + geom_point()
    p <- addopts(p, paste("CPU utilization", ns_node, sep=" - "))
    print(p)
    makeFootnote(footnote)
}


cat("Generating Memory/CPU usage for beam.smp and memcached\n")

for(ip in levels(factor(system_stats$ip))) {

    node_sys_stats = system_stats[system_stats$ip == ip, ]

    beam_temp_data_frame  = createProcessUsageDataFrame(node_sys_stats, "(beam.smp)")
    mc_temp_data_frame  = createProcessUsageDataFrame(node_sys_stats, "(memcached)")

    cat("generating beam cpu ticks \n")
    p <- ggplot(mc_temp_data_frame, aes(row, cpu_time_diff, color=buildinfo.version , label= prettySize(cpu_time_diff))) + labs(x="----time (sec)--->", y="cpu_time_diff")
    p <- p + geom_line()
    p <- addopts(p, paste("cpu_time_diff: memcached - ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("CPU tick differences (jiffies) ",
                        "among snapshots across time",
                        sep="\n"))

    cat("generating memcached cpu ticks \n")
    p <- ggplot(beam_temp_data_frame, aes(row, cpu_time_diff, color=buildinfo.version , label= prettySize(cpu_time_diff))) + labs(x="----time (sec)--->", y="cpu_time_diff")
    p <- p + geom_point()
    p <- addopts(p, paste("cpu_time_diff : beam.smp - ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("CPU tick differences (jiffies) ",
                        "among snapshots across time",
                        sep="\n"))

}

cat("generating data disk size\n")
p <- ggplot(disk_data, aes(row, size, color=buildinfo.version, label=size)) + labs(x="----time (sec)--->", y="MB")
p <- p + geom_point()
p <- addopts(p,"Data disk size")
print(p)
makeFootnote(footnote)

if (nrow(latency_get_histo) > 0) {
    cat("plotting latency-get histogram \n")
    p <- ggplot(latency_get_histo, aes(time, count, color=buildinfo.version , label= prettySize(count))) + labs(x="----latency (ms)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency get histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for get commands")

    cat("plotting latency-get histogram (0-10ms) \n")
    latency_get_histo = latency_get_histo[latency_get_histo$time < 10, ]
    p <- ggplot(latency_get_histo, aes(time, count, color=buildinfo.version , label= prettySize(count))) + labs(x="----latency (ms)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency get histogram (0-10 ms)")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_set_histo) > 0) {
    cat("plotting latency-set histogram \n")
    p <- ggplot(latency_set_histo, aes(time, count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (ms)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency set histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for set commands")

    cat("plotting latency-set histogram (0-10ms) \n")
    latency_set_histo = latency_set_histo[latency_set_histo$time < 10, ]
    p <- ggplot(latency_set_histo, aes(time, count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (ms)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency set histogram (0-10 ms)")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_query_histo) > 0) {
    cat("plotting latency-query histogram \n")
    p <- ggplot(latency_query_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (ms)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency query histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for query commands")
}

if (nrow(latency_get) > 0) {
    cat("Latency-get 90th\n")
    temp <- latency_get[latency_get$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_90th, linetype=buildinfo.version)) + labs(x="----time (sec)--->", y="ms")
    #p  <-  p + stat_smooth(se = TRUE)
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-get 90th (0 - 10ms) \n")
    temp <- latency_get[latency_get$client_id ==0,]
    temp$percentile_90th_0_10 <- ifelse(temp$percentile_90th > 10, 10, temp$percentile_90th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_90th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 90th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-get 95th\n")
    temp <- latency_get[latency_get$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-get 95th (0 - 10ms) \n")
    temp <- latency_get[latency_get$client_id ==0,]
    temp$percentile_95th_0_10 <- ifelse(temp$percentile_95th > 10, 10, temp$percentile_95th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_95th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 95th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-get 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 99th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-get 99th (0 - 10ms) \n")
    temp <- latency_get[latency_get$client_id ==0,]
    temp$percentile_99th_0_10 <- ifelse(temp$percentile_99th > 10, 10, temp$percentile_99th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_99th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-get 99th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_set) > 0) {

    cat("Latency-set 90th\n")
    temp <- latency_set[latency_set$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-set 90th (0 - 10ms) \n")
    temp <- latency_set[latency_set$client_id ==0,]
    temp$percentile_90th_0_10 <- ifelse(temp$percentile_90th > 10, 10, temp$percentile_90th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_90th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 90th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-set 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-set 95th (0 - 10ms) \n")
    temp <- latency_set[latency_set$client_id ==0,]
    temp$percentile_95th_0_10 <- ifelse(temp$percentile_95th > 10, 10, temp$percentile_95th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_95th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 95th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-set 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 99th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-set 99th (0 - 10ms) \n")
    temp <- latency_set[latency_set$client_id ==0,]
    temp$percentile_99th_0_10 <- ifelse(temp$percentile_99th > 10, 10, temp$percentile_99th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_99th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-set 99th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_query) > 0) {

    if (length(latency_query$percentile_80th)) {
        cat("Latency-query 80th\n")
        temp <- latency_query[latency_query$client_id ==0,]
        p <- ggplot(temp, aes(temp$row, temp$percentile_80th, color=buildinfo.version, label=temp$percentile_80th)) + labs(x="----time (sec)--->", y="ms")
        p <- p + geom_point()
        p <- addopts(p,"Latency-query 80th  percentile")
        print(p)
        makeFootnote(footnote)

        cat("Latency-query 80th (0 - 10ms) \n")
        temp <- latency_query[latency_query$client_id ==0,]
        temp$percentile_80th_0_10 <- ifelse(temp$percentile_80th > 10, 10, temp$percentile_80th)
        p <- ggplot(temp, aes(temp$row, temp$percentile_80th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_80th_0_10, linetype=buildinfo.version))
        p <- p + labs(x="----time (sec)--->", y="ms")
        p <- p + geom_point()
        p <- addopts(p,"Latency-query 80th  percentile (0 - 10ms)")
        print(p)
        makeFootnote(footnote)
    }

    cat("Latency-query 90th\n")
    temp <- latency_query[latency_query$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-query 90th (0 - 10ms) \n")
    temp <- latency_query[latency_query$client_id ==0,]
    temp$percentile_90th_0_10 <- ifelse(temp$percentile_90th > 10, 10, temp$percentile_90th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_90th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 90th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-query 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-query 95th (0 - 10ms) \n")
    temp <- latency_query[latency_query$client_id ==0,]
    temp$percentile_95th_0_10 <- ifelse(temp$percentile_95th > 10, 10, temp$percentile_95th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_95th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 95th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    cat("Latency-query 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 99th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-query 99th (0 - 10ms) \n")
    temp <- latency_query[latency_query$client_id ==0,]
    temp$percentile_99th_0_10 <- ifelse(temp$percentile_99th > 10, 10, temp$percentile_99th)
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_99th_0_10, linetype=buildinfo.version))
    p <- p + labs(x="----time (sec)--->", y="ms")
    p <- p + geom_point()
    p <- addopts(p,"Latency-query 99th  percentile (0 - 10ms)")
    print(p)
    makeFootnote(footnote)

    if (length(latency_query$percentile_999th)) {
        cat("Latency-query 99.9th\n")
        temp <- latency_query[latency_query$client_id ==0,]
        p <- ggplot(temp, aes(temp$row, temp$percentile_999th, color=buildinfo.version, label=temp$percentile_999th)) + labs(x="----time (sec)--->", y="ms")
        p <- p + geom_point()
        p <- addopts(p,"Latency-query 99.9th  percentile")
        print(p)
        makeFootnote(footnote)

        cat("Latency-query 99.9th (0 - 10ms) \n")
        temp <- latency_query[latency_query$client_id ==0,]
        temp$percentile_999th_0_10 <- ifelse(temp$percentile_999th > 10, 10, temp$percentile_999th)
        p <- ggplot(temp, aes(temp$row, temp$percentile_999th_0_10, color=buildinfo.version ,fill= buildinfo.version, label=temp$percentile_999th_0_10, linetype=buildinfo.version))
        p <- p + labs(x="----time (sec)--->", y="ms")
        p <- p + geom_point()
        p <- addopts(p,"Latency-query 99.9th  percentile (0 - 10ms)")
        print(p)
        makeFootnote(footnote)
    }

}

if (nrow(throughput_query) > 0) {
    if (length(throughput_query$queries_per_sec)) {
        cat("query throughput\n")
        temp <- throughput_query[, ]
        p <- ggplot(temp, aes(temp$row, temp$queries_per_sec, color=buildinfo.version, label=temp$queries_per_sec)) + labs(x="----time (sec)--->", y="Queries/sec")
        p <- p + geom_point()
        p <- addopts(p,"Query throughput")
        print(p)
        makeFootnote(footnote)
    }
}

# cat("generating swap_used \n")
# p <- ggplot(ns_server_data_system, aes(row, swap_used, color=buildinfo.version , label= swap_used)) + labs(x="----time (sec)--->", y="swap_used")
# p <- p + geom_point()
# p <- addopts(p,"swap_used")
# print(p)
# makeFootnote(footnote)


#memcached stats

 # cat("generating ep_bg_wait_avg \n")
 # p <- ggplot(memcached_stats, aes(row, ep_bg_wait_avg, color=buildinfo.version , label= ep_bg_wait_avg)) + labs(x="----time (sec)--->", y="ep_bg_wait_avg")
 # p <- p + geom_point()
 # p <- addopts(p,"ep_bg_wait_avg")
 # print(p)
 # makeFootnote(footnote)
# cat("generating ep_tap_bg_fetched \n")
# p <- ggplot(memcached_stats, aes(row, ep_tap_bg_fetched, color=buildinfo.version , label= ep_tap_bg_fetched)) + labs(x="----time (sec)--->", y="ep_tap_bg_fetched")
# p <- p + geom_point()
# p <- addopts(p,"ep_tap_bg_fetched")
# print(p)
# makeFootnote(footnote)
# cat("generating ep_bg_max_load \n")
# p <- ggplot(memcached_stats, aes(row, ep_bg_max_load, color=buildinfo.version , label= ep_bg_max_load)) + labs(x="----time (sec)--->", y="ep_bg_max_load")
# p <- p + geom_point()
# p <- addopts(p,"ep_bg_max_load")
# print(p)
# makeFootnote(footnote)
# cat("generating ep_bg_load \n")
# p <- ggplot(memcached_stats, aes(row, ep_bg_load, color=buildinfo.version , label= ep_bg_load)) + labs(x="----time (sec)--->", y="ep_bg_load")
# p <- p + geom_point()
# p <- addopts(p,"ep_bg_load")
# print(p)
# makeFootnote(footnote)

# beam <- system_stats[system_stats$comm == "(beam.smp)",]
# cat("generating beam.smp mem \n")
# p <- ggplot(beam, aes(row, rss, color=buildinfo.version , label= prettySize(rss))) + labs(x="----time (sec)--->", y="rss")
# p <- p + geom_point()
# p <- addopts(p,"rss")
# print(p)
# makeFootnote(footnote)

# memcached <- system_stats[system_stats$comm == "(memcached)",]
# cat("generating (memcached) mem \n")
# p <- ggplot(memcached, aes(row, rss, color=buildinfo.version , label= prettySize(rss))) + labs(x="----time (sec)--->", y="rss")
# p <- p + geom_point()
# p <- addopts(p,"rss")
# print(p)
# makeFootnote(footnote)

conf_file = buildPath(test_name, "../../conf/perf/", ".conf")
dumpTextFile(conf_file)

if (builds_list$cluster_name != "") {
    ini_file = buildPath(builds_list$cluster_name, "../../resources/perf/", ".ini")
    dumpTextFile(ini_file)
}

dev.off()

