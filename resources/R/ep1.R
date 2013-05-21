source("resources/R/executive.R")

require(reshape, quietly=TRUE)
require(plyr, quietly=TRUE)
require(rjson, quietly=TRUE)
require(ggplot2, quietly=TRUE)
require(gridExtra, quietly=TRUE)
require(gplots, quietly=TRUE)

library(plotrix)
library(methods)
library(gtools)

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
# dbip = "10.5.2.41"
# dbname= "eperf"
# pdfname = "mixed-suv-4-10-1.8.0vs1.8.1-852"

if (is.na(run_id) | length(run_id) == 0) {
        run_id = "*"
}

if (unlist(strsplit(run_id, "-"))[1] == "reb") {
    reb = TRUE
} else {
    reb = FALSE
}

if (is.na(pdfname) | length(pdfname) == 0) {
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
    f <- ifelse(s < 1, NA, e <- floor(log(s, 1024)))
    suffix <- ifelse(s < 1, '', sizes[f+1])
    prefix <- ifelse(s < 1, s, sprintf(fmt, s/(1024 ^ floor(e))))
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

rebFilter <- function(data, builds_list) {
    # filter data to only include results during rebalance

    if (length(data) == 0) {
        print("cannot apply rebalance filter: invalid data frame")
        return
    }

    if (length(builds_list) == 0) {
        print("cannot apply rebalance filter: invalid build list")
        return
    }

    temp_data_frame <- data.frame()
    for (index in 1:nrow(builds_list)) {
        build = builds_list[index,]
        temp_data_frame <- rbind(temp_data_frame,
                                 data[data$buildinfo.version == build$build
                                      & data$row < build$reb_end
                                      & data$row > build$reb_start,])
    }

    return(temp_data_frame)
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

names(builds_list) <- c('id', 'build', 'test_name', 'test_spec_name','runtime','is_json', 'reb_start', 'reb_dur',  'testrunner', 'cluster_name', 'test_time')

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

# Get relative rebalance start time
builds_list$reb_start = as.numeric(builds_list$reb_start) - as.numeric(builds_list$test_time)
builds_list$reb_end = builds_list$reb_start + as.numeric(builds_list$reb_dur)

# Get ns_server_data

cat("generating ns_server_data ")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","ns_server_data", sep='')
    tryCatch({
        doc_json <- fromJSON(file=url)
        unlisted <- plyr::ldply(doc_json, unlist)
        if (ncol(result) == 0) {
            result <- rbind(result,unlisted)
        } else {
            if (ncol(result) != ncol(unlisted)) {
                result <- rbind.fill(result,unlisted)
            } else {
                result <- smartbind(result, unlisted)
            }
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
tryCatch({
    ns_server_data $views_ops <- as.numeric(ns_server_data$couch_views_ops)
}, error=function(e) {
    print("Cannot find view read per sec.")
})
ns_server_data $ep_bg_fetched <- as.numeric(ns_server_data $ep_bg_fetched)
ns_server_data $ep_tmp_oom_errors <- as.numeric(ns_server_data $ep_tmp_oom_errors)
ns_server_data $vb_active_resident_items_ratio <- as.numeric(ns_server_data $vb_active_resident_items_ratio)
ns_server_data $vb_replica_resident_items_ratio <- as.numeric(ns_server_data $vb_replica_resident_items_ratio)
ns_server_data $vb_active_eject <- as.numeric(ns_server_data $vb_active_eject)
ns_server_data $vb_replica_eject <- as.numeric(ns_server_data $vb_replica_eject)
ns_server_data $ep_tap_replica_queue_drain <- as.numeric(ns_server_data$ep_tap_replica_queue_drain)
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
ns_server_data$curr_connections <- as.numeric(ns_server_data$curr_connections)
tryCatch({
    ns_server_data $xdc_ops <- as.numeric(ns_server_data$xdc_ops)
    ns_server_data $ep_num_ops_get_meta <- as.numeric(ns_server_data$ep_num_ops_get_meta)
    ns_server_data $ep_num_ops_set_meta <- as.numeric(ns_server_data$ep_num_ops_set_meta)
    ns_server_data $ep_num_ops_del_meta <- as.numeric(ns_server_data$ep_num_ops_del_meta)
}, error=function(e) {
    print("Cannot find incoming XDCR stats")
})
tryCatch({
    ns_server_data $replication_changes_left <- as.numeric(ns_server_data$replication_changes_left)
    ns_server_data $replication_docs_rep_queue <- as.numeric(ns_server_data$replication_docs_rep_queue)
    ns_server_data $replication_size_rep_queue <- as.numeric(ns_server_data$replication_size_rep_queue)

    ns_server_data $replication_docs_checked <- as.numeric(ns_server_data$replication_docs_checked)
    ns_server_data $replication_docs_written <- as.numeric(ns_server_data$replication_docs_written)
    ns_server_data $replication_data_replicated <- as.numeric(ns_server_data$replication_data_replicated)

    ns_server_data $replication_commit_time <- as.numeric(ns_server_data$replication_commit_time)
    ns_server_data $replication_work_time <- as.numeric(ns_server_data$replication_work_time)

    ns_server_data $replication_active_vbreps <- as.numeric(ns_server_data$replication_active_vbreps)
    ns_server_data $replication_waiting_vbreps <- as.numeric(ns_server_data$replication_waiting_vbreps)

    ns_server_data $replication_num_checkpoints <- as.numeric(ns_server_data$replication_num_checkpoints)
    ns_server_data $replication_num_failedckpts <- as.numeric(ns_server_data$replication_num_failedckpts)
}, error=function(e) {
    print("Cannot find outbound XDCR stats")
})
tryCatch({
    ns_server_data $replication_rate_replication <- as.numeric(ns_server_data$replication_rate_replication)
    ns_server_data $replication_bandwidth_usage <- as.numeric(ns_server_data$replication_bandwidth_usage)

    ns_server_data $replication_docs_latency_wt <- as.numeric(ns_server_data$replication_docs_latency_wt)
    ns_server_data $replication_meta_latency_wt <- as.numeric(ns_server_data$replication_meta_latency_wt)
}, error=function(e) {
    print("Cannot find 2.0.2 XDCR stats")
})

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
#warmup_stats <- data.frame()
memcached_stats <- data.frame()
for(index in 1:nrow(builds_list)) {
       tryCatch({
                url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","membasestats", sep='')
                cat(paste(url,"\n"))
                doc_json <- fromJSON(file=url)
				for(index in 1:length(doc_json)) {
					unlisted <- plyr::ldply(doc_json[index], unlist)
					memcached_stats <- rbind.fill(memcached_stats, unlisted)
                    #if(unlisted$ep_warmup_thread == "complete") {
                    #    unlisted <- unlisted[c('row', 'ep_warmup_thread', 'ip', 'ep_warmup_time', 'buildinfo.version')]
                    #    warmup_stats <- rbind.fill(warmup_stats, unlisted)
					#}
				}
       },error=function(e) {
                print("Error getting system stats from memcached")
       })
}

#warmup_stats$row <- as.numeric(warmup_stats$row)
#warmup_stats$ep_warmup_time <- as.numeric(memcached_stats$ep_warmup_time)
memcached_stats$row <- as.numeric(memcached_stats$row)
memcached_stats$ep_diskqueue_drain <- as.numeric(memcached_stats$ep_diskqueue_drain)
memcached_stats$uptime <- as.numeric(memcached_stats$uptime)

if (!is.null(memcached_stats$ep_bg_wait_avg)) {
    memcached_stats$ep_bg_wait_avg <- as.numeric(memcached_stats$ep_bg_wait_avg) / 1000
}
if (!is.null(memcached_stats$ep_bg_load_avg)) {
    memcached_stats$ep_bg_load_avg <- as.numeric(memcached_stats$ep_bg_load_avg) / 1000
}
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
ns_server_data_system $swap_used <- as.numeric(ns_server_data_system $swap_used)

cat("generating system stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","systemstats", sep='')
        #cat(paste(url,"\n"))
        doc_json <- fromJSON(file=url)
        # cat(paste(builds_list[index,]$id,"\n"))
        unlisted <- plyr::ldply(doc_json, unlist)
        print(ncol(unlisted))
        result <- rbind(result,unlisted)
    }, error=function(e) e)
}
cat("generated system stats data frame\n")
cat(paste("result has ", nrow(result)," rows \n"))
system_stats <- result
if (nrow(system_stats)) {
    system_stats = transform(system_stats,utime=as.numeric(utime),stime=as.numeric(stime),rss=(as.numeric(rss) * 4096) / (1024 * 1024))
    system_stats$row <- as.numeric(factor(system_stats$row))
    system_stats$nswap <- as.numeric(system_stats$nswap)
    system_stats$cnswap <- as.numeric(system_stats$cnswap)
}

cat("generating io stats \n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","iostats", sep='')
        doc_json <- fromJSON(file=url)
        unlisted <- plyr::ldply(doc_json, unlist)
        print(ncol(unlisted))
        result <- rbind(result,unlisted)
    }, error=function(e) e)
}
cat(paste("result has ", nrow(result)," rows \n"))
iostats <- result
if (nrow(iostats)) {
    iostats$row <- as.numeric(factor(iostats$row))
    iostats$time <- as.numeric(iostats$time)
    iostats$read <- as.numeric(iostats$read)
    iostats$write <- as.numeric(iostats$write)
    iostats$util <- as.numeric(iostats$util)
    iostats$iowait <- as.numeric(iostats$iowait)
    iostats$cpu <- 100 - as.numeric(iostats$idle)
}

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

# Get Latency-woq-obs
cat("generating latency-woq-obs\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq-obs", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq-obs"))
}
latency_woq_obs <- result
if (nrow(latency_woq_obs) > 0) {

    latency_woq_obs$row <- as.numeric(latency_woq_obs$row)
    latency_woq_obs$mystery <- as.numeric(latency_woq_obs$mystery)
    if (length(latency_woq_obs$percentile_999th)) {
        latency_woq_obs$percentile_999th <- as.numeric(latency_woq_obs$percentile_999th)
    }
    latency_woq_obs$percentile_99th <- as.numeric(latency_woq_obs$percentile_99th)
    latency_woq_obs$percentile_95th <- as.numeric(latency_woq_obs$percentile_95th)
    latency_woq_obs$percentile_90th <- as.numeric(latency_woq_obs$percentile_90th)
    if (length(latency_woq_obs$percentile_80th)) {
        latency_woq_obs$percentile_80th <- as.numeric(latency_woq_obs$percentile_80th)
    }

    all_builds = factor(latency_woq_obs$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_woq_obs[latency_woq_obs $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_woq_obs <- result
}

# Get Latency-woq-query
cat("generating latency-woq-query\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq-query", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq-query"))
}
latency_woq_query <- result
if (nrow(latency_woq_query) > 0) {

    latency_woq_query$row <- as.numeric(latency_woq_query$row)
    latency_woq_query$mystery <- as.numeric(latency_woq_query$mystery)
    if (length(latency_woq_query$percentile_999th)) {
        latency_woq_query$percentile_999th <- as.numeric(latency_woq_query$percentile_999th)
    }
    latency_woq_query$percentile_99th <- as.numeric(latency_woq_query$percentile_99th)
    latency_woq_query$percentile_95th <- as.numeric(latency_woq_query$percentile_95th)
    latency_woq_query$percentile_90th <- as.numeric(latency_woq_query$percentile_90th)
    if (length(latency_woq_query$percentile_80th)) {
        latency_woq_query$percentile_80th <- as.numeric(latency_woq_query$percentile_80th)
    }

    all_builds = factor(latency_woq_query$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_woq_query[latency_woq_query $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_woq_query <- result
}

# Get Latency-woq
cat("generating latency-woq\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq"))
}
latency_woq <- result
if (nrow(latency_woq) > 0) {

    latency_woq$row <- as.numeric(latency_woq$row)
    latency_woq$mystery <- as.numeric(latency_woq$mystery)
    if (length(latency_woq$percentile_999th)) {
        latency_woq$percentile_999th <- as.numeric(latency_woq$percentile_999th)
    }
    latency_woq$percentile_99th <- as.numeric(latency_woq$percentile_99th)
    latency_woq$percentile_95th <- as.numeric(latency_woq$percentile_95th)
    latency_woq$percentile_90th <- as.numeric(latency_woq$percentile_90th)
    if (length(latency_woq$percentile_80th)) {
        latency_woq$percentile_80th <- as.numeric(latency_woq$percentile_80th)
    }

    all_builds = factor(latency_woq$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_woq[latency_woq $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_woq <- result
}

# Get Latency-cor
cat("generating latency-cor\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-cor", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-cor"))
}
latency_cor <- result
if (nrow(latency_cor) > 0) {

    latency_cor$row <- as.numeric(latency_cor$row)
    latency_cor$mystery <- as.numeric(latency_cor$mystery)
    if (length(latency_cor$percentile_999th)) {
        latency_cor$percentile_999th <- as.numeric(latency_cor$percentile_999th)
    }
    latency_cor$percentile_99th <- as.numeric(latency_cor$percentile_99th)
    latency_cor$percentile_95th <- as.numeric(latency_cor$percentile_95th)
    latency_cor$percentile_90th <- as.numeric(latency_cor$percentile_90th)
    if (length(latency_cor$percentile_80th)) {
        latency_cor$percentile_80th <- as.numeric(latency_cor$percentile_80th)
    }

    all_builds = factor(latency_cor$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_cor[latency_cor $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_cor <- result
}

# Get Latency-obs-persist-server
cat("generating latency-obs-persist-server\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-persist-server", sep='')
        cat(paste(url,"\n"))
        doc_json <- fromJSON(file=url)
        cat(paste(builds_list[index,]$id,"\n"))
        unlisted <- plyr::ldply(doc_json, unlist)
        result <- rbind(result,unlisted)
    },error=function(e)e, finally=print("Error getting latency-obs-persist-server"))
}
latency_obs_persist_server <- result
if (nrow(latency_obs_persist_server) > 0) {

    latency_obs_persist_server$row <- as.numeric(latency_obs_persist_server$row)
    latency_obs_persist_server$mystery <- as.numeric(latency_obs_persist_server$mystery)
    if (length(latency_obs_persist_server$percentile_999th)) {
        latency_obs_persist_server$percentile_999th <- as.numeric(latency_obs_persist_server$percentile_999th)
    }
    latency_obs_persist_server$percentile_99th <- as.numeric(latency_obs_persist_server$percentile_99th)
    latency_obs_persist_server$percentile_95th <- as.numeric(latency_obs_persist_server$percentile_95th)
    latency_obs_persist_server$percentile_90th <- as.numeric(latency_obs_persist_server$percentile_90th)

    all_builds = factor(latency_obs_persist_server$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_obs_persist_server[latency_obs_persist_server $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_obs_persist_server <- result
}

# Get Latency-obs-persist-client
cat("generating latency-obs-persist-client\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-persist-client", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-obs-persist-client"))
}
latency_obs_persist_client <- result
if (nrow(latency_obs_persist_client) > 0) {

    latency_obs_persist_client$row <- as.numeric(latency_obs_persist_client$row)
    latency_obs_persist_client$mystery <- as.numeric(latency_obs_persist_client$mystery)
    if (length(latency_obs_persist_client$percentile_999th)) {
        latency_obs_persist_client$percentile_999th <- as.numeric(latency_obs_persist_client$percentile_999th)
    }
    latency_obs_persist_client$percentile_99th <- as.numeric(latency_obs_persist_client$percentile_99th)
    latency_obs_persist_client$percentile_95th <- as.numeric(latency_obs_persist_client$percentile_95th)
    latency_obs_persist_client$percentile_90th <- as.numeric(latency_obs_persist_client$percentile_90th)

    all_builds = factor(latency_obs_persist_client$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_obs_persist_client[latency_obs_persist_client $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_obs_persist_client <- result
}

# Get Latency-obs-repl-client
cat("generating latency-obs-repl-client\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-repl-client", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-obs-repl-client"))
}
latency_obs_repl_client <- result
if (nrow(latency_obs_repl_client) > 0) {

    latency_obs_repl_client$row <- as.numeric(latency_obs_repl_client$row)
    latency_obs_repl_client$mystery <- as.numeric(latency_obs_repl_client$mystery)
    if (length(latency_obs_repl_client$percentile_999th)) {
        latency_obs_repl_client$percentile_999th <- as.numeric(latency_obs_repl_client$percentile_999th)
    }
    latency_obs_repl_client$percentile_99th <- as.numeric(latency_obs_repl_client$percentile_99th)
    latency_obs_repl_client$percentile_95th <- as.numeric(latency_obs_repl_client$percentile_95th)
    latency_obs_repl_client$percentile_90th <- as.numeric(latency_obs_repl_client$percentile_90th)

    all_builds = factor(latency_obs_repl_client$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- latency_obs_repl_client[latency_obs_repl_client $buildinfo.version==a_build,]
        tt$mystery <- as.numeric(tt$mystery)
        min_myst = min(tt$mystery)
        filtered = transform(tt,row=mystery-min_myst)
        result <- rbind(result,filtered)
    }
    latency_obs_repl_client <- result
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

# Get indexing time stats
cat("generating indexing time stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","view_info", sep='')
		doc_json <- fromJSON(file=url)
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	}, error=function(e) e)
}

update_history <- result
if (nrow(update_history) > 0) {
    update_history$row <- as.numeric(update_history$row)
    update_history$indexing_time <- as.numeric(update_history$indexing_time)
    update_history$timestamp <- as.numeric(update_history$timestamp)

        all_builds = factor(update_history$buildinfo.version)
        result <- data.frame()
        for(a_build in levels(all_builds)) {
            tt <- update_history[update_history $buildinfo.version==a_build,]
            tt$timestamp <- as.numeric(tt$timestamp)
            min_timestamp = min(tt$timestamp)
            filtered = transform(tt, row=timestamp-min_timestamp)
            result <- rbind(result, filtered)
        }
        update_history <- result
}

# Get xdcr lag stats
cat("generating xdcr lag stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","xdcr_lag", sep="")
		doc_json <- fromJSON(file=url)
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	}, error=function(e) e)
}

xdcr_lag <- result
if (nrow(xdcr_lag) > 0) {
    xdcr_lag$row <- as.numeric(xdcr_lag$row)
    xdcr_lag$xdcr_persist_time <- as.numeric(xdcr_lag$xdcr_persist_time)
    xdcr_lag$xdcr_lag <- as.numeric(xdcr_lag$xdcr_lag)
    xdcr_lag$xdcr_diff <- as.numeric(xdcr_lag$xdcr_diff)
    xdcr_lag$timestamp <- as.numeric(xdcr_lag$timestamp)

    all_builds = factor(xdcr_lag$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- xdcr_lag[xdcr_lag $buildinfo.version==a_build,]
        tt$timestamp <- as.numeric(tt$timestamp)
        min_timestamp = min(tt$timestamp)
        filtered = transform(tt, row=timestamp-min_timestamp)
        result <- rbind(result, filtered)
    }
    xdcr_lag <- result
}

# Get rebalance progress
cat("generating rebalance progress stats\n")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","rebalance_progress", sep="")
		doc_json <- fromJSON(file=url)
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	}, error=function(e) e)
}

rebalance_progress <- result
if (nrow(rebalance_progress) > 0) {
    rebalance_progressrow <- as.numeric(rebalance_progress$row)
    rebalance_progress$rebalance_progress <- as.numeric(rebalance_progress$rebalance_progress)
    rebalance_progress$timestamp <- as.numeric(rebalance_progress$timestamp)

    all_builds = factor(rebalance_progress$buildinfo.version)
    result <- data.frame()
    for(a_build in levels(all_builds)) {
        tt <- rebalance_progress[rebalance_progress $buildinfo.version==a_build,]
        tt$timestamp <- as.numeric(tt$timestamp)
        min_timestamp = min(tt$timestamp)
        filtered = transform(tt, row=timestamp-min_timestamp)
        result <- rbind(result, filtered)
    }
    rebalance_progress <- result
}

result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","indexer_info", sep='')
		doc_json <- fromJSON(file=url)
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	}, error=function(e) e)
}

indexer_stats <- result
if (nrow(indexer_stats) > 0) {
    indexer_stats$row <- as.numeric(indexer_stats$row)
    indexer_stats$indexing_throughput <- as.numeric(indexer_stats$indexing_throughput)
    indexer_stats$timestamp <- as.numeric(indexer_stats$timestamp)

        all_builds = factor(indexer_stats$buildinfo.version)
        result <- data.frame()
        for(a_build in levels(all_builds)) {
            tt <- indexer_stats[indexer_stats $buildinfo.version==a_build,]
            tt$timestamp <- as.numeric(tt$timestamp)
            min_timestamp = min(tt$timestamp)
            filtered = transform(tt, row=timestamp-min_timestamp)
            result <- rbind(result, filtered)
        }
        indexer_stats <- result
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

# Get Latency-woq-obs histogram
cat("generating latency-woq-obs histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq-obs-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq-obs histogram"))
}
latency_woq_obs_histo <- result
latency_woq_obs_histo$time <- as.numeric(latency_woq_obs_histo$time)
latency_woq_obs_histo$count <- as.numeric(latency_woq_obs_histo$count)

# Get Latency-woq-query histogram
cat("generating latency-woq-query histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq-query-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq-query histogram"))
}
latency_woq_query_histo <- result
latency_woq_query_histo$time <- as.numeric(latency_woq_query_histo$time)
latency_woq_query_histo$count <- as.numeric(latency_woq_query_histo$count)

# Get Latency-woq histogram
cat("generating latency-woq histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-woq-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-woq histogram"))
}
latency_woq_histo <- result
latency_woq_histo$time <- as.numeric(latency_woq_histo$time)
latency_woq_histo$count <- as.numeric(latency_woq_histo$count)

# Get Latency-cor histogram
cat("generating latency-cor histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-cor-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-cor histogram"))
}
latency_cor_histo <- result
latency_cor_histo$time <- as.numeric(latency_cor_histo$time)
latency_cor_histo$count <- as.numeric(latency_cor_histo$count)

# Get latency_obs_persist_server histogram
cat("generating latency_obs_server histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
    tryCatch({
        url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-persist-server-histogram", sep='')
        cat(paste(url,"\n"))
        doc_json <- fromJSON(file=url)
        cat(paste(builds_list[index,]$id,"\n"))
        unlisted <- plyr::ldply(doc_json, unlist)
        result <- rbind(result,unlisted)
    },error=function(e)e, finally=print("Error getting latency-obs-persist-server histogram"))
}
latency_obs_persist_server_histo <- result
latency_obs_persist_server_histo$time <- as.numeric(latency_obs_persist_server_histo$time)
latency_obs_persist_server_histo$count <- as.numeric(latency_obs_persist_server_histo$count)

# Get latency_obs_persist_client histogram
cat("generating latency_obs_persist_client histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-persist-client-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-obs-persist-client histogram"))
}
latency_obs_persist_client_histo <- result
latency_obs_persist_client_histo$time <- as.numeric(latency_obs_persist_client_histo$time)
latency_obs_persist_client_histo$count <- as.numeric(latency_obs_persist_client_histo$count)

# Get latency_obs_repl_client histogram
cat("generating latency_obs_repl_client histogram")
result <- data.frame()
for(index in 1:nrow(builds_list)) {
	tryCatch({
		url = paste("http://",dbip,":5984/",dbname,"/",builds_list[index,]$id,"/","latency-obs-persist-client-histogram", sep='')
		cat(paste(url,"\n"))
		doc_json <- fromJSON(file=url)
		cat(paste(builds_list[index,]$id,"\n"))
		unlisted <- plyr::ldply(doc_json, unlist)
		result <- rbind(result,unlisted)
	},error=function(e)e, finally=print("Error getting latency-obs-persist-client histogram"))
}
latency_obs_repl_client_histo <- result
latency_obs_repl_client_histo$time <- as.numeric(latency_obs_repl_client_histo$time)
latency_obs_repl_client_histo$count <- as.numeric(latency_obs_repl_client_histo$count)

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

# apply rebalance filter
if (reb) {
    ns_server_data = rebFilter(ns_server_data, builds_list)
    latency_get = rebFilter(latency_get, builds_list)
    latency_set = rebFilter(latency_set, builds_list)
    latency_query = rebFilter(latency_query, builds_list)
    latency_obs_persist_client = rebFilter(latency_obs_persist_client, builds_list)
    latency_obs_persist_server = rebFilter(latency_obs_persist_server, builds_list)
    latency_obs_repl_client = rebFilter(latency_obs_repl_client, builds_list)
    latency_woq_obs = rebFilter(latency_woq_obs, builds_list)
    latency_woq_query = rebFilter(latency_woq_query, builds_list)
    latency_woq = rebFilter(latency_woq, builds_list)
    throughput_query = rebFilter(throughput_query, builds_list)
    disk_data = rebFilter(disk_data, builds_list)

    builds_list$runtime = builds_list$reb_dur
}

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
    fi_0 <- ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$ep_diskqueue_drain==0, ]

    print(paste("disk drain log: ", build))
    num_zero = length(fi_0$ep_diskqueue_drain)
    num_samples = num_zero + length(fi$ep_diskqueue_drain)

    print(paste("num of samples: ", num_samples))
    print(paste("num of 0s: ", num_zero))
    print(paste("percentage - 0s: ", num_zero/num_samples, "%"))

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

nodes = factor(ns_server_data_system$node)
first_node_stats = subset(ns_server_data_system, node==levels(nodes)[1])

for(build in levels(builds)) {
	data <- subset(first_node_stats, buildinfo.version==build)
	average_cpu <- mean(data$cpu_util)
	if(build == baseline){
		row <-c ("baseline", "Average CPU rate", as.numeric(average_cpu))
		combined <- rbind(combined, row)
	}
	else{
		row <-c (build, "Average CPU rate", as.numeric(average_cpu))
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

if(!is.null(ns_server_data$xdc_ops)) {
    for(build in levels(builds)) {
        fi <-ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$xdc_ops!=0, ]
        d <- mean(fi$xdc_ops)
        if(build == baseline){
            row <-c ("baseline", "Avg. XDCR ops/sec", as.numeric(d))
            combined <- rbind(combined, row)
        }
        else{
            row <-c (build, "Avg. XDCR ops/sec", as.numeric(d))
            combined <- rbind(combined, row)
        }
    }
}

if(!is.null(ns_server_data$replication_changes_left)) {
    for(build in levels(builds)) {
        fi <-ns_server_data[ns_server_data$buildinfo.version==build & ns_server_data$replication_changes_left!=0, ]
        d <- mean(fi$replication_changes_left)
        if(build == baseline){
            row <-c ("baseline", "Avg. XDCR docs to replicate", as.numeric(d))
            combined <- rbind(combined, row)
        }
        else{
            row <-c (build, "Avg. XDCR docs to replicate", as.numeric(d))
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

#for(build in levels(builds)) {
#    fi <- warmup_stats[warmup_stats$buildinfo.version == build, ]
#	d <- fi$ep_warmup_time[1] / 1000000
#
#    if(build == baseline){
#		row <-c ("baseline", "warmup time (with os flush)", as.numeric(d))
#		combined <- rbind(combined, row)
#	}
#	else{
#		row <-c (build, "warmup time (with os flush)", as.numeric(d))
#		combined <- rbind(combined, row)
#	}
#}

#for(build in levels(builds)) {
#    fi <- warmup_stats[warmup_stats$buildinfo.version == build, ]
#	d <- fi$ep_warmup_time[2] / 1000000
#
#    if(build == baseline){
#		row <-c ("baseline", "warmup time (no os flush)", as.numeric(d))
#		combined <- rbind(combined, row)
#	}
#	else{
#		row <-c (build, "warmup time (no os flush)", as.numeric(d))
#		combined <- rbind(combined, row)
#	}
#}

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
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[11])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[3])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[12])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[13])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[4])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[14])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[15])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[16])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[17])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[18])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[19])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[20])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[21])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[22])))
MB <- append(MB,as.numeric(sprintf("%.2f",MB1[23])))
MB <- append(MB,combined[combined[,'system'] == 'baseline',]$value[25])

#if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
#    MB <- append(MB,as.numeric(sprintf("%.2f",MB1[26])))
#    MB <- append(MB,as.numeric(sprintf("%.2f",MB1[27])))
#}

CB <- c()
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[1]/3600)))
CB <- append(CB, commaize(CB1[2]))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[5]/1024)))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[6]/1024)))
CB <- append(CB, commaize(CB1[7]))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[8]/1024)))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[9])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[10])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[11])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[3])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[12])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[13])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[4])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[14])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[15])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[16])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[17])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[18])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[19])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[20])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[21])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[22])))
CB <- append(CB,as.numeric(sprintf("%.2f",CB1[23])))
CB <- append(CB,combined[combined[,'system'] != 'baseline',]$value[25])

#if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
#    CB <- append(CB,as.numeric(sprintf("%.2f",CB1[26])))
#    CB <- append(CB,as.numeric(sprintf("%.2f",CB1[27])))
#}

testdf <- data.frame(MB,CB)

row_names <- c("Runtime (in hr)","Avg. Drain Rate","Peak Disk (GB)","Peak Memory (GB)", "Avg. OPS", "Avg. mem memcached (GB)", "Avg. mem beam.smp (MB)", "Avg. CPU rate (%)", "Latency-get (90th) (ms)", "Latency-get (95th) (ms)","Latency-get (99th) (ms)","Latency-set (90th) (ms)","Latency-set (95th) (ms)","Latency-set (99th) (ms)","Latency-query (80th) (ms)","Latency-query (90th) (ms)","Latency-query (95th) (ms)","Latency-query (99th) (ms)", "Latency-query (99.9th) (ms)", "Avg. QPS", "Avg. XDC ops/sec", "Avg. XDC docs to replicate", "Rebalance Time (sec)", "Testrunner Version")

#if ("warmup" %in% unlist(strsplit(test_name, '\\.'))) {
#	row_names <- c(row_names, c("Warmup Time - flush (sec)", "Warmup Time - no flush (sec)"))
#}

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

    if(!is.null(ns_server_data$views_ops)) {
        cat("generating couch_views_ops \n")
        p <- ggplot(ns_server_data, aes(row, views_ops, color=buildinfo.version , label=views_ops)) + labs(x="----time (sec)--->", y="Queries/sec")
        p <- p + geom_point()
        p <- addopts(p,"View read per sec.")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("All the view reads for all design documents",
                            "including scatter gather.",
                            sep="\n"))
    }

    cat("generating ep queue size \n")
    p <- ggplot(ns_server_data, aes(row, ep_queue_size, color=buildinfo.version , label= prettySize(ep_queue_size))) + labs(x="----time (sec)--->", y="ep_queue_size")
    p <- p + geom_point()
    p <- addopts(p,"ep queue size")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Number of items queued for storage")

    cat("generating ns_server: ep_diskqueue_drain \n")
    p <- ggplot(ns_server_data, aes(row, ep_diskqueue_drain, color=buildinfo.version , label= prettySize(ep_diskqueue_drain))) + labs(x="----time (sec)--->", y="ep_diskqueue_drain")
    p <- p + geom_point()
    p <- addopts(p,"ns_server: ep_diskqueue_drain")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Total number of items per second being",
                        "written to disk in this bucket (from ns_server)",
                        sep="\n"))

    for(ip in levels(factor(memcached_stats$ip))) {

        stats <- data.frame()
        for (build in levels(factor(memcached_stats$buildinfo.version))) {
            tmp = memcached_stats[memcached_stats$ip == ip & memcached_stats$buildinfo.version == build, ]
            tmp$ep_diskqueue_drain = c(0 , diff(tmp$ep_diskqueue_drain) / diff(tmp$uptime))
            stats <- rbind(stats, tmp)
        }

        cat(paste("generating ep-engine: ep_diskqueue_drain - \n", ip))
        p <- ggplot(stats, aes(row, ep_diskqueue_drain, color=buildinfo.version , label= prettySize(ep_diskqueue_drain))) + labs(x="----time (sec)--->", y="ep_diskqueue_drain")
        p <- p + geom_point()
        p <- addopts(p, paste("ep-engine : ep_diskqueue_drain - ", ip))
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Total number of items per second being",
                            "written to disk in this node (from ep-engine)",
                            sep="\n"))
    }

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

    cat("generating ep_tap_replica_queue_drain \n")
    p <- ggplot(ns_server_data, aes(row, ep_tap_replica_queue_drain, color=buildinfo.version , label= prettySize(ep_tap_replica_queue_drain))) + labs(x="----time (sec)--->", y="ep_tap_replica_queue_backoff")
    p <- p + geom_point()
    p <- addopts(p,"ep_tap_replica_queue_drain/sec")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Number of items per second ",
                        "been sent over replication ",
                         "TAP connections",
                         sep="\n"))

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
        p <- ggplot(ns_server_data, aes(row, docs_fragmentation, color=buildinfo.version, label=docs_fragmentation)) + labs(x="----time (sec)--->", y="%")
        p <- p + geom_point()
        p <- addopts(p,"Docs fragmentation")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("How much fragmented data there is to be ",
                            "compacted compared to real data for the ",
                            "data files in this bucket (2.0 only)",
                            sep="\n"))

        cat("generating couch_views_fragmentation \n")
        p <- ggplot(ns_server_data, aes(row, views_fragmentation, color=buildinfo.version, label=views_fragmentation)) + labs(x="----time (sec)--->", y="%")
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

    cat("generating connections \n")
    p <- ggplot(ns_server_data, aes(row, curr_connections, color=buildinfo.version , label= curr_connections)) + labs(x="----time (sec)--->", y="connections")
    p <- p + geom_point()
    p <- addopts(p,"Number of connections")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Number of connections to this server",
                        "including connections from external",
                        "drivers, proxies, TAP requests and",
                        "internal statistic gathering (measured",
                        "from curr_connections)",
                        sep="\n"))

    ################################################################################################################
    if(!is.null(ns_server_data$replication_rate_replication)) {
        p <- ggplot(ns_server_data,
                    aes(row, replication_rate_replication, color=buildinfo.version, label=replication_rate_replication))
        p <- p + labs(x="----time (sec)--->", y="ops/sec") + geom_point()
        p <- addopts(p, "Mutation replication rate")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Rate of replication in terms of number",
                            "of replicated mutations per second", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_bandwidth_usage, color=buildinfo.version, label=replication_bandwidth_usage))
        p <- p + labs(x="----time (sec)--->", y="Bytes/sec") + geom_point()
        p <- addopts(p, "Data replication rate")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Rate of replication in terms",
                            "of bytes replicated per second", sep="\n"))
        ################################################################################################################
        p <- ggplot(ns_server_data,
                    aes(row, replication_docs_latency_wt, color=buildinfo.version, label=replication_docs_latency_wt))
        p <- p + labs(x="----time (sec)--->", y="ms") + geom_point()
        p <- addopts(p, "ms doc ops latency")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Weighted average latency in ms of",
                            "sending replicated mutations to remote cluster", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_meta_latency_wt, color=buildinfo.version, label=replication_meta_latency_wt))
        p <- p + labs(x="----time (sec)--->", y="ms") + geom_point()
        p <- addopts(p, "ms meta ops latency")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Weighted average latency in ms of",
                            "sending getMeta and waiting for conflict solution",
                            "result from remote cluster", sep="\n"))
    }
    ################################################################################################################
    if(!is.null(ns_server_data$replication_num_failedckpts)) {
        p <- ggplot(ns_server_data,
                    aes(row, replication_changes_left, color=buildinfo.version, label=replication_changes_left))
        p <- p + labs(x="----time (sec)--->", y="items") + geom_point()
        p <- addopts(p, "Outbound XDCR mutations")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of mutations to be",
                            "replicated to other clusters", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_docs_rep_queue, color=buildinfo.version, label=replication_docs_rep_queue))
        p <- p + labs(x="----time (sec)--->", y="items") + geom_point()
        p <- addopts(p, "Mutations in queue")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of document mutations",
                            "in XDC replication queue", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_size_rep_queue, color=buildinfo.version, label=replication_size_rep_queue))
        p <- p + labs(x="----time (sec)--->", y="Bytes") + geom_point()
        p <- addopts(p, "XDCR queue size")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Size in bytes of",
                            "XDC replication queue", sep="\n"))
        ################################################################################################################
        p <- ggplot(ns_server_data,
                    aes(row, replication_docs_checked, color=buildinfo.version, label=replication_docs_checked))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "Mutations checked")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Document mutations checked",
                            "for XDC replication", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_docs_written, color=buildinfo.version, label=replication_docs_written))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "Mutations replicated")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Document mutations replicated",
                            "to remote cluster", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_data_replicated, color=buildinfo.version, label=replication_data_replicated))
        p <- p + labs(x="----time (sec)--->", y="Bytes") + geom_point()
        p <- addopts(p, "XDCR data replicated")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Data in bytes replicated",
                            "to remote cluster", sep="\n"))
        ################################################################################################################
        p <- ggplot(ns_server_data,
                    aes(row, replication_work_time, color=buildinfo.version, label=replication_work_time))
        p <- p + labs(x="----time (sec)--->", y="secs") + geom_point()
        p <- addopts(p, "XDCR secs in replicating")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Total time in secs all vb replicators",
                            "spent checking and writing", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_commit_time, color=buildinfo.version, label=replication_commit_time))
        p <- p + labs(x="----time (sec)--->", y="secs") + geom_point()
        p <- addopts(p,"XDCR secs in checkpointing")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Total time all vb replicators",
                            "spent waiting for commit and checkpoint", sep="\n"))
        ################################################################################################################
        p <- ggplot(ns_server_data,
                    aes(row, replication_active_vbreps, color=buildinfo.version, label=replication_active_vbreps))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "XDCR active vb reps")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of active vbucket",
                            "replications", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_waiting_vbreps, color=buildinfo.version, label=replication_waiting_vbreps))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "XDCR waiting vb reps")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of waiting vbucket",
                            "replications", sep="\n"))
        ################################################################################################################
        p <- ggplot(ns_server_data,
                    aes(row, replication_num_checkpoints, color=buildinfo.version, label=replication_num_checkpoints))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "XDCR checkpoints issued")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of successful checkpoints",
                            "out of the last 10 issued on each node",
                            "in current replication", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, replication_num_failedckpts, color=buildinfo.version, label=replication_num_failedckpts))
        p <- p + labs(x="----time (sec)--->", y="") + geom_point()
        p <- addopts(p, "XDCR checkpoints failed")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of failed checkpoints",
                            "out of the last 10 issued on each node",
                            "in current replication", sep="\n"))
    }
    ################################################################################################################
    if(!is.null(ns_server_data$xdc_ops)) {
        p <- ggplot(ns_server_data,
                    aes(row, xdc_ops, color=buildinfo.version, label=xdc_ops))
        p <- p + labs(x="----time (sec)--->", y="ops/sec") + geom_point()
        p <- addopts(p, "XDC ops per sec")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Incoming XDCR operations",
                            "per second for this bucket", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, ep_num_ops_get_meta, color=buildinfo.version, label=ep_num_ops_get_meta))
        p <- p + labs(x="----time (sec)--->", y="ops/sec") + geom_point()
        p <- addopts(p, "Metadata gets per sec")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of metadata read operations",
                            "per second for this bucket",
                            "as the target for XDCR", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, ep_num_ops_set_meta, color=buildinfo.version, label=ep_num_ops_set_meta))
        p <- p + labs(x="----time (sec)--->", y="ops/sec") + geom_point()
        p <- addopts(p, "Metadata sets per sec")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of set operations",
                            "per second for this bucket",
                            "as the target for XDCR", sep="\n"))

        p <- ggplot(ns_server_data,
                    aes(row, ep_num_ops_del_meta, color=buildinfo.version, label=ep_num_ops_del_meta))
        p <- p + labs(x="----time (sec)--->", y="ops/sec") + geom_point()
        p <- addopts(p, "Metadata dels per sec")
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("Number of delete operations",
                            "per second for this bucket",
                            "as the target for XDCR", sep="\n"))
    }
    ################################################################################################################
}

if (nrow(rebalance_progress) > 0) {
    p <- ggplot(rebalance_progress, aes(row, rebalance_progress, color=buildinfo.version, label=rebalance_progress))
    p <- p + labs(x="----time (sec)--->", y="%")
    p <- p + geom_point()
    p <- addopts(p, "Rebalance progress")
    print(p)
    makeFootnote(footnote)
}

cat("generating cpu_util \n")
nodes = factor(ns_server_data_system$node)
for(ns_node in levels(nodes)) {
    node_system_stats = subset(ns_server_data_system, node==ns_node)
    p <- ggplot(node_system_stats, aes(row, cpu_util, color=buildinfo.version, label=cpu_util))
    p <- p + labs(x="----time (min)--->", y="%")
    p <- p + coord_cartesian(ylim = c(0, 100))
    p <- p + geom_point()
    p <- addopts(p, paste("CPU utilization", ns_node, sep=" - "))
    print(p)
    makeFootnote(footnote)
}

cat("generating swap_used \n")
for(ns_node in levels(nodes)) {
    node_system_stats = subset(ns_server_data_system, node==ns_node)
    p <- ggplot(node_system_stats, aes(row, swap_used, color=buildinfo.version, label=swap_used))
    p <- p + labs(x="----time (min)--->", y="Bytes")
    p <- p + geom_point()
    p <- addopts(p, paste("SWAP Usage", ns_node, sep=" - "))
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

for(ip in levels(factor(memcached_stats$ip))) {

    node_stats = memcached_stats[memcached_stats$ip == ip, ]
    node_stats$uptime = node_stats$uptime - min(node_stats$uptime)

    if (!is.null(memcached_stats$ep_bg_wait_avg)) {
        cat(paste("generating ep-engine: ep_bg_wait_avg - \n", ip))
        p <- ggplot(node_stats, aes(uptime, ep_bg_wait_avg, color=buildinfo.version , label= prettySize(ep_bg_wait_avg))) + labs(x="----time (sec)--->", y="ep_bg_wait_avg")
        p <- p + geom_point()
        p <- addopts(p, paste("ep-engine : ep_bg_wait_avg - ", ip))
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The average wait time (ms) for an item",
                            "before it is serviced by the dispatcher",
                            sep="\n"))
    }

    if (!is.null(memcached_stats$ep_bg_load_avg)) {
        cat(paste("generating ep-engine: ep_bg_load_avg - \n", ip))
        p <- ggplot(node_stats, aes(uptime, ep_bg_load_avg, color=buildinfo.version , label= prettySize(ep_bg_load_avg))) + labs(x="----time (sec)--->", y="ep_bg_load_avg")
        p <- p + geom_point()
        p <- addopts(p, paste("ep-engine : ep_bg_load_avg - ", ip))
        print(p)
        makeFootnote(footnote)
        makeMetricDef(paste("The average wait time (ms) for an item",
                            "loaded from the persistence layer",
                            sep="\n"))
    }
}

cat("generating iostat \n")
for(ip in levels(factor(iostats$ip))) {
    stats <- data.frame()
    for (build in levels(factor(iostats$buildinfo.version))) {
        tmp = iostats[iostats$ip == ip & iostats$buildinfo.version == build, ]
        tmp$read = c(0 , diff(tmp$read) / diff(tmp$time))
        tmp$write = c(0, diff(tmp$write) / diff(tmp$time))
        tmp$time = tmp$time - min(tmp$time)
        stats <- rbind(stats, tmp)
    }

    p <- ggplot(stats, aes(time, read, color=buildinfo.version, label=stats)) + labs(x="----time (sec)--->", y="iostat - Disk Read (kB/s)")
    p <- p + geom_line()
    p <- addopts(p, paste("Disk Read (kB/s) : ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Disk read across all hard drives (kB/s)")

    p <- ggplot(stats, aes(time, write, color=buildinfo.version, label= stats)) + labs(x="----time (sec)--->", y="iostat - Disk Write (kB/s)")
    p <- p + geom_line()
    p <- addopts(p, paste("Disk Write (kB/s) : ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Disk write across all hard drives (kB/s)")

    p <- ggplot(stats, aes(time, util, color=buildinfo.version, label= stats)) + labs(x="----time (sec)--->", y="iostat - Disk %util")
    p <- p + geom_line()
    p <- addopts(p, paste("Average %util : ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Average disk utilization percentage ",
                        " across all hard drives (kB/s)", sep="\n"))

    p <- ggplot(stats, aes(time, iowait, color=buildinfo.version, label= stats)) + labs(x="----time (sec)--->", y="iostat - %iowait")
    p <- p + geom_line()
    p <- addopts(p, paste("Average %iowait : ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Average cpu percentage waiting for ",
                        "outstanding IO", sep="\n"))

    p <- ggplot(stats, aes(time, cpu, color=buildinfo.version, label= stats)) + labs(x="----time (sec)--->", y="iostat - %cpu")
    p <- p + geom_line()
    p <- addopts(p, paste("Average %cpu : ", ip))
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Average cpu percentage measured from ",
                        "iostat excluding %idle", sep="\n"))

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

if (nrow(latency_woq_obs_histo) > 0) {
    cat("plotting latency-woq-obs histogram \n")
    latency_woq_obs_histo = latency_woq_obs_histo[!is.na(latency_woq_obs_histo$time), ]
    p <- ggplot(latency_woq_obs_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (s)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency woq-obs histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for woq-obs commands")
}

if (nrow(latency_woq_query_histo) > 0) {
    cat("plotting latency-woq-query histogram \n")
    latency_woq_query_histo = latency_woq_query_histo[!is.na(latency_woq_query_histo$time), ]
    p <- ggplot(latency_woq_query_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (s)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency woq-query histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for woq-query commands")
}

if (nrow(latency_woq_histo) > 0) {
    cat("plotting latency-woq histogram \n")
    latency_woq_histo = latency_woq_histo[!is.na(latency_woq_histo$time), ]
    p <- ggplot(latency_woq_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (s)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency woq histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for woq commands")
}

if (nrow(latency_cor_histo) > 0) {
    cat("plotting latency-cor histogram \n")
    latency_cor_histo = latency_cor_histo[!is.na(latency_cor_histo$time), ]
    p <- ggplot(latency_cor_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count))) + labs(x="----latency (s)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Latency cor histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef("Final latency histogram for cor commands")
}

if (nrow(latency_obs_persist_server_histo) > 0) {
    cat("plotting latency_obs_persist_server histogram \n")
    latency_obs_persist_server_histo = latency_obs_persist_server_histo[!is.na(latency_obs_persist_server_histo$time), ]
    p <- ggplot(latency_obs_persist_server_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count)))
    p <- p + labs(x="----latency (sec)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Server side observe latency histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Server side latency histogram ",
                        "for observe persistency stats",
                        sep="\n"))
}

if (nrow(latency_obs_persist_client_histo) > 0) {
    cat("plotting latency_obs_client histogram \n")
    latency_obs_persist_client_histo = latency_obs_persist_client_histo[!is.na(latency_obs_persist_client_histo$time), ]
    p <- ggplot(latency_obs_persist_client_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count)))
    p <- p + labs(x="----latency (sec)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Client side persist observe latency histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Client side latency histogram ",
                        "for observe persistency stats",
                        sep="\n"))
}

if (nrow(latency_obs_repl_client_histo) > 0) {
    cat("plotting latency_obs_client histogram \n")
    latency_obs_repl_client_histo = latency_obs_repl_client_histo[!is.na(latency_obs_repl_client_histo$time), ]
    p <- ggplot(latency_obs_repl_client_histo, aes(x=time, y=count, color=buildinfo.version, label= prettySize(count)))
    p <- p + labs(x="----latency (sec)--->", y="count")
    p <- p + geom_point()
    p <- addopts(p,"Client side repl observe latency histogram")
    print(p)
    makeFootnote(footnote)
    makeMetricDef(paste("Client side latency histogram ",
                        "for observe replication stats",
                        sep="\n"))
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

if (nrow(latency_woq_obs) > 0) {

    cat("Latency-woq-obs 90th\n")
    temp <- latency_woq_obs[latency_woq_obs$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-obs 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq-obs 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-obs 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq-obs 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-obs 99th  percentile")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_woq_query) > 0) {

    cat("Latency-woq-query 90th\n")
    temp <- latency_woq_query[latency_woq_query$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-query 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq-query 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-query 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq-query 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq-query 99th  percentile")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_woq) > 0) {

    cat("Latency-woq 90th\n")
    temp <- latency_woq[latency_woq$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-woq 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-woq 99th  percentile")
    print(p)
    makeFootnote(footnote)

}

if (nrow(latency_cor) > 0) {

    cat("Latency-cor 90th\n")
    temp <- latency_cor[latency_cor$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-cor 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-cor 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-cor 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-cor 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-cor 99th  percentile")
    print(p)
    makeFootnote(footnote)

}

if (nrow(latency_obs_persist_server) > 0) {

    cat("Latency-obs-persist-server 90th\n")
    temp <- latency_obs_persist_server[latency_obs_persist_server$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-server 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-persist-server 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-serve 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-persist-server 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-server 99th  percentile")
    print(p)
    makeFootnote(footnote)
}

if (nrow(latency_obs_persist_client) > 0) {

    cat("Latency-obs-persist-client 90th\n")
    temp <- latency_obs_persist_client[latency_obs_persist_client$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-client 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-persist-client 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-client 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-persist-client 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-persist-client 99th  percentile")
    print(p)
    makeFootnote(footnote)

}

if (nrow(latency_obs_repl_client) > 0) {

    cat("Latency-obs-repl-client 90th\n")
    temp <- latency_obs_repl_client[latency_obs_repl_client$client_id ==0,]
    p <- ggplot(temp, aes(temp$row, temp$percentile_90th, color=buildinfo.version, label=temp$percentile_90th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-repl-client 90th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-repl-client 95th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_95th, color=buildinfo.version, label=temp$percentile_95th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-repl-client 95th  percentile")
    print(p)
    makeFootnote(footnote)

    cat("Latency-obs-repl-client 99th\n")
    p <- ggplot(temp, aes(temp$row, temp$percentile_99th, color=buildinfo.version, label=temp$percentile_99th)) + labs(x="----time (sec)--->", y="sec")
    p <- p + geom_point()
    p <- addopts(p,"Latency-obs-repl-client 99th  percentile")
    print(p)
    makeFootnote(footnote)

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

if (nrow(update_history) > 0) {
    nodes = factor(update_history$node)
    for(ns_node in levels(nodes)) {
        node_update_history = subset(update_history, node==ns_node)
        p <- ggplot(node_update_history, aes(row, indexing_time, color=buildinfo.version, label=indexing_time))
        p <- p + labs(x="----time (sec)--->", y="Seconds")
        p <- p + geom_point()
        p <- addopts(p, paste("Indexing time", ns_node, sep=" - "))
        print(p)
        makeFootnote(footnote)
    }

    for(ns_node in levels(nodes)) {
        node_update_history = subset(update_history, node==ns_node)
        p <- ggplot(node_update_history, aes(row, indexing_time, color=buildinfo.version, label=indexing_time))
        p <- p + labs(x="----time (sec)--->", y="Seconds")
        p <- p + coord_cartesian(ylim = c(0, 5))
        p <- p + geom_point()
        p <- addopts(p, paste("Indexing time (0-5 sec)", ns_node, sep=" - "))
        print(p)
        makeFootnote(footnote)
    }
}

if (nrow(indexer_stats) > 0) {
    p <- ggplot(indexer_stats, aes(row, indexing_throughput, color=buildinfo.version, label=indexing_throughput))
    p <- p + labs(x="----time (sec)--->", y="ops/sec")
    p <- p + geom_point()
    p <- addopts(p, "Indexing throughput")
    print(p)
    makeFootnote(footnote)
}

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

conf_file = buildPath(test_name, "conf/perf/", ".conf")
dumpTextFile(conf_file)

if (builds_list$cluster_name != "") {
    ini_file = buildPath(builds_list$cluster_name, "resources/perf/", ".ini")
    dumpTextFile(ini_file)
}

dev.off()

