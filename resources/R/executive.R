require(ggplot2)

buildComparison <- function(df, field, value) {
  dfsub <- df[df[,field] == value,]
  colnames(dfsub)[length(dfsub)] <- 'frombaseline'
  rv <- merge(df, dfsub[,-1])
  transform(rv, offpercent=ifelse(value > frombaseline,
                  value / frombaseline * -1,
                  frombaseline / value))
}

colors <- c('Pass'='#009900', 'Fail'='#990000')

magnitude_limit <- 3

fixupData <- function(df) {
  ## Flip the direction of the drain rate.
  df[df$test == 'drain rate','offpercent'] = df[df$test == 'drain rate','offpercent'] * -1

  df$offpercent = df$offpercent
  df <- transform(df,
                  position=ifelse(abs(offpercent) > magnitude_limit,
                    sign(offpercent) * (magnitude_limit),
                    offpercent) - sign(offpercent),
                  color=ifelse(offpercent > -1.1, 'Pass', 'Fail'))
  df
}
