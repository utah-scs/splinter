source('scripts/common.R')

readSamples <- function(filename) {
    read.table(filename, header=TRUE)
}

plotLatencyVsThroughput <- function(t, f) {
    d <- merge(t, f, by="Offered")
    d <- melt(d, id.vars="Offered",
              measure.vars=c("X50.x", "X50.y", "X99.x", "X99.y"))

    p <- ggplot(d, aes(x=Offered, y=value/1000, col=variable)) +
            geom_line() +
            geom_point() +
            scale_x_continuous(name='Throughput (Million Operations per sec)',
                    breaks=c(0.1e6, 0.2e6, 0.3e6, 0.4e6, 0.5e6, 0.6e6, 0.7e6,
                             0.8e6, 0.9e6, 1e6, 1.1e6, 1.2e6),
                    labels=c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1,
                             1.1, 1.2)) +
            scale_y_continuous(name=expression(paste('Latency (', mu, 's)')),
                    breaks=c(15, 30, 45, 60, 75, 90)) +
            coord_cartesian(xlim=c(0, 1.2e6), ylim=c(0, 100)) +
            scale_color_brewer(palette='Set1', name='',
                    labels=c('Invoke, 50th', 'Native, 50th',
                             'Invoke, 99th', 'Native, 99th')) +
            myTheme

    ggsave(plot=p, filename='ycsb_latency_vs_thrpt.pdf', width=5, height=2,
           units='in')
}

plotAllFigs <- function() {
    t = readSamples("ycsb_invoke_true.out")
    f = readSamples("ycsb_invoke_false.out")

    plotLatencyVsThroughput(t, f)
}
