#!/bin/bash
#
# Copyright (c) 2018 University of Utah
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

source('scripts/common.R')

# Read samples from a file into a dataframe.
#
# \param filename: File containing samples obtained by running YCSB against a
#                  Sandstorm server.
#
# \return A dataframe containing samples, with the header intact.
readSamples <- function(filename) {
    read.table(filename, header=TRUE)
}

# Plot the Median and 99th latency vs throughput for the YCSB workload.
#
# \param t: Samples (with header) from an invoke() based YCSB run.
# \param f: Samples (with header) from a native operation based YCSB run.
plotLatencyVsThroughput <- function(t, f) {
    # Merge the passed in frames based on the Offered throughput. Effectively
    # creates a single x-axis. *.x correspond to the invoke() based
    # measurements, and *.y correspond to the native operation based
    # measurements.
    d <- merge(t, f, by="Offered")
    d <- melt(d, id.vars="Offered",
              measure.vars=c("X50.x", "X50.y", "X99.x", "X99.y"))

    # Plot the samples. x-axis = Offered throughput. y-axis = Latency.
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

    # Save the plot to a file.
    ggsave(plot=p, filename='ycsb_latency_vs_thrpt.pdf', width=5, height=2,
           units='in')
}

# Plots all YCSB graphs.
# - Latency vs Throughput
plotAllFigs <- function() {
    t = readSamples("ycsb_invoke_true.out")
    f = readSamples("ycsb_invoke_false.out")

    plotLatencyVsThroughput(t, f)
}
