# Copyright (c) 2017 University of Utah
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

source('common.R')

# Read samples from a file.
readSamples <- function(fileName='samples.data')
{
    samples <- read.table(fileName, header=TRUE)

    return (samples)
}

# Plot a cdf of context switch times.
plotCDF <- function(fileName='samples.data')
{
    samples <- readSamples(fileName)

    p <- ggplot(samples, aes(x=(Cycles*1e6)/CyclesPerSec)) +
         stat_ecdf(geom='step', color='red', size=1.5) +
         scale_y_continuous(name='') +
         scale_x_continuous(name='2 x Context Switch Time (Micro seconds)',
                            breaks=c(0, 2, 10, 20, 30, 40)) +
         myTheme

    return (p)
}

# Wrapper function. Calls plotCDF() and saves the plot into a pdf file.
plotContextSwitch <- function()
{
    p <- plotCDF()
    ggsave(plot=p, filename='context_switch_cdf.pdf', width=5, height=3)
}
