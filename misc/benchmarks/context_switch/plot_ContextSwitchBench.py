#!/usr/bin/python

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

import csv
import sys
import glob
import gzip
import numpy

import matplotlib.pyplot as plt

# A dictionary mapping column names in the *.data.gz files to column indices.
columnDict = {
    "ExptId"       : 0,
    "TS"           : 1,
    "Cycles"       : 2,
    "CyclesPerSec" : 3,
    "Machine"      : 4,
    "ActiveCores"  : 5,
    "Cores"        : 6,
    "Sockets"      : 7
}

def plotConfigCDF(sortedData, machine, activeCores):
    """Plot a cdf for a given experimental configuration.

    @param sortedData: A list of two way context-switch times in
                       micro-seconds.

    @param machine: The machine the data was collected on.
    @type  machine: C{str}

    @param activeCores: The number of hardware cores the data was collected on.
    @type  activeCores: C{str}
    """
    prob = numpy.arange(len(sortedData)) / float(len(sortedData) - 1)

    # Two plots to avoid the tail from cluttering the data.
    plt.close()
    f, (ax1, ax2) = plt.subplots(1,2,sharey=True, facecolor='w')

    # First, plot the relevant data.
    ax1.plot(sortedData, prob)
    ax1.set_xlim(0, 6)
    ax1.spines['right'].set_visible(False)
    ax1.set_ylabel('CDF')
    ax1.set_xlabel('Time to Context Switch Out and Back In (' + \
                   r'$\mu$' + 'sec)')
    ax1.xaxis.set_label_coords(1.1, -0.07)

    # Next, plot the tail.
    ax2.plot(sortedData, prob)
    ax2.set_xlim(sortedData[len(sortedData) - 1] - 3, \
                 sortedData[len(sortedData) - 1] + 3)
    ax2.spines['left'].set_visible(False)
    ax2.yaxis.set_visible(False)

    # Break the x-axis.
    d = 0.03
    kwargs = dict(transform = ax1.transAxes, color = 'k', clip_on = False)
    ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)
    ax1.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)
    kwargs.update(transform = ax2.transAxes)
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
    ax2.plot((-d, +d), (-d, +d), **kwargs)

    # Save the figure to a file.
    f.suptitle('Measured on ' + str(activeCores) + ' core(s), ' + machine)
    f.subplots_adjust(wspace = 0.07)
    f.savefig(machine + '_' + str(activeCores) + 'active-cores.pdf')

    return

def plotCDFAndGetDists(fileName):
    """Plot a cdf for every configuration in the supplied file, and return the
       median and 99.9th percentile measurements for each configuration.

    @param fileName: The name of the file containing data to be plotted.
    @type  fileName: C{str}

    @return: A list containing the median and 99.9th percentile for each
             configuration. Each element of the returned list is itself a
             list of the form [machineType, activeCores, median, 99.9th].
             machineType and activeCores together represent the experimental
             configuration.
    """
    # Will hold the median and tail context switch time for each configuration
    # in the input file.
    dists = []

    with gzip.open(fileName, 'r') as dataFile:
        reader = csv.reader(dataFile, delimiter = ' ')

        # Get the samples and experiment configuration(s).
        samples = [row for row in reader \
                   if row[0] != '#' and row[0] != 'ExptId']
        machine = samples[0][columnDict["Machine"]]
        activeCoreSet = set([sample[columnDict["ActiveCores"]] \
                            for sample in samples])
        cyclesPerSec = float(samples[0][columnDict["CyclesPerSec"]])

        # For each configuration, plot and save an rcdf of context switch
        # times.
        for activeCores in activeCoreSet:
            # Collect only the context switch time for this configuration.
            data = [(float(sample[columnDict["Cycles"]]) * 1e6) / cyclesPerSec \
                    for sample in samples \
                    if sample[columnDict["ActiveCores"]] == activeCores]
            data.sort()

            # Get the median and tail of the data.
            median = data[(len(data) + 1) / 2] if len(data) % 2 == 1 else \
                     (data[len(data) / 2] + data[(len(data) + 1) / 2]) / 2
            tail999 = data[(len(data) * 999) / 1000]
            dists.append([machine, activeCores, median, tail999])

            # Plot a cdf of context switch time for this configuration.
            plotConfigCDF(data, machine, activeCores)

    return dists

def plotMedianAnd999(dists):
    """Plot the median and 99.9th latency for each provided configuration.

    @param dists: A list consisting of the median and 99.9th two way context
                  switch time for each experimental configuration to be plotted.
                  Each element of the list is itself a list of the form
                  [machine, activeCores, median, 99.9th], where machine and
                  activeCores represent an experimental configuration.
    """
    # Get xaxis ticks and labels
    configList = [dist[0] + '\n' + dist[1] + ' active core(s)' \
                  for dist in dists]
    xaxis = numpy.arange(len(configList))

    # Get the data to be plotted.
    medianList = [dist[2] for dist in dists]
    tail999List = [dist[3] for dist in dists]

    # Plot a bar graph.
    plt.close()
    plt.bar(xaxis-0.2, medianList, color='r', width=0.2, align='center', \
            label='Median')
    plt.bar(xaxis, tail999List, color='b', width=0.2, align='center', \
            label='99.9th')
    plt.xticks(xaxis, configList)
    plt.ylabel('Time to Context Switch Out and Back In (' + r'$\mu$' + 'sec)')
    plt.legend()

    plt.savefig('median_tail999.pdf')

    return

def plotAllGraphs():
    """Plot cdf, median, and 99.9th measurements from data files present
       in the working directory.
    """
    # The median and tail context switch times for each configuration.
    medianAnd999Dists = []

    # Plot cdfs and get the median and 99.9th measurements for each
    # configuration.
    for fileName in glob.glob('samples*.data.gz'):
        dists = plotCDFAndGetDists(fileName)
        [medianAnd999Dists.append(dist) for dist in dists]

    # Print out the median and tail context switch times.
    print medianAnd999Dists

    # Plot all the median and and 99.9th context switch times.
    plotMedianAnd999(medianAnd999Dists)

    return

if __name__ == "__main__":
    # Generate all the graphs.
    plotAllGraphs()

    sys.exit(0)
