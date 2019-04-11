#!/usr/bin/python
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

import argparse
import subprocess
import pprint
import logging
import sys
import time
import os

# local imports
import execute as ex
import cluster as cl


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Setup a machine for Sandstorm')

    parser.add_argument('-v', '--verbose',
                        help='Logging level. 10 for debug',
                        nargs='?',
                        type=int,
                        default=30,
                        const=20,
                        choices=range(0, 51),
                        metavar='lvl')

    parser.add_argument('-b', '--branch',
                        help='Specifies branch to be used. Defaults to master. Flag without variable for current branch',
                        nargs='?',
                        default='master',
                        const='current',
                        metavar='brch')

    parser.add_argument('--push',
                        help='push local changes which are commited',
                        action='store_false',
                        default=True)

    parser.add_argument('--wipe',
                        help='wipe the repository before building (rm splinter)',
                        action='store_true',
                        default=False)

    parser.add_argument('--setup',
                        help='setup the cluster (clone, setup.py, etc.)',
                        action='store_true',
                        default=False)

    parser.add_argument('--build',
                        help='build splinter on the cluster (pull, make)',
                        action='store_true',
                        default=False)

    parser.add_argument('user',
                        help='the user for ssh',
                        metavar='user')

    parser.add_argument('host',
                        help='any host in the cluster. e.g. "hp174.utah.cloudlab.us"',
                        metavar='host')

    subparsers = parser.add_subparsers(
        title="Command",
        description='run a command',
        dest='command')

    parserBench = subparsers.add_parser('bench',
                                        help='benchmark Sandstorm')

    parserYCSB = subparsers.add_parser('ycsb',
                                       help='run YCSB on Sandstorm')
    parserYCSB.add_argument('min',
                            help='Minimum request rate of the YCSB clients (in thousands)',
                            nargs='?',
                            type=int,
                            default='250')
    parserYCSB.add_argument('max',
                            help='Maximum request rate of the YCSB clients (in thousands)',
                            nargs='?',
                            type=int,
                            default='1500')
    parserYCSB.add_argument('delta',
                            help='delta between tested request rates of the YCSB clients (in thousands)',
                            nargs='?',
                            type=int,
                            default='125',
                            metavar='dlt')

    parserAuth = subparsers.add_parser('auth',
            help='run auth on Sandstorm')
    # parserAuth.add_argument('min',
    #                         help='Minimum request rate of the YCSB clients (in thousands)',
    #                         nargs='?',
    #                         type=int,
    #                         default='250')

    parserKill = subparsers.add_parser('kill',
                                       help='kill clients and server running Sandstorm')

    args = parser.parse_args()

    # Setup Logging
    logDir = time.strftime("%Y-%m-%d_%H:%M:%S")
    logCurrent = "./logs/"+logDir
    try:
        os.makedirs(logCurrent)
    except FileExistsError:
        pass  # directory already exists
    logging.basicConfig(filename=logCurrent+'/everything.log', level=logging.DEBUG)
    logger = logging.getLogger('')
    logger.setLevel(args.verbose)

    # Setup Cluster
    try:
        cluster = cl.Cluster(args.user, args.host)
        cluster.setupLogger(logCurrent, args.verbose)
    except Exception as e:
        logger.error("Could not establish cluster information!")
        logger.error(e.msg)
        exit(1)
    cluster.dump(logging.INFO)
    cluster.checkAuth()

    # Setup symlink
    logLatest = "./logs/latest"
    try:
        os.makedirs(logLatest)
    except FileExistsError:
        pass  # directory already exists
    ex.execute("cd {0}; ln -sf ../{1}/* ./".format(logLatest, logDir), logger)

    branch = args.branch
    if branch == 'current':
        branch = subprocess.check_output('git rev-parse --abbrev-ref HEAD', shell=True).decode("UTF-8").strip()

    if args.push:
        logger.info("Pushing local changes..")
        # TODO @jmbarzee check that there are no unstaged changes
        ex.execute("git push", logger)

    if args.wipe:
        cluster.wipe()

    if args.setup:
        cluster.setup(branch)

    if args.build:
        cluster.build(branch)

    cmd = args.command
    if cmd == 'ycsb':

        rates = range(args.min*1000, args.max*1000+1, args.delta*1000)

        cluster.startServer()
        cluster.runYCSB(rates)
        # cluster.killServer()
    if cmd == 'auth':
        cluster.startServer()
        cluster.runAuth()
        # cluster.killServer()

    elif cmd == 'kill':
        # TODO @jmbarzee logic to know if we should kill the extension
        cluster.kill(args.extension)
        # cluster.killServer() //TODO make this work

    elif cmd == 'bench':
        raise NotImplementedError()

    else:
        raise NotImplementedError()
