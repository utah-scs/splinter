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
import xml.etree.ElementTree as ET
import multiprocessing.pool as pool


class SubprocessException(Exception):
    def __init__(self, cmd, returnCode):
        self.cmd = cmd
        self.returnCode = returnCode


class RemoteSubprocessException(SubprocessException):
    def __init__(self, host, cmd, returnCode):
        self.host = host
        super(RemoteSubprocessException, self).__init__(cmd, returnCode)


def run(cmd, logger):
    process = subprocess.Popen(cmd,
                               shell=True,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               close_fds=True)
    pout, perr = process.communicate()
    logProcess(logger, cmd, pout, perr)

    if process.returncode:
        raise SubprocessException(cmd, process.returncode)


def logProcess(logger, cmd, pout, perr):
    logger.debug(cmd)

    poutString = pout.decode("UTF-8")
    if poutString != '':
        logger.debug(poutString)

    perrString = perr.decode("UTF-8")
    if perrString != '':
        logger.error(perrString)


class Host(object):
    def __init__(self, name, id, hostName):
        self.__name = name
        self.__id = id
        self.__hostName = hostName

    def getPublicName(self):
        return self.__name + '.utah.cloudlab.us'

    def dump(self, level):
        logger.log(level, '\t\tName: ' + self.__name)
        logger.log(level, '\t\tId: ' + self.__id)
        logger.log(level, '\t\thostName: ' + self.__hostName)

    def ssh(self, logger, user, cmd):
        cmd = 'ssh {0}@{1} {2}'.format(user, self.getPublicName(), cmd)
        process = subprocess.Popen(cmd,
                                   shell=True,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   close_fds=True)
        pout, perr = process.communicate()
        logProcess(logger, cmd, pout, perr)

        if process.returncode:
            raise RemoteSubprocessException(self, cmd, process.returncode)


class Cluster(object):
    def __init__(self, logger, user, server):
        self.__logger = logger
        self.__server = None
        self.__clients = []
        self.__user = user

        cmd = 'ssh {0}@{1} /usr/bin/geni-get manifest'.format(
            self.__user, server)
        process = subprocess.Popen(cmd,
                                   shell=True,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   close_fds=True)
        pout, perr = process.communicate()
        logProcess(self.__logger, cmd, bytes(), perr)

        root = ET.fromstring(pout.decode("UTF-8"))
        for xmlNode in list(root):
            if not xmlNode.tag.endswith('node'):
                continue

            for child in list(xmlNode):
                if child.tag.endswith('host'):
                    hostName = child.get('name')

                if child.tag.endswith('vnode'):
                    name = child.get('name')

            id = xmlNode.get('client_id')
            if id == 'server' and self.__server == None:
                self.__server = Host(name, id, hostName)
            else:
                self.__clients.append(Host(name, id, hostName))
        if server == None:
            raise Exception("No server found!")

    def dump(self, level):
        self.__logger.log(level, '1 server:')
        self.__server.dump(level)

        self.__logger.log(level, '{} clients:'.format(len(self.__clients)))
        for i, client in enumerate(self.__clients):
            self.__logger.log(level, '\t{}:'.format(i))
            client.dump(level)

    def checkAuth(self):
        self.__server.ssh(self.__logger, '-o StrictHostKeyChecking=no {}'.format(self.__user), '')

        for i, client in enumerate(self.__clients):
            client.ssh(self.__logger, '-o StrictHostKeyChecking=no {}'.format(self.__user), '')

    def __executeOnClients(self, cmd):
        tpool = pool.ThreadPool(processes=len(self.__clients))
        async_results = []
        for client in self.__clients:
            c = client
            def wrapSSHFunc(client, user, cmd, logger):
                try:
                    client.ssh(logger, user, cmd)
                except SubprocessException as e:
                    return e

            async_result = tpool.apply_async(
                wrapSSHFunc, (client, self.__user, cmd, self.__logger))
            async_results.append(async_result)

        for async_result in async_results:
            return_val = async_result.get()
            if return_val == None:
                continue
            elif isinstance(return_val, SubprocessException):
                raise return_val
            elif isinstance(return_val, Exception):
                raise return_val
            else:
                raise Exception(
                    'Unrecognized return value from ssh: {0}'.format(str(return_val)))

    def __executeOnServer(self, cmd):
        self.__server.ssh(self.__logger, self.__user, cmd)

    def setup(self, branch):
        try:
            logger.info("Server setup started...")
            self.__executeOnServer('"git clone https://github.com/utah-scs/splinter.git"')
            self.__executeOnServer('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnServer('"cd splinter; ./scripts/setup.py --full > /dev/null 2>&1"')  # beware, dirty trick. nic_info is local
            self.__executeOnServer('"cd splinter; cat nic_info" > nic_info')

            pci = subprocess.check_output("awk '/^pci/ { print $2; }' < nic_info", shell=True).decode("UTF-8").rstrip()
            if not pci:
                raise Exception("Failed to gather pci!")

            mac = subprocess.check_output("awk '/^mac/ { print $2; }' < nic_info", shell=True).decode("UTF-8").rstrip()
            if not mac:
                raise Exception("Failed to gather mac!")

            self.__executeOnServer('"cd splinter; cp db/server.toml-example db/server.toml; \
                                 sed -E -i \'s/01:02:03:04:05:06/' + mac + '/;\' db/server.toml; \
                                 sed -E -i \'s/0000:04:00.1/' + pci + '/;\' db/server.toml"')
            self.__logger.info('Server setup concluded.')

        except Exception as e:
            self.__logger.error('Server setup failed!')
            self.__logger.error(str(e))
            exit(1)

        try:
            self.__logger.info("Clients setup started...")
            self.__executeOnClients('"git clone https://github.com/utah-scs/splinter.git"')
            self.__executeOnClients('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnClients('"cd splinter; ./scripts/setup.py --full > /dev/null 2>&1"')

            self.__executeOnClients('"cd splinter; \
                                 echo "server_mac: ' + mac + '" >> nic_info; \
                                 ./scripts/create-client-toml"')

            self.__logger.info('Clients setup concluded.')

        except Exception as e:
            self.__logger.error('Clients setup failed!')
            self.__logger.error(str(e))
            exit(1)

    def wipe(self,):
        try:
            self.__logger.info("Server wipe started...")
            self.__executeOnServer('"rm -rf splinter"')
            self.__logger.info("Server wipe concluded...")

        except Exception as e:
            self.__logger.error('Server wipe failed!')
            self.__logger.error(str(e))
            exit(1)

        try:
            self.__logger.info("Clients wipe started...")
            self.__executeOnClients('"rm -rf splinter"')
            self.__logger.info("Clients wipe concluded...")

        except Exception as e:
            self.__logger.error('Server wipe failed!')
            self.__logger.error(str(e))
            exit(1)

    def build(self, branch):
        try:
            self.__logger.info("Server build started...")
            self.__executeOnServer('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnServer('"cd splinter; git pull"')
            self.__executeOnServer('"cd splinter; source ~/.cargo/env; make > /dev/null 2>&1;"')
            self.__logger.info("Server build concluded...")

        except Exception as e:
            self.__logger.error('Server build failed!')
            self.__logger.error(str(e))
            exit(1)

        try:
            self.__logger.info("Clients build started...")
            self.__executeOnClients('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnClients('"cd splinter; git pull"')
            self.__executeOnClients('"cd splinter; source ~/.cargo/env; make > /dev/null 2>&1;"')
            self.__logger.info("Clients build concluded...")

        except Exception as e:
            self.__logger.error('Clients build failed!')
            self.__logger.error(str(e))
            exit(1)

    def startServer(self):
        try:
            self.__logger.info("Server start started...")
            self.__executeOnServer('"cd splinter; sudo scripts/run-server"')
            self.__logger.info("Server start concluded...")

        except Exception as e:
            self.__logger.error('Server start failed!')
            self.__logger.error(str(e))
            exit(1)

    def killServer(self):
        # TODO @jmbarzee implement kill server
        raise NotImplementedError()

    def runYCSB(self, rates):
        # TODO @jmbarzee add flexibility to extensions
        try:
            for rate in rates:
                self.__logger.info("run YSCB started...")
                self.__executeOnClients('"cd splinter; sudo ./scripts/run-ycsb {0}"'.format(rate))
                self.__logger.info("run YSCB concluded...")

        except Exception as e:
            self.__logger.error('run YSCB failed!')
            self.__logger.error(str(e))
            exit(1)

    def killClients(self, ext):
        try:
            self.__logger.info("Clients kill started...")
            self.__executeOnServer('"sudo kill -9 `pidof {0}`"'.format(ext))
            self.__logger.info("Clients kill concluded...")

        except Exception as e:
            self.__logger.error('Clients kill failed!')
            self.__logger.error(str(e))
            exit(1)

    def bench(self):
        # TODO @jmbarzee implement bench
        # TODO @jmbarzee configure logging correctly
        # TODO @jmbarzee symlink latest (.log, .extract)
        raise NotImplementedError()


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
                        help='Specifies branch to be used.',
                        nargs='?',
                        default='master',
                        const='current',
                        metavar='brch')

    parser.add_argument('--wipe',
                        help='wipe the repository before building (rm splinter)',
                        action='store_true',
                        default=False)

    parser.add_argument('--setup',
                        help='setup the cluster (clone, setup.py, etc.)',
                        action='store_true',
                        default=False)

    parser.add_argument('--push',
                        help='push local changes which are commited',
                        action='store_false',
                        default=True)

    parser.add_argument('--build',
                        help='build splinter on the cluster (pull, make)',
                        action='store_false',
                        default=True)

    parser.add_argument('user',
                        help='the user for ssh',
                        metavar='user')

    parser.add_argument('server',
                        help='the server of the cluster. e.g. "hp174.utah.cloudlab.us"',
                        metavar='server')

    parser.add_argument('command',
                        help='instructions for the script [run|kill|bench]',
                        choices=['run', 'kill', 'bench'],
                        metavar='command')

    args, remainingArgs = parser.parse_known_args()

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

    # Setup symlink
    logLatest = "./logs/latest"
    try:
        os.makedirs(logLatest)
    except FileExistsError:
        pass  # directory already exists
    run("cd {0}; ln -sf ../{1}/* ./".format(logLatest, logDir), logger)

    # Setup Cluster
    try:
        cluster = Cluster(logger, args.user, args.server)
    except Exception as e:
        logger.error("Could not establish cluster information!")
        cluster.dump(logging.ERROR)
        exit(1)
    cluster.dump(logging.INFO)
    cluster.checkAuth()

    if args.push:
        logger.info("Pushing local changes..")
        # TODO @jmbarzee check that there are no unstaged changes
        run("git push", logger)

    if args.wipe:
        cluster.wipe()

    if args.setup:
        cluster.setup(args.branch)

    if args.build:
        cluster.build(args.branch)

    cmd = args.command
    if cmd == 'run':
        subParser = argparse.ArgumentParser(
            description='run an extension on Sandstorm')
        subParser.add_argument('extension',
                            help='Specifies extension to be run',
                            choices=['ycsb'],
                            default='ycsb',
                            metavar='ext')
        subArgs, remainingSubArgs = subParser.parse_known_args(remainingArgs)

        if args.extension == 'ycsb':
            extParser = argparse.ArgumentParser(
                description='run an ycsb on Sandstorm')
            extParser.add_argument('min',
                                help='Minimum request rate of the YCSB clients (in thousands)',
                                type=int,
                                default='250')
            extParser.add_argument('max',
                                help='Maximum request rate of the YCSB clients (in thousands)',
                                type=int,
                                default='1500')
            extParser.add_argument('epsilon',
                                help='epsilon between tested request rates of the YCSB clients (in thousands)',
                                type=int,
                                default='125',
                                metavar='esp')
            extArgs = extParser.parse_args(remainingSubArgs)

            rates = range(extArgs.min*1000, extArgs.max*1000+1, extArgs.epsilon*1000)

            cluster.startServer()
            cluster.runYCSB(rates)
            cluster.killServer()

    elif cmd == 'kill':
        # TODO @jmbarzee check for --extension
        cluster.killClients(args.e)
        cluster.killServer()

    elif cmd == 'bench':
        raise NotImplementedError()

    else:
        raise NotImplementedError()
