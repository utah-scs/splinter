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

# local imports
import execute as ex
import logpipe as lp


class Host(object):
    def __init__(self, name, id, hostName):
        self.__name = name
        self.__id = id
        self.__hostName = hostName
        self.__logger = logging.getLogger("cluster." + self.__id)

    def setupLogger(self, logDirName, level):
        logFile = '{}/{}.log'.format(logDirName, self.__id)
        handler = logging.FileHandler(logFile)
        self.__logger.setLevel(level)
        self.__logger.addHandler(handler)
        self.__logger.info("Begin Logging!")

    def getPublicName(self):
        return self.__name + '.utah.cloudlab.us'

    def dump(self, level, logger):
        self.__logger.log(level, '\t\tName: ' + self.__name)
        self.__logger.log(level, '\t\tId: ' + self.__id)
        self.__logger.log(level, '\t\thostName: ' + self.__hostName)

    def execute(self, user, cmd):
        cmd = 'ssh {0}@{1} {2}'.format(user, self.getPublicName(), cmd)
        ex.execute(cmd, self.__logger)


class HostPool(object):
    def __init__(self):
        self.__hosts = []

    def setupLogger(self, logDirName, level):
        for host in self.__hosts:
            host.setupLogger(logDirName, level)

    def dump(self, level, logger):
        for _, host in enumerate(self.__hosts):
            host.dump(level, logger)

    def addHost(self, host):
        self.__hosts.append(host)

    def execute(self, user, cmd):
        tpool = pool.ThreadPool(processes=len(self.__hosts))
        async_results = []
        for host in self.__hosts:
            def wrapExecute(host, user, cmd):
                try:
                    host.execute(user, cmd)
                except ex.SubprocessException as e:
                    return e

            async_result = tpool.apply_async(
                wrapExecute, (host, user, cmd))
            async_results.append(async_result)

        for async_result in async_results:
            return_val = async_result.get()
            if return_val == None:
                continue
            elif isinstance(return_val, ex.SubprocessException):
                raise return_val
            elif isinstance(return_val, Exception):
                raise return_val
            else:
                raise Exception(
                    'Unrecognized return value from ssh: {0}'.format(str(return_val)))


class Cluster(object):
    def __init__(self, user, server):
        self.__logger = logging.getLogger('cluster')
        self.__server = None
        self.__clients = HostPool()
        self.__user = user

        cmd = 'ssh {0}@{1} /usr/bin/geni-get manifest'.format(
            self.__user, server)
        errPipe = lp.LogPipe(self.__logger, logging.ERROR)
        completedProcess = subprocess.run(cmd,
                                          shell=True,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE,
                                          stderr=errPipe,
                                          close_fds=True)
        errPipe.close()

        root = ET.fromstring(completedProcess.stdout.decode("UTF-8"))
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
                self.__clients.addHost(Host(name, id, hostName))
        if server == None:
            raise Exception("No server found!")
        self

    def setupLogger(self, logDirName, level):
        self.__clients.setupLogger(logDirName, level)
        self.__server.setupLogger(logDirName, level)

        logFile = '{}/cluster.log'.format(logDirName)
        handler = logging.FileHandler(logFile)
        self.__logger.setLevel(level)
        self.__logger.addHandler(handler)
        self.__logger.info("Begin Logging!")

    def dump(self, level):
        self.__logger.log(level, 'server:')
        self.__server.dump(level, self.__logger)
        self.__logger.log(level, 'clients:')
        self.__clients.dump(level, self.__logger)

    def checkAuth(self):
        # this flag adds the server to the known hosts file automatically
        self.__server.execute('-o StrictHostKeyChecking=no {}'.format(self.__user), '')
        self.__clients.execute('-o StrictHostKeyChecking=no {}'.format(self.__user), '')

    def __executeOnServer(self, cmd):
        self.__server.execute(self.__user, cmd)

    def __executeOnClients(self, cmd):
        self.__clients.execute(self.__user, cmd)

    def setup(self, branch):
        # Setup clients and servers in parallel
        tpool = pool.ThreadPool(processes=2)
        serverRes = tpool.apply_async(self.__setupServer, (branch,))
        clientsRes = tpool.apply_async(self.__setupClients, (branch,))

        # Wait for both to complete
        serverRes.get()
        clientsRes.get()
        self.__setupNIC()

    def __setupServer(self, branch):
        try:
            self.__logger.info("Server setup started...")
            self.__executeOnServer('"git clone https://github.com/utah-scs/splinter.git"')
            self.__executeOnServer('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnServer('"cd splinter; ./scripts/setup.py --full > /dev/null 2>&1"')
            self.__logger.info('Server setup concluded.')

        except Exception as e:
            self.__logger.error('Server setup failed!')
            self.__logger.error(str(e))
            exit(1)

    def __setupClients(self, branch):
        try:
            self.__logger.info("Clients setup started...")
            self.__executeOnClients('"git clone https://github.com/utah-scs/splinter.git"')
            self.__executeOnClients('"cd splinter; git checkout {0}"'.format(branch))
            self.__executeOnClients('"cd splinter; ./scripts/setup.py --full > /dev/null 2>&1"')

            self.__logger.info('Clients setup concluded.')

        except Exception as e:
            self.__logger.error('Clients setup failed!')
            self.__logger.error(str(e))
            exit(1)

    def __setupNIC(self):
        try:
            self.__logger.info("NIC setup started...")
            self.__executeOnServer('"cd splinter; cat nic_info" > nic_info')  # beware, dirty trick. nic_info is local

            pci = subprocess.check_output("awk '/^pci/ { print $2; }' < nic_info", shell=True).decode("UTF-8").rstrip()
            if not pci:
                raise Exception("Failed to gather pci!")

            mac = subprocess.check_output("awk '/^mac/ { print $2; }' < nic_info", shell=True).decode("UTF-8").rstrip()
            if not mac:
                raise Exception("Failed to gather mac!")

            self.__executeOnServer('"cd splinter; cp db/server.toml-example db/server.toml; \
                                 sed -E -i \'s/01:02:03:04:05:06/' + mac + '/;\' db/server.toml; \
                                 sed -E -i \'s/0000:04:00.1/' + pci + '/;\' db/server.toml"')
            self.__executeOnClients('"cd splinter; \
                                 echo "server_mac: ' + mac + '" >> nic_info; \
                                 ./scripts/create-client-toml"')

            self.__logger.info('NIC setup concluded.')

        except Exception as e:
            self.__logger.error('NIC setup failed!')
            self.__logger.error(str(e))
            exit(1)

    def wipe(self):
        # Wipe clients and servers in parallel
        tpool = pool.ThreadPool(processes=2)
        serverRes = tpool.apply_async(self.__wipeServer, ())
        clientsRes = tpool.apply_async(self.__wipeClients, ())

        # Wait for both to complete
        serverRes.get()
        clientsRes.get()

    def __wipeServer(self):
        try:
            self.__logger.info("Server wipe started...")
            self.__executeOnServer('"rm -rf splinter"')
            self.__logger.info("Server wipe concluded...")

        except Exception as e:
            self.__logger.error('Server wipe failed!')
            self.__logger.error(str(e))
            exit(1)

    def __wipeClients(self):
        try:
            self.__logger.info("Clients wipe started...")
            self.__executeOnClients('"rm -rf splinter"')
            self.__logger.info("Clients wipe concluded...")

        except Exception as e:
            self.__logger.error('Server wipe failed!')
            self.__logger.error(str(e))
            exit(1)

    def build(self, branch):
        # Build clients and servers in parallel
        tpool = pool.ThreadPool(processes=2)
        serverRes = tpool.apply_async(self.__buildServer, (branch,))
        clientsRes = tpool.apply_async(self.__buildClients, (branch,))

        # Wait for both to complete
        serverRes.get()
        clientsRes.get()

    def __buildServer(self, branch):
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

    def __buildClients(self, branch):
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

    def kill(self, extension):
        # Kill clients and servers in parallel
        tpool = pool.ThreadPool(processes=2)
        serverRes = tpool.apply_async(self.__killServer, ())
        clientsRes = tpool.apply_async(self.__killClients, (extension,))

        # Wait for both to complete
        serverRes.get()
        clientsRes.get()

    def __killServer(self):
        # TODO @jmbarzee implement kill server
        raise NotImplementedError()

    def __killClients(self, ext):
        try:
            self.__logger.info("Clients kill started...")
            self.__executeOnServer('"sudo kill -9 `pidof {0}`"'.format(ext))
            self.__logger.info("Clients kill concluded...")

        except Exception as e:
            self.__logger.error('Clients kill failed!')
            self.__logger.error(str(e))
            exit(1)

    def startServer(self):
        try:
            self.__logger.info("Server start started...")
            self.__executeOnServer('"cd splinter; sudo scripts/run-server &"')
            self.__logger.info("Server start concluded...")

        except Exception as e:
            self.__logger.error('Server start failed!')
            self.__logger.error(str(e))
            exit(1)

    def runYCSB(self, rates):
        try:
            self.__logger.info("run YSCB started...")
            for rate in rates:
                self.__logger.info("\tYSCB({0})".format(rate))
                self.__executeOnClients('"cd splinter; sudo ./scripts/run-ycsb {0}"'.format(rate))
            self.__logger.info("run YSCB concluded...")

        except Exception as e:
            self.__logger.error('run YSCB failed!')
            self.__logger.error(str(e))
            exit(1)

    def bench(self):
        # TODO @jmbarzee implement bench
        # TODO @jmbarzee configure logging correctly
        # TODO @jmbarzee symlink latest (.log, .extract)
        raise NotImplementedError()
