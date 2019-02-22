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
import xml.etree.ElementTree as ET

def logProcess(cmd, pout, perr):
    logger.debug(cmd)

    poutString = pout.decode("UTF-8")
    if poutString != '':
        logger.debug(poutString)

    perrString = perr.decode("UTF-8")
    if perrString != '':
        logger.error(perrString)



class Host(object):
    def __init__(self, name, id, hostName):
        self.name = name
        self.id = id
        self.hostName = hostName

    def getPublicName(self):
        return self.name + '.utah.cloudlab.us'

    def dump(self, level):
        logger.log(level, '\t\tName: ' + self.name)
        logger.log(level, '\t\tId: ' + self.id)
        logger.log(level, '\t\thostName: ' + self.hostName)
    
    def ssh(self):
        raise NotImplementedError



class Cluster(object):
    def __init__(self, user, server):
        self.server = None
        self.clients = []

        cmd = 'ssh {0}@{1} /usr/bin/geni-get manifest'.format(user, server)
        process = subprocess.Popen(cmd,
                             shell=True,
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             close_fds=True)
        pout, perr = process.communicate()
        logProcess(cmd, pout, perr)
        

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
            if id == 'server' and self.server == None:
                self.server = Host(name, id, hostName)
            else:
                self.clients.append(Host(name, id, hostName))
        if server == None:
            raise Exception("No server found!")

    def dump(self, level):
        logger.log(level, '1 server:')
        self.server.dump(level)

        logger.log(level, '{} clients:'.format(len(self.clients)))
        for i, client in enumerate(self.clients):
            logger.log(level, '\t{}:'.format(i))
            client.dump(level)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Setup a machine for Sandstorm')

    parser.add_argument('-v',
                        help='Logging level. 10 for debug',
                        nargs='?',
                        type=int,
                        default=30,
                        const=20,
                        choices=range(0, 51),
                        metavar='lvl')

    parser.add_argument('-b',
                        help='Specifies branch to be used.',
                        nargs='?',
                        default='master',
                        const='current',
                        metavar='brch')

    parser.add_argument('user',
                        help='the user for ssh',
                        metavar='usr')

    parser.add_argument('server',
                        help='the server of the cluster. e.g. "hp174.utah.cloudlab.us"',
                        metavar='svr')

    args = parser.parse_args()

    logging.basicConfig(filename='test.log', level=logging.DEBUG)
    logger = logging.getLogger('')
    logger.setLevel(args.v)

    try:
        cluster = Cluster(args.user, args.server)
    except Exception as e:
        logger.error("Could not establish cluster information!")
        cluster.dump(logging.ERROR)
        exit(1)

    cluster.dump(logging.INFO)
