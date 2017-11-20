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

"""
Allocate a single CloudLab machine to measure context switch time.
"""

import geni.urn as urn
import geni.portal as portal
import geni.rspec.pg as rspec
import geni.aggregate.cloudlab as cloudlab

# The possible set of base disk-images the machine can be booted with.
# The second field of every tupule is what is displayed on the cloudlab
# dashboard.
images = [("UBUNTU16-64-STD", "Ubuntu 16.04 (64-bit)")]

# The possible set of node-types that can be used for the experiment.
nodes = [ ("r320", "r320 (Xeon E5 2450, Sandy Bridge)"),
        ("c6220", "c6220 (Xeon E5 2650v2, Ivy Bridge)"),
        ("d430", "d430 (Xeon E5 2630v3, Haswell)"),
        ("m510", "m510 (Xeon D-1548, Broadwell)")]

# The disk on each machine.
disks = ["/dev/sdb"]

# Allows for general parameters like disk image to be passed in. Useful for
# setting up the cloudlab dashboard for this profile.
context = portal.Context()

# Default the disk image to 64-bit Ubuntu 16.04
context.defineParameter("image", "Disk Image",
        portal.ParameterType.IMAGE, images[0], images,
        "Specify the base disk image that the machine should be booted with.")

# Default the node type to the d430.
context.defineParameter("type", "Node Type",
        portal.ParameterType.NODETYPE, nodes[2], nodes,
        "Specify the type of nodes the cluster should be configured with. " +\
        "For more details, refer to " +\
        "\"http://docs.cloudlab.us/hardware.html#%28part._apt-cluster%29\"")

params = context.bindParameters()

request = rspec.Request()

# Create the node (call it "master").
node = rspec.RawPC("master")
node.hardware_type = params.type
node.disk_image = urn.Image(cloudlab.Utah, "emulab-ops:%s" % params.image)
request.addResource(node)

# Generate the RSpec
context.printRequestRSpec(request)
