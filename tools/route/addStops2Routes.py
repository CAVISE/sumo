#!/usr/bin/env python
# Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.org/sumo
# Copyright (C) 2010-2021 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the Eclipse
# Public License 2.0 are satisfied: GNU General Public License, version 2
# or later which is available at
# https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later

# @file    addStops2Routes.py
# @author  Yun-Pang Floetteroed
# @date    2019-04-25

"""
add stops to vehicle routes
"""
from __future__ import absolute_import
from __future__ import print_function
import os
import sys
import optparse

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sumolib.output import parse, parse_fast  # noqa
import sumolib  # noqa


def get_options(args=None):
    optParser = optparse.OptionParser()
    optParser.add_option("-n", "--net-file", dest="netfile",
                         help="define the net filename")
    optParser.add_option("-r", "--route-files", dest="routefiles",
                         help="define the route file seperated by comma(mandatory)")
    optParser.add_option("-o", "--output-file", dest="outfile",
                         help="define the output filename")
    optParser.add_option("-t", "--typesfile", dest="typesfile",
                         help="Give a typesfile")
    optParser.add_option("-d", "--duration",
                         help="Give a time, how long the vehicle stands")
    optParser.add_option("-u", "--until",
                         help="specify a time until the vehicle is parked")
    optParser.add_option("-p", "--parking", dest="parking", action="store_true",
                         default=False, help="where is the vehicle parking")
    optParser.add_option("--parking-areas", dest="parkingareas",
                         default=False, help="load parkingarea definitions and stop at parkingarea on the arrival edge if possible")
    optParser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                         default=False, help="tell me what you are doing")

    (options, args) = optParser.parse_args(args=args)

    if not options.routefiles or not options.netfile or not options.outfile or not options.typesfile:
        optParser.print_help()
        sys.exit("input file missing")

    if not options.duration and not options.until:
        optParser.print_help()
        sys.exit("stop duration or until missing")
    return options


def readTypes(options):
    vtypes = {None: "passenger"}
    for file in options.typesfile.split(','):
        for vtype in sumolib.output.parse(file, 'vType'):
            vtypes[vtype.id] = vtype.vClass
    # print(vtypes)
    return vtypes


def main(options):

    edge2parking = {}
    if options.parkingareas:
        for pafile in options.parkingareas.split(','):
            for pa in sumolib.xml.parse(pafile, "parkingArea"):
                edge = '_'.join(pa.lane.split('_')[:-1])
                edge2parking[edge] = pa.id

    # with io.open(options.outfile, 'w', encoding="utf8") as outf:
    # with open(options.outfile, 'w', encoding="utf8") as outf:
    with open(options.outfile, 'w') as outf:
        net = sumolib.net.readNet(options.netfile)
        vtypes = readTypes(options)
        sumolib.writeXMLHeader(outf, "$Id: addStops2Routes.py v1_3_1+0411-36956f96df michael.behrisch@dlr.de 2019-09-21 21:10:12 +0200 $", "routes")  # noqa
        numSkipped = 0
        for file in options.routefiles.split(','):
            for veh in sumolib.output.parse(file, 'vehicle'):
                edgesList = veh.route[0].edges.split()
                lastEdge = net.getEdge(edgesList[-1])
                lanes = lastEdge.getLanes()
                skip = False
                stopAttrs = {}
                if options.parkingareas:
                    if lastEdge.getID() in edge2parking:
                        stopAttrs["parkingArea"] = edge2parking[lastEdge.getID()]
                    else:
                        skip = True
                        numSkipped += 1
                        print("Warning: no parkingArea found on edge '%s' for vehicle '%s'" % (
                            lastEdge.getID(), veh.id), file=sys.stderr)
                else:
                    # find usable lane
                    for lane in lanes:
                        if lane.allows(vtypes[veh.type]):
                            stopAttrs["lane"] = lane.getID()
                            break

                if options.parking:
                    stopAttrs["parking"] = "true"
                if options.duration:
                    stopAttrs["duration"] = options.duration
                if options.until:
                    stopAttrs["until"] = options.until
                if not skip:
                    veh.addChild("stop", attrs=stopAttrs)

                outf.write(veh.toXML(' '*4))
        outf.write('</routes>\n')
    outf.close()

    if numSkipped > 0:
        print("Warning: No stop added for %s vehicles" % numSkipped)


if __name__ == "__main__":
    options = get_options(sys.argv)
    main(options)
