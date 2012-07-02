#!/usr/bin/env python

import sys
import time

print "starting ..."

for i in range(20):
    print "sleeping %s ... " % i
    #sys.stdout.flush()
    #sys.stderr.flush()
    time.sleep(1)

print "done"    
