#!/usr/bin/env python

import os
import sys
import imp
import boto
import time
from glob import glob

def checkAndCloseExpiredInstances(results_directory):
    #common = imp.load_module('common', *imp.find_module('common', [os.path.join(os.path.dirname(__file__), '..', 'cassandralauncher')]))
    import common
    config, KEY_PAIR, PEM_HOME, HOST_FILE, PEM_FILE = common.header()
    conn = boto.connect_ec2(config.get('EC2', 'aws_access_key_id'), config.get('EC2', 'aws_secret_access_key'))
    reservations = dict(map(lambda x: (x.id, x), conn.get_all_instances()))
    result_files = glob(os.path.join(results_directory, '*.results'))
    
    print 'checking %s' % result_files
    for result_file in result_files:
        results = None
        with open(result_file, 'r') as f:
            results = dict(map(lambda l: l.strip().split('='), f.readlines()))

        if time.time() - float(results['launch_time']) > float(results['ttl_seconds']):
            res = reservations.get(results['reservation_id'])
            if res != None:
                print 'killing %s' % res.id
                instances = [i.id for i in res.instances if i.state != 'terminated']
                if len(instances) > 0:
                    conn.terminate_instances(instances)
            else:
                os.rename(result_file, '%s.%s' % (result_file, 'done'))

def cli_main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-r', '--results_directory', dest='results_directory', help='direcotry to search for running clusters', metavar='RESULTS_DIRECTORY', default='/tmp/wl')
    (options, args) = parser.parse_args()
    checkAndCloseExpiredInstances(options.results_directory)    
    
if __name__ == '__main__':
    cli_main()
