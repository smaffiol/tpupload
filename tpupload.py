#!/usr/bin/env python
#
#   Copyright (C) 2018, 2019 - bodenmillerlab, University of Zurich
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

"""
Utility to transfer data from UZH resources to ETH LeoMed.
Procedure:
- Within the scope of TumorProfiler, each new dataset to be uploaded should provide one or more list of files that will be uploaded.
- Each file should contain only the content of a single folder from the TumorProfiler agreed: 'raw', 'derived', 'vsrs'
- File name convention: [method]__[folder].txt
Example: IMC__derived.txt
.All_Cells_I.csv
.All_Experiment_I.csv
.All_Image_I.csv
...

- Run `tpupload` providing one or more files as above (named 'filters')
- `tpupload` will:
- parse filters filename to derive what folder the filter refers to ('raw', 'derived', 'vsrs')
- read the content of each filter and find the corresponding file in the corresponding folder
- creates a tempdir and makes symlinks for each file
- if run in dryrun mode, simply logs the rsync command to run afterwards
- else, try to run the rsync command directly

Usage:
tpupload [-h] [-d] [-n VPN] [-i IDENTITY] [-e SERVERNAME] [-v]
                source destination [filters [filters ...]]

example usage:
$ tpupload /data/IMC/PoC/PoC_4 /cluster/work/tumorp/dropbox/imc/USZ-M-9/PZT-MOHUA-2/ ./IMC__derived.txt ./IMC__raw.txt ./IMC__vsrs.txt ./USZ-M-9.MOHUA-TS.I__raw_files.txt

Assumptions:
1. VPN connection to ETH should already be established
2. user running the script should already have an account to LeoMed and have verified the 2-factor authentication as well as have the right ssh-key being deployed on LeoMed
3. user running the script should have the private ssh-key deployed on the ScienceCloud node where the script will be running
4. user running the script should already have ssh configuration to connect to LeoMed (via the jump-host) 

"""

import sys
import os
import argparse
import tempfile

import sh
import logging
import logging.config
logging.basicConfig()
log = logging.getLogger()
log.propagate = True


# Defaults
dryrun=0
ssh_identity="~/.ssh/id_rsa"
tp_server_name="leomed"

# Utility methods

def generate_rsync_list(files_to_upload):
    """
    take list of tuples and create an rsync file list.
    to be passed to '--files-from' option.
    """

    dirpath = tempfile.mkdtemp()
    os.mkdir(os.path.join(dirpath,"derived"))
    os.mkdir(os.path.join(dirpath,"raw"))
    os.mkdir(os.path.join(dirpath,"vsrs"))
              
    with open(os.path.join(dirpath,'md5sum.txt'), 'w') as md:
        for (source,fname,chksum,dest) in files_to_upload:
            # create simlink
            log.debug("creating symlink for {0} in {1}".format(source,
                                                               os.path.join(dirpath,fname)))
            try:
                
                os.symlink(source, os.path.join(dirpath,fname))
                md.write("{0},{1}\n".format(fname,
                                            chksum))
            except OSError as osx:
                log.error("Failed to create symlinlk for {0}: '{1}'".format(source,
                                                                            osx))
                pass
    return (dirpath,md.name)

def get_checksum(fname):
    import hashlib
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        hash_md5.update(f.read())
    return hash_md5.hexdigest()

def get_files(source, filter):
    """
    1. Read filter filename and extract folder name reference
    filename format: cytof_<folder>.txt or cytof_<folder>_files.txt
    2. Read content of filename and search for matching
    in source/<fodler>
    for each found file, compute checksum and add to return
    list. Each element of the list containes a tuple (source,file name,checksum,destination)
    3. for each unfound file, add to the list of unfound files
    return both lists
    """

    found = []
    
    filename = os.path.basename(filter)
    folder_name = filename.split("__")[1].split(".")[0].split("_")[0]
    log.debug("from file {0} extracted folder name {1}.".format(filename,
                                                                folder_name))
    log.debug("reading content of filter {0}".format(filter))

    with open(filter,'r') as fd:
        # Read all files that should be found and transferred
        filter_list = [line.rstrip('\n')[:-1] for line in fd]
    
    for r,d,f in os.walk(os.path.join(source,folder_name)):
        for data in f:
            for filter_file in filter_list:
                if filter_file in data:
                    found.append(
                        (os.path.join(r,data),
                         os.path.join(folder_name,data),
                         get_checksum(os.path.join(r,data)),
                         folder_name))
                    filter_list.remove(filter_file)

    # filter_list should be empty by now.
    return found, filter_list
    
def main(source, destination, filters, ssh_identity, tp_server_name, dryrun):
    """
    Run the main workflow:
    * Building map of source files to be uploaded by reading input .txt files from `filters`
    * create destination folder with symlinks to original data
    * For each valid input file create a checksum to be included in a checksum file
    * Run rsync to send data to remote destination.
    """
    log.info("Transferring data from {0} to {1}:{2}".format(source,
                                                            tp_server_name,
                                                            destination))

    log.debug("Building map of source files to be uploaded")
    files_to_upload = []

    for filter in filters:
        found,missing = get_files(source, filter)
        # If `missing` is not empty, something went wrong. Stop here
        assert len(missing) == 0, "Files {0} not found when reading {1}".format(missing,
                                                                                filter)
        files_to_upload += found

    dir_location,checksum_file = generate_rsync_list(files_to_upload)

    if dryrun:
        log.info("DBG: rsync -rt --copy-links -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' --progress {0}/ {1}:{2}.".format(dir_location,
                                                                                                                                                    tp_server_name,
                                                                                                                                                    destination))
    else:
        try:
            sh.rsync("-rt","--copy-links","-e","ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
                     "--progress",
                     "{0}/".format(dir_location),
                     "{0}:{1}".format(tp_server_name,destination)
            )
        except sh.ErrorReturnCode as ex:
            log.error("Failed running rsync. Error {0}".format(ex))
    
    log.info("Done")

if __name__ == "__main__":
    # Setup the command line arguments
    parser = argparse.ArgumentParser(
        description='', prog='tpupload')

    parser.add_argument('source', type=str,
                        help='Source folder to upload.')

    parser.add_argument('destination', type=str,
                        help='Destination folder to upload.')

    parser.add_argument('filters', nargs='*',
                        help="filter files of type '[a-A]__[folder_name].txt' or '[a-A]__[folder_name]_[a-A].txt'")
   
    parser.add_argument('-d','--dryrun',
                        action='store_true',
                        default=False,
                        help='Enable dryrun. Default: %(default)s.')

    parser.add_argument('-i','--identity',
                        type=str,
                        default=ssh_identity,
                        help='Selects a file from which the identity (private key) '
                        'for public key authentication is read. It will be passed '
                        'to the underneath ssh command. Default: %(default)s.')

    parser.add_argument('-e','--servername',
                        type=str,
                        default=tp_server_name,
                        help='Connect to TumorProfiler server using named jumphost '
                        'as specified in ssh config. Default: %(default)s.')

    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    assert os.path.isdir(args.source), "Source folder {0} not found".format(args.source)
    for data in args.filters:
        assert os.path.isfile(data), "Filter file {0} not found".format(data)
        
    sys.exit(main(args.source, args.destination, args.filters, args.identity, args.servername, args.dryrun))
