"""This module allows to download the raw data
from the Eulerian Data Warehouse"""

import re
import time
import urllib
import gzip
import csv
import requests
import ijson
import sys
import os

from eanalytics_api_py.internal import _request, _log

#
# @brief Get session token from Eulerian Authority services.
#
# @param domain - Eulerian Authority domain.
# @param headers - Http headers.
# @param ip - host IP.
# @param log - Print log message.
#
# @return Session token.
#
def session( domain, headers, ip, log ) :
    url = f"{domain}/er/account/get_dw_session_token.json"
    payload = { 'ip' : ip }
    json = _request._to_json(
        request_type = "get",
        url = url,
        headers = headers,
        params = payload,
        print_log = log
    )
    return json[ 'data' ][ 'rows' ][ 0 ][ 0 ]
#
# @brief Compress given file.
#
# @param path_in - Input file path.
# @param path_out - Compressed file path.
#
def gzip_compress( path_in, path_out ) :
    try :
        f = open( path_in, "rb" )
        data = f.read()
        f.close()
    except IOError as e :
        print( str( e ) )
        raise e
    with gzip.open( filename = path_out, mode = "wb" ) as gzipfile :
        gzipfile.write( data )
        gzipfile.close()
#
# @brief Create a JOB on Eulerian Data Warehouse Platform.
#
# @param url - Url of Eulerian Data Warehouse Platform.
# @param headers - HTTP headers.
# @param query - Eulerian Data Warehouse Command.
# @param log - Print log message.
#
def job_create( url, headers, query, log ) :
    request = {
        "kind" : "edw#request",
        "query" : query
    }
    return _request._to_json(
        request_type = 'post',
        url = url,
        json_data = request,
        headers = headers,
        print_log = log
        )
#
# @brief Download JSON reply file of a JOB.
#
# @param url - URL of Eulerian Data Warehouse Job reply.
# @param reply - Last reply.
# @param headers - HTTP headers.
# @param directory - Output directory.
#
# @return JSON reply file path.
#
def job_download( conn, reply, headers, directory ) :
    uuid, url = reply[ 'data' ]
    reply = requests.get( url, headers = headers, stream = True )
    if reply.status_code != 200 :
        return [ None, None ]
    # prefix is an advice on which reply format we expect.
    if reply.headers[ 'Content-Type' ] == 'application/json' :
        prefix = 'json'
    elif reply.headers[ 'Content-Type' ] == 'text/plain' :
        prefix = 'parquet'
    elif reply.headers[ 'Content-Type' ] == 'text/csv' :
        prefix = 'csv'
    if directory and directory != '' :
        path = directory + '/' + str( uuid ) + '.' + prefix
    else :
        path = str( uuid ) + '.' + prefix
    stream = open( path, 'wb' )
    if stream is None :
        return [ None, None ]
    total = 0
    length = reply.headers[ 'Content-Length' ]
    for line in reply.iter_content( 8192 ) :
        total += len( line )
        conn._logrewind(
            "Write : " + str( len( line ) ) + "/" + unit( total ) + 
            "/" + unit( int( length ) )
            )
        stream.write( line )
    conn._log( "" )
    stream.close()
    return [ path, prefix ]
# 
# @brief Get JOB status.
#
# @param url - URL to Eulerian Data Warehouse JOB.
# @param headers - Http headers.
# @param log - Print log message.
#
# @return JSON reply
#
def job_status( url, headers, log ) :
    return _request._to_json(
        request_type = 'get',
        url = url,
        headers = headers,
        print_log = log
        )
#
# @brief Wait end of a JOB.
#
# @param reply - Reply to JOB creation.
# @param headers - HTTP headers.
# @param log - Print log message.
#
# @return Last reply
#
def job_wait( reply, headers, log ) :
    status = reply[ 'status' ]
    while status == 'Running' :
        uuid, url = reply[ 'data' ]
        time.sleep( 1 )
        # Get job status
        reply = job_status( url, headers, log )
        if reply is None :
            status = 'Error'
        else :
            status = reply[ 'status' ]
    return reply
#
# @brief Get human readable value.
#
# @param value - Input value.
#
# @return [ value, unit ]
#
def unit( value ) :
    iunit = 0
    units = [ '', 'K', 'M', 'G', 'T', 'P' ]
    while ( value / 1024.00 ) > 1.0 :
        iunit += 1
        value /= 1024
    return "{:.2f}".format( value ) + units[ iunit ]
#
# @brief Add a JOB on Eulerian Data Warehouse plateform, wait end of the JOB,
#        Download JSON reply, convert reply to CSV format then compress it.
#
# return Path to compressed file.
#
def download_edw(
    self,
    query: str,
    status_waiting_seconds=1,
    ip: str = None,
    output_path2file=None,
    accept="application/json",
    encoding="identity",
    override_file=False,
    compress=True,
    uuid=None,
) -> str:
    """ Fetch edw data from the API into a gzip compressed file

    Parameters
    ----------
    query: str, obligatory
        EDW query

     status_waiting_seconds: int, optional
        Waiting time in seconds between each status query

    ip: str, optional
        Coma separated ip values
        Default: Automatically fetch your external ip address

    output_path2file: str, optional
        path2file where the data will be stored
        If not set, the file will be created in the current
            working directory with a default name

    override_file : bool, optional
        If set to True, will override output_path2file (if exists)
            with the new datamining content
        Default: False

    compress : bool, optional
        If set to True, reply file is compressed

    accept : str, optional
        Specify expected reply output format ( application/json, 
         application/parquet, text/csv )

    encoding : str, optional
        Specify transport layer encoding ( identity, gzip )

    uuid : str, optional
        The job id to download directly from a previously requested jobrun

    Returns
    -------
    str
        The output_path2file containing the downloaded datamining data
    """

    # Parse query looking for request timerange
    request_begin = time.time()
    epochs_found = re.findall( r'{\W+?(\d+)\W+?(\d+)\W+?}', query )
    if not epochs_found :
        raise ValueError( 
            f"Could not read request timerange : \n{query}"
            )

    # Parse query looking for request readers
    readers_found = re.findall( r'(\w+):(\w+)@([\w_-]+)', query )
    if not readers_found :
        raise ValueError(
            f"Could not read READER from query=\n{query}"
            )

    readers = []
    for reader in readers_found :
        store, object, site = reader
        self._is_allowed_website_name( site )
        readers += [ store, object, site ]

    # Get accept reply format 
    format = accept.split( '/' )[ 1 ]

    if output_path2file :
        # Check that given reply file path prefix match accepted format
        prefix = output_path2file.split( '/' )[ -1 ].split( '.' )[ -1 ]
        if prefix != format :
            raise ValueError(
                f"Given file path prefix : {prefix} doesnt match accept reply format {accept}"
                )
    else:
        # Build reply file path
        output_path2file = '_'.join([
            "edw",
            self._gridpool_name,
            "_".join( epochs_found[ 0 ] ),
            "_".join( readers ),
        ]) + '.' + format

    skippable = output_path2file
    if compress :
        skippable += '.gz'

    # If this file already exists we are done
    if _request._is_skippable(
        output_path2file = skippable,
        override_file = override_file,
        print_log = self._print_log ) :
        return output_path2file

    if not ip :
        self._log("No ip provided\
            \n Fetching external ip from https://api.ipify.org\
            \nif using a vpn, please provide the vpn ip\
        ")
        ip = requests.get( url = "https://api.ipify.org" ).text

    # Get Eulerian session token
    self._log( "Requesting Authority services for a Session token" )
    begin = time.time()
    bearer = session(
        self._api_v2, self._http_headers, ip, self._print_log
        )
    end = time.time()
    self._log(
        "Done requesting authority service : {:.2f} s".format( end - begin )
        )

    # Create a Job
    self._log( "Submitting JOB" )
    begin = time.time()
    headers = {
        "Authorization": "Bearer " + bearer,
        "Content-Type": "application/json",
        "Accept-Encoding" : encoding,
        "Accept" : accept
    }
    reply = job_create( self._edw_jobs, headers, query, self._print_log )
    end = time.time()
    if reply is None :
        self._log( "Failed to Submit JOB" )
        sys.exit( 2 )
    status = reply[ 'status' ]
    if status != 'Running' :
        self._log( "Failed to submit JOB." )
        sys.exit( 2 )
    uuid, url = reply[ 'data' ]
    self._log(
        "Done submitting JOB. {:.2f} s".format( end - begin )
        )

    # Wait end of Job
    self._log( "Waiting end of JOB : " + str( uuid ) + "." )
    begin = time.time()
    reply = job_wait( reply, headers, self._print_log )
    if reply[ 'status' ] != 'Done' :
        self._log( "JOB failed." + str( reply ) )
        sys.exit( 2 )
    end = time.time()
    self._log( "JOB done. {:.2f} s".format( end - begin ) )

    # Download Job reply
    self._log( "Downloading JOB reply from the server" )
    outdir = os.path.split( output_path2file )[ 0 ]
    begin = time.time()
    path, prefix = job_download( self, reply, headers, outdir )
    if path is None :
        self._log( "Failed to download JOB reply" )
        sys.exit( 2 )
    end = time.time()
    self._log( "JOB reply downloaded. {:.2f} s".format( end - begin ) )

    # If gateway doesn't know the request reply format, rename output file
    # to reflect really downloaded format
    if format != prefix :
        self._log( "Requested reply format can't be provided." )
        output_path2file = output_path2file[ : output_path2file.rfind( format ) ] + prefix
        self._log( "JSON reply format is returned. " + output_path2file )

    # Compress reply if requested
    if compress :
        output_path2file += '.gz'
        gzip_compress( path, output_path2file )
        os.remove( path )
    else :
        # Rename file
        os.rename( path, output_path2file )

    # Kill the request on the server
    kill( url, headers )

    return output_path2file
#
# @brief Kill Eulerian Data Warehouse JOB.
#
# @param url - URL to Eulerian Data Warehouse JOB.
# @param headers - HTTP headers.
#
def kill( url, headers ) :
    url = f"{url}/cancel"
    requests.get(
        url, headers = headers
        )
