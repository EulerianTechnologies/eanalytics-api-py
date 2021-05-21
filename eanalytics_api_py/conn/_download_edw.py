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
        f = open( path_in, "r" )
        data = f.read()
        f.close()
    except IOError as e :
        print( str( e ) )
        raise e
    with gzip.open( filename = path_out, mode = "wt" ) as gzipfile :
        gzipfile.write( data )
        gzipfile.close()
#
# @brief Convert JSON reply file to CSV file.
#
# @param path_json - JSON file path.
#
# @return CSV file path
#
def json_to_csv( path_json ) :
    pattern = re.compile( "^/.+/\d+" )
    match = pattern.search( path_json )
    path_csv = match.group() + ".csv"
    stream_in = open( path_json, "r" )
    stream_out = open( path_csv, "w" )
    writer = csv.writer( stream_out, delimiter = ";" )
    objects = ijson.items( stream_in, "headers.schema.item" )
    try :
        headers = ( header for header in objects )
    except ijson.common.IncompleteJSONError as e :
        raise e
    columns = []
    for header in headers :
        columns.append( header[ 1 ] )
    writer.writerow( columns )
    stream_in.seek( 0 )
    objects = ijson.items( stream_in, "rows.item" )
    try :
        rows = ( row for row in objects )
    except ijson.common.IncompleteJSONError as e :
        raise e
    for row in rows :
        writer.writerow( row )
    stream_out.close()
    stream_in.close()
    return path_csv
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
        return None
    length = reply.headers[ 'Content-Length' ]
    path = directory + '/' + str( uuid ) + '.json'
    stream = open( path, 'wb' )
    if stream is None :
        return None
    total = 0
    for line in reply.iter_content( 8192 ) :
        total += len( line )
        conn._logrewind(
            "Write : " + str( len( line ) ) + "/" + unit( total ) + 
            "/" + unit( int( length ) )
            )
        stream.write( line )
    conn._log( "" )
    stream.close()
    return path
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
        If set to True, CSV reply file is compressed

    uuid : str, optional
        The job id to download directly from a previously requested jobrun

    Returns
    -------
    str
        The output_path2file containing the downloaded datamining data
    """

    request_begin = time.time()
    if not isinstance(query, str):
        raise TypeError("query should be a string")

    epoch_from_to_findall = re.findall(
        r'{\W+?(\d+)\W+?(\d+)\W+?}',  # { 1602958590 1602951590 }
        query
    )
    if not epoch_from_to_findall:
        raise ValueError(f"Could not read epoch_from and epoch_to from query=\n{query}")

    readers_findall = re.findall(r'(\w+):(\w+)@([\w_-]+)', query)  # ea:pageview@demo-fr
    if not readers_findall:
        raise ValueError(f"Could not read READER from query=\n{query}")

    readers = []
    for reader in readers_findall:
        storage_name, table_name, website_name = reader
        self._is_allowed_website_name(website_name)
        readers += [storage_name, table_name, website_name]

    if output_path2file:
        if not isinstance(output_path2file, str):
            raise TypeError("output_path2file should be a str type")
        if compress :
            output_path2file += ".gz"

    else:
        output_path2file = '_'.join([
            "dw",
            self._gridpool_name,
            "_".join(epoch_from_to_findall[0]),
            "_".join(readers),
        ]) + ".csv"

        if compress :
            output_path2file += ".gz"

        if _request._is_skippable(
                output_path2file=output_path2file,
                override_file=override_file,
                print_log=self._print_log
        ):
            return output_path2file

    if ip:
        if not isinstance(ip, str):
            raise ValueError("ip should be a str type")
    else:
        self._log("No ip provided\
            \n Fetching external ip from https://api.ipify.org\
            \nif using a vpn, please provide the vpn ip\
        ")
        ip = requests.get(url="https://api.ipify.org").text

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
        self._log( "Failed to submit JOB" )
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
    begin = time.time()
    outdir = os.path.split( output_path2file )[ 0 ]
    path_json = job_download( self, reply, headers, outdir )
    if path_json is None :
        self._log( "Failed to download JOB reply" )
        sys.exit( 2 )
    end = time.time()
    self._log( "JOB reply downloaded. {:.2f} s".format( end - begin ) )

    # JSON to CSV
    self._log( "Converting JSON to CSV" )
    begin = time.time()
    path_csv = json_to_csv( path_json )
    end = time.time()
    self._log( "Done converting JSON to CSV. {:.2f} s".format( end - begin ) )
 
    if compress :
        # Compress CSV to GZ
        self._log( "Compressing CSV file" )
        begin = time.time()
        gzip_compress( path_csv, output_path2file )
        end = time.time()
        self._log( "Done compressing CSV file. {:.2f} s".format( end - begin ) )
        self._log(
            "Reply file path=" + output_path2file +
            ". {:.2f} s".format( end - request_begin )
            )
    else :
        # Rename CSV file
        os.rename( path_csv, output_path2file )

    # Kill the request on the server
    kill( url, headers )

    # Remove json reply 
    os.remove( path_json )

    # Remove CSV reply 
    if compress :
        os.remove( path_csv )

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
