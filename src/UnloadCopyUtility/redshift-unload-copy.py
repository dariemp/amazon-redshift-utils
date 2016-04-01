#!/usr/bin/env python
"""
Usage:

python redshift-unload-coyp.py <config file> <region>


* Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
"""

import sys
import json
import base64
import boto
import datetime
from pg import DB
from boto import kms, s3

kmsClient = None
s3Client = None
nowString = None
config = None
region = None
bucket = None
key = None

encryptionKeyID = 'alias/RedshiftUnloadCopyUtility'

options = """keepalives=1 keepalives_idle=200 keepalives_interval=200
             keepalives_count=6"""

set_timeout_stmt = "set statement_timeout = 1200000"

unload_stmt = """unload ('SELECT * FROM %s.%s')
                 to '%s' credentials
                 'aws_access_key_id=%s;aws_secret_access_key=%s;master_symmetric_key=%s'
                 manifest
                 encrypted
                 gzip
                 delimiter '^'
                 addquotes
                 null as 'NULL'
                 escape
                 allowoverwrite;"""

copy_stmt = """copy %s.%s
               from '%smanifest' credentials
               'aws_access_key_id=%s;aws_secret_access_key=%s;master_symmetric_key=%s'
               manifest
               encrypted
               gzip
               delimiter '^'
               removequotes
               explicit_ids
               null as 'NULL'
               escape;"""

generate_tbl_ddl_stmt = """select ddl from admin.v_generate_tbl_ddl
                           where schemaname='%s' and tablename='%s';"""


def conn_to_rs(host, port, db, usr, pwd, opt=options, timeout=set_timeout_stmt):
    rs_conn_string = """host=%s port=%s dbname=%s user=%s password=%s
                         %s""" % (host, port, db, usr, pwd, opt)
    print "Connecting to %s:%s:%s as %s" % (host, port, db, usr)
    rs_conn = DB(dbname=rs_conn_string)
    rs_conn.query(timeout)
    return rs_conn


def unload_data(conn, aws_access_key_id, aws_secret_key, master_symmetric_key, dataStagingPath, schema_name, table_name):
    print "Exporting %s.%s to %s" % (schema_name, table_name, dataStagingPath)
    conn.query(unload_stmt % (schema_name, table_name, dataStagingPath, aws_access_key_id,
                              aws_secret_key, master_symmetric_key))


def create_view_table_ddl_generator(conn):
    print "Creating admin schema if not exists..."
    conn.query("create schema if not exists admin;")
    with open("../AdminViews/v_generate_tbl_ddl.sql") as f:
        create_view_stmt = f.read()
        print "Creating view to export table DDL..."
        conn.query(create_view_stmt)


def copy_table_ddl(src_conn, dest_conn, schema_name, table_name):
    result = src_conn.query(generate_tbl_ddl_stmt % (schema_name, table_name)).getresult()
    result.pop(0)  # Ignore DROP TABLE comment
    create_table_ddl_stmt = ""
    for stmt_chunk in result:
        create_table_ddl_stmt += "\n" + stmt_chunk[0]
    print "Creating destination table: %s" % table_name
    dest_conn.query(create_table_ddl_stmt)


def copy_data(conn, aws_access_key_id, aws_secret_key, master_symmetric_key, dataStagingPath, schema_name, table_name):
    print "Importing %s.%s from %s" % (schema_name, table_name, dataStagingPath)
    conn.query(copy_stmt % (schema_name, table_name, dataStagingPath, aws_access_key_id, aws_secret_key, master_symmetric_key))


def decrypt(b64EncodedValue):
    return kmsClient.decrypt(base64.b64decode(b64EncodedValue))['Plaintext']


def tokeniseS3Path(path):
    pathElements = path.split('/')
    bucketName = pathElements[2]
    prefix = "/".join(pathElements[3:])

    return (bucketName, prefix)


def s3Delete(stagingPath):
    print "Cleaning up S3 Data Staging Location %s" % (stagingPath)
    s3Info = tokeniseS3Path(stagingPath)

    stagingBucket = s3Client.get_bucket(s3Info[0])

    for key in stagingBucket.list(s3Info[1]):
        stagingBucket.delete_key(key)


def getConfig(path):
    # datetime alias for operations
    global nowString
    nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())

    global config

    if path.startswith("s3://"):
        # download the configuration from s3
        s3Info = tokeniseS3Path(path)

        bucket = s3Client.get_bucket(s3Info[0])
        key = bucket.get_key(s3Info[1])

        configContents = key.get_contents_as_string()
        config = json.loads(configContents)
    else:
        with open(path) as f:
            config = json.load(f)


def usage():
    print "Redshift Unload/Copy Utility"
    print "Exports data from a source Redshift database to S3 as an encrypted dataset, and then imports into another Redshift Database"
    print ""
    print "Usage:"
    print "python redshift-unload-copy.py <configuration> <region>"
    print "    <configuration> Local Path or S3 Path to Configuration File on S3"
    print "    <region> Region where Configuration File is stored (S3) and where Master Keys and Data Exports are stored"
    sys.exit(-1)


def main(args):
    if len(args) != 3:
        usage()

    global region
    region = args[2]

    global s3Client
    s3Client = boto.s3.connect_to_region(region)

    # load the configuration
    getConfig(args[1])

    # parse options
    dataStagingPath = "%s/%s/" % (config['s3Staging']['path'].rstrip("/") , nowString)
    if not dataStagingPath.startswith("s3://"):
        print "s3Staging.path must be a path to S3"
        sys.exit(-1)

    accessKey = config['s3Staging']['aws_access_key_id']
    secretKey = config['s3Staging']['aws_secret_access_key']
    deleteOnSuccess = config['s3Staging']['deleteOnSuccess']

    # source from which to export data
    srcConfig = config['unloadSource']

    src_host = srcConfig['clusterEndpoint']
    src_port = srcConfig['clusterPort']
    src_db = srcConfig['db']
    src_schema = srcConfig['schemaName']
    src_user = srcConfig['connectUser']
    src_table = 'tableName' in srcConfig and srcConfig['tableName'] or None

    # target to which we'll import data
    destConfig = config['copyTarget']

    dest_host = destConfig['clusterEndpoint']
    dest_port = destConfig['clusterPort']
    dest_db = destConfig['db']
    dest_user = destConfig['connectUser']
    dest_schema = 'schemaName' in destConfig and destConfig['schemaName'] or src_schema
    dest_table = 'tableName' in srcConfig and 'tableName' in destConfig and destConfig['tableName'] or None

    global kmsClient
    kmsClient = boto.kms.connect_to_region(region)

    # create a new data key for the unload operation
    dataKey = kmsClient.generate_data_key(encryptionKeyID, key_spec="AES_256")

    master_symmetric_key = base64.b64encode(dataKey['Plaintext'])

    # decrypt the source and destination passwords
    src_pwd = decrypt(srcConfig["connectPwd"])
    dest_pwd = decrypt(destConfig["connectPwd"])

    # decrypt aws access keys
    s3_access_key = decrypt(accessKey)
    s3_secret_key = decrypt(secretKey)

    src_conn = conn_to_rs(src_host, src_port, src_db, src_user,
                          src_pwd)

    dest_conn = conn_to_rs(dest_host, dest_port, dest_db, dest_user,
                           dest_pwd)

    if src_table:
        src_tables = [src_table]
    else:
        src_tables = src_conn.get_tables(False)

    if len(dest_conn.get_tables(False)) < len(src_tables):
        create_view_table_ddl_generator(src_conn)
        create_dest_tables = True
    else:
        create_dest_tables = False

    table_prefix = src_schema + "."
    for src_table in src_tables:
        if not src_table.startswith(table_prefix):
            continue
        src_table = src_table.replace(table_prefix, "")
        if len(src_tables) > 1:
            dest_table = src_table
            if create_dest_tables:
                copy_table_ddl(src_conn, dest_conn, dest_schema, dest_table)
        unload_data(src_conn, s3_access_key, s3_secret_key,
                    master_symmetric_key, dataStagingPath,
                    src_schema, src_table)

        copy_data(dest_conn, s3_access_key, s3_secret_key,
                  master_symmetric_key, dataStagingPath,
                  dest_schema, dest_table)

    src_conn.close()
    dest_conn.close()

    if deleteOnSuccess:
        s3Delete(dataStagingPath)


if __name__ == "__main__":
    main(sys.argv)
