#!/usr/bin/env python
import sys
import base64
import datetime
import boto
from boto import kms, s3
from subprocess import Popen, PIPE
from redshift_unload_copy import encryptionKeyID, options, set_timeout_stmt, \
                        decrypt, tokeniseS3Path, s3Delete, getConfig

pg_dump_cmd = "/usr/lib/postgresql/9.3/bin/pg_dump -Fc -h %s -p %s -U %s -n %s %s | gzip"

pg_restore_cmd = "gunzip | /usr/lib/postgresql/9.3/bin/pg_restore -Fc -c -C -h %s -p %s -U %s -n %s -d %s"


def unload_data(path, host, port, user, password, db, s3_client,
                schema, s3_access_key, s3_secret_key):
    bucket_name, key_path = tokeniseS3Path(path)
    bucket = s3_client.get_bucket(bucket_name)
    key = bucket.new_key(key_name=key_path)
    key.encrypted = True
    pg_dump = pg_dump_cmd % (host, port, user, schema, db)
    print "Running pg_dump and gzipping output...",
    sys.stdout.flush()
    proc = Popen(pg_dump, stdout=PIPE, shell=True, env={"PGPASSWORD": password})
    stdout = proc.communicate()[0]
    print "done."
    print "Sending pg_dump output to S3....",
    sys.stdout.flush()
    key.set_contents_from_string(stdout, encrypt_key=True)
    print "done."

def copy_data(path, host, port, user, password, db, s3_client,
              schema, s3_access_key, s3_secret_key):
    bucket_name, key_path = tokeniseS3Path(path)
    bucket = s3_client.get_bucket(bucket_name)
    key = bucket.get_key(key_path)
    pg_restore = pg_restore_cmd % (host, port, user, schema, db)
    proc = Popen(pg_restore, stdin=PIPE, shell=True, env={"PGPASSWORD": password})
    print "Running gunzip and pg_restore on S3 data...",
    sys.stdout.flush()
    proc.communicate(key.get_contents_as_string())
    print "done."


def usage():
    print "RDS Unload/Copy Utility"
    print "Exports data from a source RDS database to S3 as an encrypted dataset, and then imports into another RDS Database"
    print ""
    print "Usage:"
    print "python rds-unload-copy.py <configuration> <region>"
    print "    <configuration> Local Path or S3 Path to Configuration File on S3"
    print "    <region> Region where Configuration File is stored (S3) and where Master Keys and Data Exports are stored"
    sys.exit(-1)

def main(args):
    if len(args) != 3:
        usage()

    region = args[2]
    s3_client = boto.s3.connect_to_region(region)

    # load the configuration
    config = getConfig(args[1], s3_client)

    nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())
    filename = "db-%s.sql.gz" % nowString
    # parse options
    dataStagingPath = "%s/%s" % (config['s3Staging']['path'].rstrip("/"), filename)
    if not dataStagingPath.startswith("s3://"):
        print "s3Staging.path must be a path to S3"
        sys.exit(-1)

    accessKey = config['s3Staging']['aws_access_key_id']
    secretKey = config['s3Staging']['aws_secret_access_key']
    deleteOnSuccess = config['s3Staging']['deleteOnSuccess']

    # source from which to export data
    srcConfig = config['unloadSource']

    src_host = srcConfig['endpoint']
    src_port = srcConfig['port']
    src_db = srcConfig['db']
    src_schema = srcConfig['schemaName']
    src_user = srcConfig['connectUser']

    # target to which we'll import data
    destConfig = config['copyTarget']

    dest_host = destConfig['endpoint']
    dest_port = destConfig['port']
    dest_db = destConfig['db']
    dest_user = destConfig['connectUser']
    dest_schema = 'schemaName' in destConfig and destConfig['schemaName'] or src_schema

    kmsClient = boto.kms.connect_to_region(region)

    # decrypt the source and destination passwords
    src_pwd = decrypt(srcConfig["connectPwd"], kmsClient)
    dest_pwd = decrypt(destConfig["connectPwd"], kmsClient)

    # decrypt aws access keys
    s3_access_key = decrypt(accessKey, kmsClient)
    s3_secret_key = decrypt(secretKey, kmsClient)

    unload_data(dataStagingPath, src_host, src_port, src_user, src_pwd, src_db,
                s3_client, src_schema, s3_access_key, s3_secret_key)

    copy_data(dataStagingPath, dest_host, dest_port, dest_user, dest_pwd, dest_db,
              s3_client, dest_schema, s3_access_key, s3_secret_key)

#    if deleteOnSuccess:
#        s3Delete(dataStagingPath, s3_client)


if __name__ == "__main__":
    main(sys.argv)
