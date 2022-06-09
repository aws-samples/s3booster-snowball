# s3booster snowball
[s3booster-snowball-v2.py](s3booster-snowball-v2.py), this script implemented batch feature in parallel so it is fast and simple to use, especially when dealing with small files. If you have headache with low performance when uploading small files, it may give you StimPack!
s3booster provides two features 1)first one is to accellerate performance when ingesting small files on Snowball, 2)second is to archive files and generate big tar file on Amazon S3 in order to improve uploading performance and save management cost.

## How to Use
Here is example to execute [s3booster-snowball-v2.py](s3booster-snowball-v2.py)
or you can refer [*run-s3booster-sbe.sh*](run-s3booster-sbe.sh) and [*run-s3booster-archive.sh*](run-s3booster-archive.sh) shell scripts.

For Snowball Usage,
```sh
python3 s3booster-snowball-v2.py --bucket_name your-own-bucket --src_dir /data/fs1/ --endpoint https://s3.ap-northeast-2.amazonaws.com --profile_name sbe1 --prefix_root fs3/ --max_process 5 --max_tarfile_size $((1*(1024**3))) --symlink no
```

For Archiving Usage,
```sh
python3 s3booster-snowball-v2.py --bucket_name your-own-bucket --src_dir /data/fs1/ --endpoint https://s3.ap-northeast-2.amazonaws.com --profile_name sbe1 --max_process 5 --max_tarfile_size $((1*(1024**3))) --no_extract 'yes' --target_file_prefix 'new_s3_path/' --storage_class 'GLACIER_IR'
```

Here is help 
```sh
ec2-user$ python3 s3booster-snowball-v2.py --help
usage: s3booster-snowball-v2.py [-h] --bucket_name BUCKET_NAME --src_dir SRC_DIR --endpoint ENDPOINT [--profile_name PROFILE_NAME]
                                [--prefix_root PREFIX_ROOT] [--max_process MAX_PROCESS] [--max_tarfile_size MAX_TARFILE_SIZE]
                                [--compression COMPRESSION] [--no_extract NO_EXTRACT] [--target_file_prefix TARGET_FILE_PREFIX]
                                [--storage_class STORAGE_CLASS]

optional arguments:
  -h, --help            show this help message and exit
  --bucket_name BUCKET_NAME
                        your bucket name e) your-bucket
  --src_dir SRC_DIR     source directory e) /data/dir1/
  --endpoint ENDPOINT   snowball endpoint e) http://10.10.10.10:8080 or https://s3.ap-northeast-2.amazonaws.com
  --profile_name PROFILE_NAME
                        aws_profile_name e) sbe1
  --prefix_root PREFIX_ROOT
                        prefix root e) dir1/
  --max_process MAX_PROCESS
                        NUM e) 5
  --max_tarfile_size MAX_TARFILE_SIZE
                        NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB, 10GB for >total 50GB
  --compression COMPRESSION
                        specify gz to enable
  --no_extract NO_EXTRACT
                        yes|no; Do not set the autoextract flag
  --target_file_prefix TARGET_FILE_PREFIX
                        prefix of the target file we are creating into the snowball
  --storage_class STORAGE_CLASS
                        specify S3 classes, be cautious Snowball support only STANDARD class; StorageClass=STANDARD|REDUCED_REDUNDANCY|STANDARD_I
                        A|ONEZONE_IA|INTELLIGENT_TIERING|GLACIER|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR
```                        

## Executing Script
Here is output of execution
```sh
sh run-s3booster-sbe.sh
multi part uploading:  1 / 11 , size: 104884733 bytes
multi part uploading:  1 / 11 , size: 104884714 bytes
multi part uploading:  1 / 11 , size: 104869657 bytes
multi part uploading:  1 / 11 , size: 104884786 bytes
multi part uploading:  1 / 11 , size: 104883288 bytes
multi part uploading:  1 / 11 , size: 104868660 bytes
multi part uploading:  1 / 11 , size: 104867541 bytes
... omitted
... omitted
snowball-20210810_152400-7Y5EPP.tgz is uploaded successfully

multi part uploading:  7 / 11 , size: 104866395 bytes
multi part uploading:  8 / 11 , size: 104862516 bytes
multi part uploading:  9 / 11 , size: 104890119 bytes
^[[O^[[Imulti part uploading:  10 / 11 , size: 104866477 bytes
metadata info: {'ResponseMetadata': {'RequestId': '3X9ZKZA90YRQ98SC', 'HostId': 'YcmBg0Syf9pEbRjMPdorhyIZgckXsz8xliXagtZxDp8gasK4TDwgG98g6rrHxTy8F6fKEOQ3/+4=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'YcmBg0Syf9pEbRjMPdorhyIZgckXsz8xliXagtZxDp8gasK4TDwgG98g6rrHxTy8F6fKEOQ3/+4=', 'x-amz-request-id': '3X9ZKZA90YRQ98SC', 'date': 'Tue, 10 Aug 2021 15:26:28 GMT', 'last-modified': 'Tue, 10 Aug 2021 15:25:24 GMT', 'etag': '"06aa2906ce7dbf864d64ff828d615c65-11"', 'x-amz-meta-snowball-auto-extract': 'true', 'accept-ranges': 'bytes', 'content-type': 'binary/octet-stream', 'server': 'AmazonS3', 'content-length': '1077720331'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2021, 8, 10, 15, 25, 24, tzinfo=tzutc()), 'ContentLength': 1077720331, 'ETag': '"06aa2906ce7dbf864d64ff828d615c65-11"', 'ContentType': 'binary/octet-stream', 'Metadata': {'snowball-auto-extract': 'true'}}

snowball-20210810_152400-MRCMA5.tgz is uploaded successfully

====================================
Duration: 0:02:27.091026
Total File numbers: 503004
S3 Endpoint: https://s3.ap-northeast-2.amazonaws.com
End
```
## Checking the logs
Log Directory: ./log/
- error-{date}.log : each file of failed to tar will be logged here
- success-{date}.log: success message will be logged here
- filelist-{date}.log: all files which are archived will be logged here

## File Path
If you want to change objecs path which are extracted, you can specify *prefix_root*.

If you want to change tarfile's path on S3, you can specify *target_file_prefix*(when you use target_file_prefix, don't forget to add '/' such as 'newpath/'.
## Caveat
### metadata, snowball-auto-extract
--no_extract = 'no': if you are moving data to Snowball Edge, "--no_extract 'yes'" should be used.
Specifying 'snowball-auto-extract=true' automatically extracts the contents of the archived files when the data is imported into Amazon S3. You can confirm this output from 'success-[date].log'
### Don't include './' path in src_dir parameter
Normally in Unix/Linux environment, './' means current directory, so someone tends to use it. However, if you use in '--src_dir' parameter, it will add '.' prefix in S3.

For example, 
when "--src_dir './d001/dir001'" 
it will create following prefix like "s3://[bucket_name]/./d001/dir001/file.1"

## Search files with s3select
When you archived files on S3, you have to know which TARFILE contains the file which you want to get back. 

[s3select.sh](s3select.sh) will search keyword in */log/filelist.log* on S3, and inform you the TARFILE.  
filelist.log is generated by s3booster-snowball.py after uploading tarfiles on S3. you can find it s3://[bucket]/log path.


Here is the result.
```sh
[archiver]$ sh s3select.sh
new_s3_path/snowball-20220329_050947-VKNI2Q.tar ,/data2/fs1/d0011/dir0009/file0441 ,fs3/d0011/dir0009/file0441 ,26726
new_s3_path/snowball-20220329_050947-QV93SP.tar ,/data2/fs1/d0006/dir0009/file0441 ,fs3/d0006/dir0009/file0441 ,33763
new_s3_path/snowball-20220329_050947-YHKQ51.tar ,/data2/fs1/d0001/dir0009/file0441 ,fs3/d0001/dir0009/file0441 ,17378
new_s3_path/snowball-20220329_050947-QV93SP.tar ,/data2/fs1/d0007/dir0009/file0441 ,fs3/d0007/dir0009/file0441 ,17968
new_s3_path/snowball-20220329_050947-R0X1MB.tar ,/data2/fs1/d0022/dir0009/file0441 ,fs3/d0022/dir0009/file0441 ,22852

===== TAR Files containing dir0009/file0441 =====
new_s3_path/snowball-20220329_050947-QV93SP.tar
new_s3_path/snowball-20220329_050947-R0X1MB.tar
new_s3_path/snowball-20220329_050947-VKNI2Q.tar
new_s3_path/snowball-20220329_050947-YHKQ51.tar
```

Here is the s3select.sh script.
```sh
#!/bin/bash
bucket="your-own-bucket"                        # S3 bucket name
key="log/filelist-20220329_050947.log"          # filelist log file which will be generated by s3booster 
keyword="dir0009/file0441"                      # keyword or filename which you want to find
limitNum="100"                                  # filename list which you want to print
tmpfile="/tmp/temp-s3select.log"                # output file

aws s3api select-object-content \
    --bucket $bucket \
    --key $key \
    --expression "SELECT * FROM s3object s where Lower(s._2) like '%${keyword}%' limit $limitNum" \
    --expression-type 'SQL' \
    --input-serialization '{"CSV": {"FieldDelimiter": ","}}' \
    --output-serialization '{"CSV": {"FieldDelimiter": ","}}' /tmp/temp-s3select.log

cat /tmp/temp-s3select.log
echo ""
echo "===== TAR Files containing $keyword ====="
cat /tmp/temp-s3select.log | awk '{print $1}' | sort | uniq
```
