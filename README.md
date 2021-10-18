# s3booster snowball
s3booster-snowball.py, this script implemented batch feature in parallel so it is fast and simple to use, especially when dealing with small files. If you have headache with low performance when uploading small files, it may give you StimPack!

## v1 or v2?
s3booster-snowball has two types of version 
  - version 1: suitable for low memory workstation, but little slower comparing to version 2
  - version 2: using more memory, it shows faster upload performance.
    - workstation memory requirement: 32GB preferred 
        - free memory > (max-tarfile-size * max-process) + 0.2 * (max-tarfile-size * max-process)
## How to Use
Here is example to execute s3booster-snowball.py
```sh
python3 s3booster-snowball.py --bucket_name your-own-bucket --src_dir /data/fs1/ --endpoint https://s3.ap-northeast-2.amazonaws.com --profile_name sbe1 --prefix_root fs1/ --max_process 5 --max_tarfile_size $((1*(1024**3))) --max_part_size $((100*(1024**2)))
```

Here is help 
```sh
s3booster]$ python3 s3booster-snowball.py -h
usage: s3booster-snowball.py [-h] --bucket_name BUCKET_NAME 
                                         --src_dir SRC_DIR 
                                         --endpoint ENDPOINT
                                         [--profile_name PROFILE_NAME]
                                         [--prefix_root PREFIX_ROOT]
                                         [--max_process MAX_PROCESS]
                                         [--max_tarfile_size MAX_TARFILE_SIZE]
                                         [--max_part_size MAX_PART_SIZE]
                                         [--compression COMPRESSION]

optional arguments:
  -h, --help            show this help message and exit
  --bucket_name BUCKET_NAME
                        your bucket name e) your-bucket
  --src_dir SRC_DIR     source directory e) /data/dir1/
  --endpoint ENDPOINT   snowball endpoint e) http://10.10.10.10:8080 or
                        https://s3.ap-northeast-2.amazonaws.com
  --profile_name PROFILE_NAME
                        aws_profile_name e) sbe1
  --prefix_root PREFIX_ROOT
                        prefix root e) dir1/
  --max_process MAX_PROCESS
                        NUM e) 5
  --max_tarfile_size MAX_TARFILE_SIZE
                        NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB,
                        10GB for >total 50GB
  --max_part_size MAX_PART_SIZE
                        NUM bytes e) $((100*(1024**2))) #100MB
  --compression COMPRESSION
                        specify gz to enable
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
