#!/bin/bash
python3 s3booster-snowball-v2.py --bucket_name your-own-bucket --src_dir /data/fs1/ --endpoint https://s3.ap-northeast-2.amazonaws.com --profile_name sbe1 --prefix_root fs1/ --max_process 5 --max_tarfile_size $((2*(1024**3)))  --compression '' --no_extract 'no'
