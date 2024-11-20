import boto3
import gzip
import os

def main():
    #boto3 downlaod
    s3 = boto3.client('s3')
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    bucketname = "commoncrawl"
    localpath = "/app/wet.paths.gz"
    s3.download_file(bucketname, key, localpath)
    print("Download successful")
    #unzip
    with gzip.open(localpath, 'rt') as f:
        first_line = f.readline().strip()

    next_file = '/app/downloaded_file.gz'
    s3.download_file(bucketname,first_line,next_file)
    #print each line
    with gzip.open(localpath, 'rb') as f:
        for line in f:
            print(line.decode().strip())
    pass


if __name__ == "__main__":
    main()
