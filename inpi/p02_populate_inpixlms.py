import boto3
import os

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def upload_files():
    """
    Upload XML files inside ObjectStorage (bucket inpi_xmls) organised by year and week (or name of the folder inside
    the year folder if not by week)
    :return:
    """
    os.chdir(DATA_PATH)
    list_dir = os.listdir(DATA_PATH)
    # list all the directories available
    list_dir.sort()
    for dir in list_dir:
        # for each yearly directory, we list all the folders inside
        list_dir_year = os.listdir(f"{DATA_PATH}{dir}/")
        for year in list_dir_year:
            # for each weekly folder inside a yearly folder, we list all the files
            list_files = os.listdir(f"{DATA_PATH}{dir}/{year}")
            for files in list_files:
                # get the full path of the file to upload it into inpi-xmls
                dirpath = os.path.join(DATA_PATH, dir, year, files)
                # get the prefix to order files inside the bucket
                prefix = f"{dir}/{year}"
                # connect to ObjectStorage
                conn = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                    endpoint_url=os.getenv("ENDPOINT_URL"))
                # check if the file is not already in the bucket
                result = conn.list_objects_v2(Bucket="inpi-xmls", Prefix=f"{prefix}/{files}")
                # upload onmy the files which are not already in the bucket
                if "Contents" in result:
                    pass
                else:
                    response = conn.upload_file(dirpath, "inpi-xmls", f"{prefix}/{files}")
                    print(f"{prefix}/{files} added in inpi-xmls", flush=True)
                conn.close()
