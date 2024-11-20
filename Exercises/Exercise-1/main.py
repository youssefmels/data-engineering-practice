import requests
import os
from zipfile import ZipFile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    #Create download directory
    print("Current:",os.getcwd())
    os.makedirs("downloads", exist_ok=True)
    print("Successful directory creation")

    #download files from uris
    for file in download_uris:
        try:
            response = requests.get(file)
            print(f"Response status code: {response.status_code}")
            if response.status_code == 200:
                print("Status code 200")
                #split the filename from the uri
                filename = file.split("/")
                filename = filename[-1]
                #add the filename to the downloads directory
                filepath = os.path.join(os.getcwd(),'downloads', filename)
                #save content to file
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                    print("File downloaded")
                #unzip
                if filename.endswith('.zip'):
                    with ZipFile(filepath, 'r') as zipObject:
                        zipObject.extractall('downloads')
                        print("File extracted!")
                    os.remove(filepath)
                    print("Zipped files deleted")
            else:
                print(f"Failed to download {file}: Response status code: {response.status_code}\n")

        except Exception as e:
            print(f"Error occured during processing {file}: {e}")

        pass


if __name__ == "__main__":
    main()
