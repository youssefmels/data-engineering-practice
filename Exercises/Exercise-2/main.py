import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'

def main():
    request = requests.get(URL)
    soup = BeautifulSoup(request.text, features = 'html.parser')
    table = soup.find('table')
    if table == None:
        print('no table')
        return
    rows = table.find_all('tr')
    download_url = None

    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            if len(col) > 0:
                if col[1].text.strip() == '2024-01-19 10:28':
                    file_name = col[0].find("a").text.strip()
                    download_url = URL+file_name
                    break
    if download_url:
        response = requests.get(download_url)
        if response.status_code == 200:
            file_path = os.path.join("/app", file_name)
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print(f"Download successful {file_name}")

    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df.head()
        print('Max temperature is: ' + df['HourlyDryBulbTemperature'].max())
    pass


if __name__ == "__main__":
    main()
sarah = 'sarah'
