import requests

proxies = {
    'http': 'http://172.28.5.9:8080',
    'https': 'http://172.28.5.9:8080',
}
file = "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014 Q2 spring (Apr-Jun)-Central.csv"
file_name = file.split('/')[-1]
print(file_name)
r = requests.get(file)
open(file_name, 'wb').write(r.content)

def download_file(url, save_path):
    try:
        response = requests.get(url, stream=True, timeout=30, proxies=proxies)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Successfully downloaded {url} to {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")

download_file(file, './2014 Q2 spring (Apr-Jun)-Central.csv')