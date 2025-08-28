import requests
import zipfile
import os

datasets = [
    ("Restaurant", "http://dbs.uni-leipzig.de/file/Restaurants.zip"),
    ("Cora", "http://dbs.uni-leipzig.de/file/Cora.zip"),
    ("MusicBrainz", "https://raw.githubusercontent.com/tomonori-masui/entity-resolution/main/data/musicbrainz_200k.csv")
]

for name, url in datasets:
    print(f"Downloading {name}...")
    try:
        response = requests.get(url)
        if url.endswith('.zip'):
            with open(f"data/benchmark_datasets/{name}.zip", 'wb') as f:
                f.write(response.content)
        else:
            with open(f"data/benchmark_datasets/{name}.csv", 'wb') as f:
                f.write(response.content)
        print(f"✅ {name} downloaded")
    except:
        print(f"❌ Failed to download {name}")
