import os
from src.main import save

def test_query(url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet',filename='/test/files/test_yellow_tripdata_2023-01.parquet'):
    save(url,filename)
    assert os.path.isfile(filename), "Error saving file"
    
if __name__ == "__main__":
    save()
    print("Everything passed")