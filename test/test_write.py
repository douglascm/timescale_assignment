import os
from main import write,query

def test_write(table='test',filename='test',write_method='psycopg2'):
    query("truncate table test;")
    write(table,filename,write_method)
    assert os.path.isfile(filename), "Error saving file"
    
if __name__ == "__main__":
    test_write()
    print("Everything passed")