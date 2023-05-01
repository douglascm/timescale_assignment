import pandas as pd
from main import query

def test_query(sql="select count(*) from test",mode='execute'):
    df = query(sql,mode)
    assert isinstance(df,pd.DataFrame) or isinstance(df,str), "Error saving file"
    
if __name__ == "__main__":
    test_query()
    print("Everything passed")