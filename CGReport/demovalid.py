import pandas as pd
import pandera as pa
myExpMsg = ""
try:
    df = pd.DataFrame({
    "column1": [1, 4, 0, 10, 9],
    "column2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    "column3": ["value_1", "value_2", "value_3", "value_2", "akshad"],
    })
    print(df)
    
    schema = pa.DataFrameSchema({
    "column1": pa.Column(int, checks=pa.Check.le(10)),
    "column2": pa.Column(float, checks=pa.Check.lt(-1.2)),
    "column3": pa.Column(str, checks=[
        pa.Check.str_startswith("value_"),
        pa.Check(lambda s: s.str.split("_", expand=True).shape[1] == 2)
    ]),
})
    validated_df = schema(df)
except Exception as e:
  myExpMsg = "the error is " + str(e)
finally:
  print("Final msg")
  
print(myExpMsg)