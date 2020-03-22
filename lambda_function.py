import time
import numpy
import pandas
import awswrangler as wr 

def lambda_handler(event, context):
    #download from s3 as csv
    data = wr.pandas.read_csv(event["input"], names=["RawNum1", "RawNum2"])
    print(data.head())
    #take difference and add new column
    data["Diff"] = data["RawNum2"] - data["RawNum1"]
    #add resutls column
    data["Result"] = '0'
    #add timestamp column
    data["TimeStamp"] = time.time()
    #if diff is negative then flag else clear
    data["Result"] = numpy.where(data["Diff"]<0, 'flag', 'clear')
    print(data.head())

    #if numpy is not an option
    #data.loc[data["Diff"] < 0, "Result"] = 'flag' 
    #data.loc[data["Diff"] > 0, "Result"] = 'clear'
    
    #wirte to s3 in parquet
    wr.pandas.to_parquet(
        dataframe=data,
        path=event["output"],
        mode="overwrite",
        preserve_index=False
    )
    return f"File saved to : {event['output']}"