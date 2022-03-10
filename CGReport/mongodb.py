# importing module
from pymongo import MongoClient
from bson.json_util import dumps

def get_batch(Batch_Name):
    # Connect with the portnumber and host
    client = MongoClient("mongodb://aditya:aditya@44.199.59.134:27017/admin")

    mydb = client["NVTraining"]
    mycol = mydb["TrainingBatch"]

    result = mycol.find({'cr': Batch_Name})
    list_cur = list(result)
    print(dumps(list_cur))



    #write update query
    #criteria : condition, data {trainingExam : exam details array}

    myquery = { "cr": Batch_Name }

    df = [ 
                {
                    "examName" : "MCQ1 & Codign-1 HTML",
                    "examType" : "Internal",
                    "examDate" : "20/11/2021"
                }, 
                {
                    "examName" : "M1",
                    "examType" : "External",
                    "examDate" : "24/11/2021"
                }, 
                {
                    "examName" : "Novelvista Assessment ",
                    "examType" : "Internal",
                    "examDate" : "10/12/2021"
                }, 
                {
                    "examName" : "Novelvista Assessment",
                    "examType" : "External",
                    "examDate" : "25/12/2021"
                }, 
                {
                    "examName" : "L1 Test",
                    "examType" : "External",
                    "examDate" : "19/01/2022"
                }, 
                {
                    "examName" : "Novelvista Assessment ",
                    "examType" : "Internal",
                    "examDate" : "10/12/2021"
                }, 
                {
                    "examName" : "Novelvista Assessment ",
                    "examType" : "Internal",
                    "examDate" : "25/12/2021"
                }
            ]
                    
           

    newvalues = { "$set":   { "trainingExam": df }}     
    
    x = mycol.update_many(myquery, newvalues)

    print(x.modified_count, "documents updated.")

get_batch('test')



