import boto3
import json
import time
import os, sys
import uuid
from factory import Campaign

from main import Main
main = Main()
s3 = boto3.client('s3')

#sony_televisor = main en external
class QueryData(Campaign):   #productA1
    @staticmethod
    def dataquery(self):
        try:
            company_data =[]
            student_data =[]
            item = []
            s3 = boto3.client('s3')
            #data is queried from the bucket
            resp = s3.select_object_content(
                Bucket='external-analytics-data',
                Key=self,
                ExpressionType='SQL',
                Expression="SELECT * FROM s3object/*[*].estudiantes[*]*/",
                InputSerialization = {"JSON": {"Type": "Document"},'CompressionType': 'GZIP'},
                OutputSerialization = {'JSON': {}},
            )
            finaldata={}
            cunsumidor= {}
            cunsumidor["cunsumidor"]="activeCampaign"

            for event in resp['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
                    response_json =json.loads(records)
                    cunsumidor.update(response_json)
                    
                    #data for accounts
                    res_company = response_json.get('empresas',[])

                    for factorydata in res_company:
                        for n_data in factorydata:#separate the key from each field
                            item.append(n_data)
                            if  (factorydata[n_data] == True or factorydata[n_data] == False):#changes value of boolean fields
                                factorydata[n_data]=QueryData.change_value(factorydata[n_data],n_data)
                            
                        company_data.append(QueryData.data_to_company(factorydata,item))
                        finaldata['empresas']=company_data
                        item = []#set the array value to avoid duplication of values

                    #data for students
                    res_student = response_json.get('estudiantes',[])

                    for studentdata in res_student:
                        for n_student in studentdata:
                            item.append(n_student)
                            if  studentdata[n_student] == True or studentdata[n_student] == False:
                                studentdata[n_student]=QueryData.change_value(studentdata[n_student],n_student)
                        student_data.append(QueryData.data_to_company(studentdata,item))
                        finaldata['estudiantes']=student_data
                        item = []

            return finaldata
        except Exception as e:
            print("Error: \nThe query file does not exist",e)
            return None
    
    @staticmethod
    def data_to_company(data,item):
        reviewdata={}
        #eif 'isnuevaempresa' in data:
        reviewdata=QueryData.datareview(data,item)
        return reviewdata
    
    @staticmethod
    def datareview(data,item): 
        datacompany={}
        for n in item:
            datacompany[n]=data[n]
        return datacompany 
    
    @staticmethod
    def change_value(data,n):
        newvalue =data
        if (n!= "idempresa" 
            and n!="idestudiante" 
            and n!="cursosobligatoriosasignados" 
            and n!="cursosobligatoriosterminados" 
            and n!="cursoslibresterminados"):
            if data == True:
                newvalue="Si"
            else:
                newvalue="No"
        return newvalue
    
    @staticmethod
    def prepare_data_to_send(data):
            
            datafactory={}
            datastudent ={}
            #QueryData.send_data_to_sns(data)
            
            for factory_data in data['empresas']:
                time.sleep(2)
                datafactory["empresa"]=factory_data
                QueryData.see_data(datafactory)
                #QueryData.send_data_to_sns(datafactory)
                
            for student_data in data['estudiantes']:#####
                time.sleep(2)#######
                datastudent["estudiantes"]=student_data
                QueryData.see_data(datastudent)
                #QueryData.send_data_to_sns(datastudent)


    @staticmethod
    def send_data_to_sns(data_frame):
        message = data_frame
        client = boto3.client('sns')
        topic_arn = os.environ['sns_arn']

        print("Data to SNS ", json.dumps(message))

        response = client.publish(
            TargetArn = topic_arn,
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure= 'json',
            MessageAttributes={
                "consumer":{
                    'DataType':'String',
                    'StringValue':'activeCampaign'
                }
            }
        )

        print(response)

    @staticmethod
    def see_data(data):
        print("data arrive to jane= ",json.dumps(data))
#        for active in data['estudiantes']:#####
#            time.sleep(2)#######
#            print('\n', active,'\n')