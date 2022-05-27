import boto3
import json
import time
import os, sys
import uuid
from datetime import datetime

from factory import Campaign

s3 = boto3.client('s3')
BUCKETNAME = "external-analytics-data"

#samsung_televisor = main en external
class QueryAngel(Campaign):   #productA1
    global therestudent, therecourses
    therestudent =0
    therecourses =0

    @staticmethod #method for making inquiries
    def select_object_bucket(PATHFILENAME,SQL):
        try:
            resp = s3.select_object_content(
                Bucket=BUCKETNAME,
                Key=PATHFILENAME,
                ExpressionType='SQL',
                Expression=SQL,
                InputSerialization = {"JSON": {"Type": "Document"},'CompressionType': 'GZIP'},
                OutputSerialization = {'JSON': {}},
            )
            for event in resp['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
        except Exception as e:
            print("Error: \nThe query file does not exist",e)
        return records
    
    @staticmethod
    def getTotalCompany(pathfile):
        #the number of companies in the json array is queried
        SQL = "SELECT count(0) as cantidadmepresas FROM s3object[*][*] c"
        total_companies = QueryAngel.select_object_bucket(pathfile,SQL)
        total_companies = json.loads(total_companies)["cantidadmepresas"]
        return total_companies

    @staticmethod
    def dataquery(pathfile):#main method
        try:
            company_data =[]
            student_data =[]
            studentdata ={}
            coursedata ={}
            course_data =[]
            items = []
            finaldata={}

            total_companies=QueryAngel.getTotalCompany(pathfile)

            for index in range(total_companies):

                SQLcomp = "SELECT * FROM s3object[*][{}] c".format(index)

                records = QueryAngel.select_object_bucket(pathfile,SQLcomp)
                response_json = json.loads(records)
                print("entre") 

                if "Estudiantes" in response_json:
                    global therestudent
                    therestudent =1

                for student in response_json["Estudiantes"]:

                    #courses data
                    coursedata["idempresa"]=response_json["idempresa"]
                    coursedata["nombreempresa"]=response_json["nombrecompania"]
                    coursedata["idusuario"]=student["idestudiante"]
                    coursedata["email"]=student["email"]

                    #convert string value to numeric value
                    student["tieneubitslearning"] = int(student["tieneubitslearning"])

                    if "Cursos" in student:
                        global therecourses

                        therecourses =1

                        for course in student["Cursos"]:
                            coursedata.update(course)   
                            for n_curso in coursedata:
                                items.append(n_curso)
                                coursedata=QueryAngel.bool_validate(coursedata,n_curso)# changes value of boolean fields
                            course_data.append(QueryAngel.data_to_company(coursedata, items))
                            finaldata['cursos'] = course_data
                            items = []

                    #student data
                    studentdata["idempresa"]= response_json["idempresa"]
                    studentdata["nombreempresa"]= response_json["nombrecompania"]
                    if "finvigencia" in response_json: studentdata["finvigencia"]= response_json["finvigencia"] 
                    if "tamanoempresa" in response_json: studentdata["tamanoempresa"]= response_json["tamanoempresa"]
                    studentdata.update(student)

                    if therecourses == 0:
                        studentdata = studentdata
                    else:
                        studentdata =QueryAngel.removedata(studentdata,"Cursos")

                    for n_student in studentdata:
                        items.append(n_student)
                        studentdata=QueryAngel.bool_validate(studentdata,n_student)# changes value of boolean fields
                    student_data.append(QueryAngel.data_to_company(studentdata, items))
                    finaldata['estudiantes'] = student_data
                    items = []
                    
                   
                #company data
                response_json["nombreinterno"]=f'{response_json["idempresa"]}-{response_json["nombrecompania"]}'
                if therestudent == 0:
                    response_json = response_json
                else:
                    response_json = QueryAngel.removedata(response_json,"Estudiantes")

                for n_data in response_json:# separate the key from each field
                    items.append(n_data)
                    response_json=QueryAngel.bool_validate(response_json,n_data)# changes value of boolean fields
                company_data.append(QueryAngel.data_to_company(response_json, items))
                finaldata['empresas'] = company_data
                items = []  # set the array value to avoid duplication of values

            return finaldata
        except Exception as e:
            print("Error: \nThe query file does not exist", e)
            return None
#   
    @staticmethod
    def change_format_date(data,*args):
        for item in args:
            if item in data:
                if data[item]:
                    if item in data:
                        u_date = datetime.strptime(data[item],"%Y-%m-%dT%H:%M:%SZ") # current date and time
                        dt = u_date.strftime
                        x=f"{dt('%m')}/{dt('%d')}/{dt('%Y')}"
                        data[item]=x
        return data
    
    @staticmethod
    def bool_validate(data,n_item):

        #validate boolean field
        if (data[n_item] == True or data[n_item] == False):
            data[n_item] = QueryAngel.change_value(data[n_item], n_item)
            #change value None to value "" 
        if data[n_item] == None:
            data[n_item]=""
        return data
        
    @staticmethod
    def removedata(factorydata,key):
        del factorydata[key]
        return factorydata

    @staticmethod
    def data_to_company(data,items):
        datacompany={}
        for item in items:
            k=item
            #change field name field namecompany
            if item == "nombrecompania":
                item= "nombre"
            if item == "nombreestudiante":
                item= "nombre"
            datacompany[item]=data[k]
        return datacompany 

    @staticmethod
    def change_value(data, n):
        newvalue = data
        if (n != "idempresa" 
                and n != "idestudiante" 
                and n != "cursosobligatoriosasignados" 
                and n != "cursosobligatoriosterminados"
                and n != "planprogreso"
                and n != "rankingempresa" 
                and n != "cursoslibresterminados"):
            if data == True:
                newvalue = "Si"
            else:
                newvalue = "No"
        return newvalue

    @staticmethod
    def prepare_data_to_send(data):
        datafactory = {}
        datastudent = {}
        datacourse = {}

        for factory_data in data['empresas']:
            time.sleep(2)
            datafactory["empresa"]  =factory_data
            datafactory["empresa"] = QueryAngel.change_format_date(datafactory["empresa"],"iniciovigencia","finvigencia")
            QueryAngel.see_data(datafactory)
            #QueryAngel.send_data_to_sns(datafactory)

        if therestudent == 1:
            for student_data in data['estudiantes']:#####
                time.sleep(2)#######
                datastudent["estudiantes"] = student_data
                datastudent["estudiantes"]  = QueryAngel.change_format_date(datastudent["estudiantes"],"finvigencia","fechacumpleanos","fechaprimerlogin","ultimologin","fechacreacion")
                QueryAngel.see_data(datastudent)
                #QueryAngel.send_data_to_sns(datastudent)

        if therecourses == 1:
            for course_data in data['cursos']:#####
                #time.sleep(1)#######
                datacourse["cursos"] = course_data
                datacourse["cursos"] = QueryAngel.change_format_date(datacourse["cursos"],"fechainicio","fechafinalizacioncompleta")
                #validation for course status
                if datacourse["cursos"]["fechainicio"] != "":
                    datacourse["cursos"]["estado"] ="INICIADO"
                    if "fechafinalizacioncompleta" in datacourse["cursos"]:
                        if datacourse["cursos"]["fechafinalizacioncompleta"] == "":
                            datacourse["cursos"]["estado"] ="INICIADO"
                        else:
                            datacourse["cursos"]["estado"] ="TERMINADO"

                QueryAngel.see_data(datacourse)
                #QueryAngel.send_data_to_sns(datastudent)

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
                "consumer": {
                    'DataType': 'String',
                    'StringValue': 'activeCampaign'
                }
            }
        )

        print(response)

    @staticmethod
    def see_data(data):
        print("data arrive to edy= ", json.dumps(data))
        print()
#        for active in data['estudiantes']:#####
#            time.sleep(2)#######
#            print('\n', active,'\n')

    