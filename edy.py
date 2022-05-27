import boto3
import json
import time
import os, sys
import uuid
from factory import Campaign

s3 = boto3.client('s3')

#samsung_televisor = main en external
class QueryEdy(Campaign):   #productA1
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
                #Expression="Select count('empresas') from S3Object[*].empresas[*] s",
                InputSerialization = {"JSON": {"Type": "Document"},'CompressionType': 'GZIP'},
                OutputSerialization = {'JSON': {}},
            )
            finaldata={}
            consumidor= {}
            consumidor["consumidor"]="activeCampaign"
            miestdiante={}
            estudiante ={}
            la_empresa ={}
            empresa = {}

            for event in resp['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
                    response_json =json.loads(records)
                    consumidor.update(response_json)
                    print(json.dumps(response_json))
                    print()
                    print()
                    for f in response_json['_1']:
                        la_empresa["empresas"]=QueryEdy.miempresa(f)
                        print("\n",la_empresa,"\n\n")
                        for a in f["Estudiantes"]:
                            j=0
                            otroscursos = []
                            cursos ={}
                            for c in a["Cursos"]:
                                j+=1
                                #print(c)
                                #cursos[j]=c["bit_nombre"]
                                otroscursos.append(c["bit_nombre"])
                            #print(cursos)

                            
                            #dataactive =(
                                #{
                            estudiante["idestudiante"]=a["est_id"]
                            estudiante["idempresa"]=f["comp_id"]
                            estudiante["nombreempresa"]=f['comp_nombre']
                            estudiante["finvigencia"]=f['comp_finvigencia']
                            estudiante["tamanoempresa"]=f['comp_tamano']
                            #estudiante[#"licenciasempresa"]=a['est_nombre']
                            estudiante["esnuevoestudiante"]=a['est_nuevo']
                            estudiante["ultimologin"]=a['est_ultimologin']
                            estudiante["email"]=a['est_email']
                            #estudiante[#"cursosobligatoriosasignados"]=a['est_nombre']
                            #estudiante[#"cursosobligatoriosterminados"]=a['est_nombre']
                            #estudiante[#"cursoslibresterminados"]=a['est_nombre']
                            #estudiante[#"planprogreso"]=a['est_nombre']
                            #estudiante[#"rolusuario"]=a['est_nombre']
                            estudiante["nombre"]=a['est_nombre']
                            estudiante["apellido"]=a['est_nombre']
                            estudiante["cargo"]=a['est_cargo']
                            #estudiante[#"usuario"]=a['est_nombre']
                            estudiante["fechacumpleanos"]=a['est_cumpleanos']
                            estudiante["fechaprimerlogin"]=a['est_primerlogin']
                            estudiante["usuariosuspendido"]=a['est_suspendido']
                            estudiante["tieneubitslearning"]=a['est_learning']
                            #estudiante[#"tieneplug"]=a['est_nombre']
                            estudiante["segmentacionSSestudiantes"]=a['est_segmentacion']
#                            cursos[1]="comercio"
#                            cursos[2]="ventas"
#                            cursos[3]="150000"
#                            cursos[4]="mayo"
                            x = ",".join(otroscursos)
                            cursos["miscursos"]=x
                            #estudiante["cursosinteres"]=cursos
                            #estudiante["rankingempresa"]=a['est_nombre']
                            #estudiante[#"pais"]=a['est_nombre']
                            #estudiante[#"fechacreacion"]=a['est_nombre']
                            #estudiante[#"10minutos"]=a['est_nombre']
                            #estudiante[#"ciudad"]=a['est_nombre']
                            #estudiante[#"niveleducativo"]=a['est_nombre']
                            #estudiante[#"genero"]=a['est_nombre']
                            #estudiante[#"telefono"]=a['est_nombre']
                            #estudiante[#"correopersonal"]=a['est_nombre']
                            #estudiante[#"areasconocimiento"]=a['est_nombre']
                            #estudiante[#"linkedin"]=a['est_nombre']

                                    #}
                            #)
                            miestdiante["estudiantes"]=estudiante
                            print(miestdiante,'\n\n')
    

                    #data for accounts
#                    res_company = response_json.get('empresas',[])
#
#                    for factorydata in res_company:
#                        for n_data in factorydata:#separate the key from each field
#                            item.append(n_data)
#                            if  (factorydata[n_data] == True or factorydata[n_data] == False):#changes value of boolean fields
#                                factorydata[n_data]=QueryEdy.change_value(factorydata[n_data],n_data)
#                            
#                        company_data.append(QueryEdy.data_to_company(factorydata,item))
#                        finaldata['empresas']=company_data
#                        item = []#set the array value to avoid duplication of values
#
#                    #data for students
#                    res_student = response_json.get('estudiantes',[])
#
#                    for studentdata in res_student:
#                        for n_student in studentdata:
#                            item.append(n_student)
#                            if  studentdata[n_student] == True or studentdata[n_student] == False:
#                                studentdata[n_student]=QueryEdy.change_value(studentdata[n_student],n_student)
#                        student_data.append(QueryEdy.data_to_company(studentdata,item))
#                        finaldata['estudiantes']=student_data
#                        item = []
#
#            return finaldata
        except Exception as e:
            print("Error: \nThe query file does not exist",e)
#            return None
#    
#    @staticmethod
#    def data_to_company(data,item):
#        reviewdata={}
#        #eif 'isnuevaempresa' in data:
#        reviewdata=QueryEdy.datareview(data,item)
#        return reviewdata
#    
#    @staticmethod
#    def datareview(data,item): 
#        datacompany={}
#        for n in item:
#            datacompany[n]=data[n]
#        return datacompany 
#    
#    @staticmethod
#    def change_value(data,n):
#        newvalue =data
#        if (n!= "idempresa" 
#            and n!="idestudiante" 
#            and n!="cursosobligatoriosasignados" 
#            and n!="cursosobligatoriosterminados" 
#            and n!="cursoslibresterminados"):
#            if data == True:
#                newvalue="Si"
#            else:
#                newvalue="No"
#        return newvalue
#    
#    @staticmethod
#    def prepare_data_to_send(data):
#            
#            datafactory={}
#            datastudent ={}
#            #QueryEdy.send_data_to_sns(data)
#            
#            for factory_data in data['empresas']:
#                #time.sleep(2)
#                datafactory["empresa"]=factory_data
#                QueryEdy.see_data(datafactory)
#                #QueryEdy.send_data_to_sns(datafactory)
#                
#            for student_data in data['estudiantes']:#####
#                #time.sleep(2)#######
#                datastudent["estudiantes"]=student_data
#                QueryEdy.see_data(datastudent)
#                #QueryEdy.send_data_to_sns(datastudent)
#
#
#    @staticmethod
#    def send_data_to_sns(data_frame):
#        message = data_frame
#        client = boto3.client('sns')
#        topic_arn = os.environ['sns_arn']
#
#        print("Data to SNS ", json.dumps(message))
#
#        response = client.publish(
#            TargetArn = topic_arn,
#            Message=json.dumps({'default': json.dumps(message)}),
#            MessageStructure= 'json',
#            MessageAttributes={
#                "consumer":{
#                    'DataType':'String',
#                    'StringValue':'activeCampaign'
#                }
#            }
#        )
#
#        print(response)
#
#    @staticmethod
#    def see_data(data):
###        print("data arrive to edy= ",json.dumps(data))
##        for active in data['estudiantes']:#####
##            time.sleep(2)#######
##            print('\n', active,'\n')
    @staticmethod
    def miempresa(data):
        empresa={}
        empresa["comp_nombre"]=data["comp_nombre"]
        empresa["comp_tamano"]=data["comp_tamano"]
        empresa["comp_iniciovigencia"]=data["comp_iniciovigencia"]
        empresa["comp_finvigencia"]=data["comp_finvigencia"]
        empresa["comp_tipoplan"]=data["comp_tipoplan"]
        empresa["comp_tiempominimo"]=data["comp_tiempominimo"]

        return empresa
    