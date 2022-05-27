import json#####
import time
######///import boto3
from activefactory import ActiveFactory
from edifactory import EdyFactory
from angelfactory import AngelFactory

#from main import Main
#from jane_csv import QueryData###
#cliente
#jane = QueryData()#####
#hello = jane.dataquery('activecamdata/archactive.json')#####
#print(json.dumps(hello))###
#finaldata={}#####
#finaldata["prueva"]="falsedad"
#finaldata["empresas"]=hello###
#hello = json.dumps(hello)  ###
#hello = json.loads(hello)  ###

#print(finaldata)
#print("saludos",json.dumps(hello))########
# j=0

#for active in hello['empresas']:#####
#    time.sleep(2)#######
#    print(active)#######

    #tmp = socket.recv(4096)

#     #j+=1
#
#     print(json.dumps(active['categoria']))

# s3 = boto3.resources('s3')
# def lambda_handler(event, context):
#     #trae datos del objeto o archivo puesto en s3
#     for record in event['Records']:
#         bucket = record['s3']['bucket']['name']
#         archivo = record['s3']['object']['key']


#################### funcionalidad con abstract factory ##################
class ClientCamp:
    def client_code(self, factory):
        #main = Main()
        hello = factory.get_information()

        #ok =hello.dataquery('activecamdata/properties_event_active.json.gz')
        ok =hello.dataquery('pruebas/update.json.gz')
        #ok =hello.dataquery('MuestraUBits.json.gz')
        
        #ok =hello.dataquery('activecamdata/analytics.json.gz')
        if  ok != None:
            hello.prepare_data_to_send(ok)



c= ClientCamp()
#c.client_code(ActiveFactory())
#c.client_code(EdyFactory())
c.client_code(AngelFactory())