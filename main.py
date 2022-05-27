import json
class Main:
    @staticmethod
    def transform_json_data(data,item):
        #print('impresion del item = ',item)
        print('\n\n')
        finaldata = {}
        customs={}
        customs1=[]
        dataactive={}
        lang = 'esi'
        # if data['isnuevaempresa'] == False:
        #     data['isnuevaempresa'] = "false"
        saludo = 'hola mundo\n\n' if lang=='es' else ''
        print(saludo)
        if data['isnuevaempresa']==True:
            dataactive = (
                {
                    "idempresa":data["idempresa"],
                    "isnuevaempresa":data["isnuevaempresa"],
                    "name": data["nombre"],
                    "accountUrl": data["url"],
                    "owner": 1,
                    "customs":[
                        {
                            "iniciovigencia":data["iniciovigencia"],
                            "finvigencia":data["finvigencia"],
                            "segmentacionSSempresa":data["segmentacionSSempresa"],
                            "tipodeplan":data["tipodeplan"],
                            "tiempominimo":data["tiempominimo"],                            
                        }
                    ]                
                }
            )
        else:
            for n in item:
                print('valo de n = ',data)
                dataactive[n]=data[n]
            if data["isnuevaempresa"]==False:
                dataactive["isnuevaempresa"]="holadesdelaempresa"
            #dataactive = dataactive["isnuevaempresa"]="holadesdelaempresa" if data["isnuevaempresa"]==False else ''
                
            
            
        
        #customs["iniciovigencia"]= data["iniciovigencia"]
        #customs["finvigencia"]= data["finvigencia"]
        #customs["segmentacionSSempresa"]= data["segmentacionSSempresa"]
        #customs["tipodeplan"]= data["tipodeplan"]
        #customs["tiempominimo"]= data["tiempominimo"]
        
        #customs1.append(customs)
        #finaldata["customs"] = customs1
       

        
        #finaldata["empresas"]=dataactive
        return dataactive
    @staticmethod
    def transform_json_data1(data):
        dataactive = (
            {
                "email": data['email'],
                "firstName": data['nombre'],
                "lastName": data['apellido'],
            }

        )

        return dataactive

    @staticmethod
    def transform_json_data2(data):
        dataactive = (
            {
#                "eventoscursofinalizado": [
#                    {
                        "idusuario": data['idusuario'],
                        "email": data['email'],
                        "categoria": data['categoria'],
                        # "mandatory courses assigned": data.mandatory_courses_assigned,
                        # "finished free courses": data.free_courses_count,
                        # "progress plan": data.progress_percent,
                        # "user name": data.name,
                        # "last name": data.last_name,
                        # "user organization id": data.organization_id,
                        # "email": data.email,
                        # "role": data.role,
                        # "organization name": data.organization_name,
                        # "organization size": data.organization_size,
                        # "organization renewal date": data.organization_renewal_date
#                    }
#                ],
                #"user_id": data.user_id
            }
        )

        return dataactive
    @staticmethod
    def verdata(data):
        print("la data que llega = ",json.dumps(data))