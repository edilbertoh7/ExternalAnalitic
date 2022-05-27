from factory import Factory
from jane_csv import QueryData

#sony_fabrica
class ActiveFactory(Factory): #concretefactory1
    def get_information(self):
        return QueryData()

    # def crear_celular(self):
    #     return SonyCelular()