from factory import Factory
from edy import QueryEdy

#samsung_fabrica
class EdyFactory(Factory): #concretefactory1
    def get_information(self):
        return QueryEdy()

    # def crear_celular(self):
    #     return SonyCelular()