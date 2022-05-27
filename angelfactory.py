from factory import Factory
from angel import QueryAngel

#samsung_fabrica
class AngelFactory(Factory): #concretefactory1
    def get_information(self):
        return QueryAngel()

    # def crear_celular(self):
    #     return SonyCelular()