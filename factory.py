from abc import ABC, abstractmethod
#fabrica
class Factory(ABC): #abstractfactory

    @abstractmethod
    def get_information(self):
        pass

#from abc import ABC, abstractmethod
#televisor
class Campaign(ABC): #productA
    @abstractmethod
    def dataquery(self):
        pass
