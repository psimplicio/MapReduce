from mrjob.job import MRJob
from mrjob.step import MRStep

class BolsaFamiliaPagamentos(MRJob):

    def mapper(self,_,line):
      
        try:
            (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, valor) = line.split(";")        
            yield UF.replace('"',''),float(valor.replace('"','').replace(',','.')) 
        except:
            yield None, None

    def reducer(self,UF,valores):
        for valor in valores:            
            yield UF, valor

    def mapper_exclude_nulls(self,key,value):
        if key is not None:
            yield key, value
    
    def reducer_sum_values(self,key,value):
        yield key, sum(value)

    def steps(self):
        return[
            MRStep(mapper = self.mapper, reducer = self.reducer),
            MRStep(mapper = self.mapper_exclude_nulls, reducer = self.reducer_sum_values)
        ]
if __name__ == '__main__':
    BolsaFamiliaPagamentos.run()