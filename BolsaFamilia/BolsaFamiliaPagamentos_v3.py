from mrjob.job import MRJob
from mrjob.step import MRStep

class BolsaFamiliaPagamentos(MRJob):

    def mapper(self,_,line):

        try:
            
            (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, valor) = line.split(';')
            yield (UF.replace('"',''), nomeMunicipio.replace('"','')), float(valor.replace('"','').replace(',','.'))

        except:
            yield (None, None), None

    def reducer(self,localidade,valores):

        for valor in valores:
            yield localidade, valor

    def mapper_exclude_nulls(self,localidade, valor):
        if valor is not None:
            yield localidade, valor

    def reducer_sum(self,localidade,valor):
        yield localidade, sum(valor)

    def steps(self):
        return[
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper_exclude_nulls, reducer=self.reducer_sum)
        ]

if __name__ == '__main__':
    BolsaFamiliaPagamentos.run()