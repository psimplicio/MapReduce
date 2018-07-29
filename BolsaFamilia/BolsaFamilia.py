from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class BolsaFamilia(MRJob):

    def __init__(self, *args, **kwargs):
        super(BolsaFamilia, self).__init__(*args, **kwargs)

    def mapper(self,_,line):
        
        try:
            (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, data, valor) = line.split(';')
            yield (UF.replace('"',''), nomeMunicipio.replace('"','')), (float(0), float(valor.replace('"','').replace(',','.')))
        
        except TypeError:
            yield (None, None), (None, None)

        except ValueError:
            try:
                (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, valor) = line.split(';')
                yield (UF.replace('"',''), nomeMunicipio.replace('"','')), (float(valor.replace('"','').replace(',','.')), float(0))
            
            except ValueError:
                pass
            
            except TypeError:
                yield (None, None), (None, None)
    
    def reducer(self, key, values):
        
        for value in values:
            yield key, value
    
    def mapper_exclude_nulls(self,key,value):
        if value is not None:
            yield key, value

    def reducer_sum(self,key,values):
        pagamento = []
        saque = []
        saldo = []

        for pag, saq in values:
            pagamento.append(pag)
            saque.append(saq)
            saldo.append(pag - saq)
        
        yield key, (sum(pagamento), sum(saque), sum(saldo))
             

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper_exclude_nulls, reducer=self.reducer_sum)
        ]

if __name__ == '__main__':
    BolsaFamilia.run()
