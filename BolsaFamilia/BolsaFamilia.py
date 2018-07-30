from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class BolsaFamilia(MRJob):

# Esta classe tem por finalidade aplicar o processo de mapeamento e redução em 2 datasets que representam pagamentos e 
# saques do programa social Bolsa Familia. O output do processo é um dataset contendo valor pago, valor sacado e o saldo
# por UF e municipio.

# Construtor da clase
    def __init__(self, *args, **kwargs):
        super(BolsaFamilia, self).__init__(*args, **kwargs)

    def mapper(self,_,line):
        
        try:
            # Fatia o arquivo em colunas retirando aspas duplas das strings e alterando ',' por '.' para posterior conversão 
            # dos dados numéricos para float.
            # Este primeiro bloco representa o arquivo de saques contendo 9 colunas caso ocorra falha no split é gerada uma
            # exceção direcionando o fluxo para o proximo bloco que espera um arquivo com 8 colunas.
            (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, data, valor) = line.split(';')
            yield (UF.replace('"',''), nomeMunicipio.replace('"','')), (float(0), float(valor.replace('"','').replace(',','.')))
        
        except TypeError:
            # Caso a conversão dos dados numéricos para float gere uma exceção, o fluxo é direcionado para esta linha.
            # É esperado que as linhas de cabeçalho gerem essa exceção e fiquem identificadas como nulas para serem
            # filtradas no proximo mapper.
            yield (None, None), (None, None)

        except ValueError:
            # Este segundo bloco trata o arquivo de pagamentos que contem 8 colunas.
            try:
                (mesReferencia, mesCompetencia, UF, codigoMunicipio, nomeMunicipio, NIS, nome, valor) = line.split(';')
                yield (UF.replace('"',''), nomeMunicipio.replace('"','')), (float(valor.replace('"','').replace(',','.')), float(0))
            
            # Caso ocorra qualquer outro erro de valor a linha é ignorada
            except ValueError:
                pass
            
            except TypeError:
                yield (None, None), (None, None)
    
    def reducer(self, key, values):
        
        for value in values:
            yield key, value
    
    def mapper_exclude_nulls(self,key,value):
        # Filtra os registros nulos (cabeçalhos)
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
