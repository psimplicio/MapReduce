from mrjob.job import MRJob

class Enem(MRJob):

    def mapper(self,_,line):

        # Faz o split apenas das colunas referentes as informações do candidato
        (idade, tp_sexo, nacionalidade, cod_municipio_nascimento, no_municipio_nascimento, cod_uf_nascimento, 
            uf_nascimento, st_conclusao, ano_concluiu, tp_escola, in_tp_ensino, tp_estado_civil, tp_cor_raca) = line.split(',')[15:28]

        try:
            faixa_idade = ''
            if int(idade) <= 18:
                faixa_idade = 'Ate 18 anos'
            elif int(idade) > 18 and int(idade) <= 25 :
                faixa_idade = 'De 19 a 25 anos'
            elif int(idade) > 25 and int(idade) <= 30 :
                faixa_idade = 'De 26 a 30 anos'
            elif int(idade) > 30 and int(idade) <= 40 :
                faixa_idade = 'De 31 a 40 anos'
            elif int(idade) > 40 and int(idade) <= 50 :
                faixa_idade = 'De 41 a 50 anos'
            elif int(idade) > 50 and int(idade) <= 60 :
                faixa_idade = 'De 51 a 60 anos'
            else:
                faixa_idade = 'Maior de 60 anos'
        
            raca = tp_cor_raca.replace('0','ND').replace('1', 'Branca').replace('2', 'Preta').replace('3', 'Parda').replace('4', 'Amarela').replace('5', 'Indigena').replace('TP_COR_RACA', '')
            yield (raca, faixa_idade), 1
        
        except ValueError:
            pass
        

    def reducer(self,key,value):

        yield key, sum(value)


if __name__ == '__main__':
    Enem.run()

