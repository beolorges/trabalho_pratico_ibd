import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
SELECT 
    NOMEUP,
    agua_tratada,
	Rede_de_Esgoto,
	Fornecimento_de_Energia_Eletrica,
	Possibilidade_de_acesso_Pavimentacao,
	Numero_de_veiculos_por_habitante,
	Frequencia_das_linhas_por_UP,
	Conforto_idade_media_da_frota,
    IOL_6_Infra_estrutura_urbana,
    IQVU_6_Infra_estrutura_urbana
FROM 
    QUALIDADE_VIDA_URBANA
WHERE
    ano = '2016';
'''

cursor.execute(SQL)
recset = cursor.fetchall()
con.close()

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBH',
    user='postgres', 
    password='postgres')

cursor = con.cursor()


arquivo = open('cria_insert_infraestrutura/cria_insert_infraestrutura.sql', 'w+', encoding="utf-8")

i = 0
for bairro, agua_tratada, Rede_de_Esgoto, Fornecimento_de_Energia_Eletrica, Possibilidade_de_acesso_Pavimentacao, Numero_de_veiculos_por_habitante, Frequencia_das_linhas_por_UP, Conforto_idade_media_da_frota, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        infraestrutura_urbana_bairro(
            id_bairro,
            agua_tratada,
            rede_esgoto,
            energia_eletrica,
            acesso_pavimentacao,
            veiculos_por_habitante,
            frequencia_linhas_onibus,
            idade_media_onibus,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(agua_tratada.replace(',', '.')) if agua_tratada != None else 0}', 
        '{float(Rede_de_Esgoto.replace(',', '.')) if Rede_de_Esgoto != None else 0}', 
        '{float(Fornecimento_de_Energia_Eletrica.replace(',', '.')) if Fornecimento_de_Energia_Eletrica != None else 0}', 
        '{float(Possibilidade_de_acesso_Pavimentacao.replace(',', '.')) if Possibilidade_de_acesso_Pavimentacao != None else 0}', 
        '{float(Numero_de_veiculos_por_habitante.replace(',', '.')) if Numero_de_veiculos_por_habitante != None else 0}', 
        '{float(Frequencia_das_linhas_por_UP.replace(',', '.')) if Frequencia_das_linhas_por_UP != None else 0}', 
        '{float(Conforto_idade_media_da_frota.replace(',', '.')) if Conforto_idade_media_da_frota != None else 0}', 
        '{float(iol.replace(',', '.')) if iol != None else 0}', 
        '{float(iqvu.replace(',', '.')) if iqvu != None else 0}'
    FROM 
        bairro b
    WHERE 
        unaccent(b.nome) ilike unaccent('%{bairro}%');\n\n'''

    arquivo.write(SQL_STRING)  
    cursor.execute(SQL_STRING)

    i = i + 1
    print(f'Número de inserções:{i}')

con.commit()
con.close()
arquivo.close()
