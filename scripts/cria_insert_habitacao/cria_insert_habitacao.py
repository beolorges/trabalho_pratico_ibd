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
    area_residencial_adequada,
	Padrao_de_acabamento,
	Seguranca_do_Terreno_indice_de_risco_geologico_efetivo,
	indice_de_Salubridade_Ambiental,
    IOL_5_Habitacao,
    IQVU_5_Habitacao
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


arquivo = open('cria_insert_habitacao/cria_insert_habitacao.sql', 'w+', encoding="utf-8")

i = 0
for bairro, area_residencial_adequada, Padrao_de_acabamento, Seguranca_do_Terreno_indice_de_risco_geologico_efetivo, indice_de_Salubridade_Ambiental, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        habitacao_bairro(
            id_bairro,
            area_residencial_adequada,
            padrao_acabamento,
            indice_geologico,
            indice_salubridade,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(area_residencial_adequada.replace(',', '.')) if area_residencial_adequada != None else 0}', 
        '{float(Padrao_de_acabamento.replace(',', '.')) if Padrao_de_acabamento != None else 0}', 
        '{float(Seguranca_do_Terreno_indice_de_risco_geologico_efetivo.replace(',', '.')) if Seguranca_do_Terreno_indice_de_risco_geologico_efetivo != None else 0}', 
        '{float(indice_de_Salubridade_Ambiental.replace(',', '.')) if indice_de_Salubridade_Ambiental != None else 0}', 
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
