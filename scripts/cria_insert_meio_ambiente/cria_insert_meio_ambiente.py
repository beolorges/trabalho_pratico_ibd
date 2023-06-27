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
    Tranquilidade_sonora,
	Ausencia_de_coletivos_poluidores,
	area_de_verde__por_habitante,
    IOL_7_Meio_Ambiente,
    IQVU_7_Meio_Ambiente
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


arquivo = open('cria_insert_meio_ambiente/cria_insert_meio_ambiente.sql', 'w+', encoding="utf-8")

i = 0
for bairro, Tranquilidade_sonora, Ausencia_de_coletivos_poluidores, area_de_verde__por_habitante, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        meio_ambiente_bairro(
            id_bairro,
            tranquilidade_sonora,
            ausencia_coletivos_poluidores,
            area_verde_por_hab,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(Tranquilidade_sonora.replace(',', '.')) if Tranquilidade_sonora != None else 0}', 
        '{float(Ausencia_de_coletivos_poluidores.replace(',', '.')) if Ausencia_de_coletivos_poluidores != None else 0}', 
        '{float(area_de_verde__por_habitante.replace(',', '.')) if area_de_verde__por_habitante != None else 0}', 
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
