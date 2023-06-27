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
	Quadras_campos_e_pistas_de_cooper,
    IOL_4_Esportes,
    IQVU_4_Esportes
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


arquivo = open('cria_insert_esporte/cria_insert_esporte.sql', 'w+', encoding="utf-8")

i = 0
for bairro, quadras, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        habitacao_bairro(
            id_bairro,
            quadras_campos_cooper,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(quadras.replace(',', '.')) if quadras != None else 0}', 
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
