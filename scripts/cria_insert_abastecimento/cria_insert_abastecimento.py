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
    Hiper_e_Supermercados_M2_hab,
	Hiper_e_Supermercados_estabelecimentos_hab,
	Mercearias_e_similares_M2_hab,
	Mercearias_e_similares_estabelecimentos_hab,
	Restaurantes_e_similares,
	Abrangencia_tiragem_de_publicacoes_locais,
    IOL_1_Abastecimento,
    IQVU_1_Abastecimento
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


arquivo = open('cria_insert_abastecimento/cria_insert_abastecimento.sql', 'w+', encoding="utf-8")

i = 0
for bairro, Hiper_e_Supermercados_M2_hab, Hiper_e_Supermercados_estabelecimentos_hab, Mercearias_e_similares_M2_hab, Mercearias_e_similares_estabelecimentos_hab, Restaurantes_e_similares, Abrangencia_tiragem_de_publicacoes_locais, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        abastecimento_bairro(
            id_bairro,
            hipermercado_supermercado_m2_hab,
            hipermercado_supermercado_estabelecimento_hab,
            mercearia_m2_hab,
            mercearia_estabelecimento_hab,
            restaurante,
            abrangecia_tiragem_publicacao_local,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(Hiper_e_Supermercados_M2_hab.replace(',', '.')) if Hiper_e_Supermercados_M2_hab != None else 0}', 
        '{float(Hiper_e_Supermercados_estabelecimentos_hab.replace(',', '.')) if Hiper_e_Supermercados_estabelecimentos_hab != None else 0}', 
        '{float(Mercearias_e_similares_M2_hab.replace(',', '.')) if Mercearias_e_similares_M2_hab != None else 0}', 
        '{float(Mercearias_e_similares_estabelecimentos_hab.replace(',', '.')) if Mercearias_e_similares_estabelecimentos_hab != None else 0}', 
        '{float(Restaurantes_e_similares.replace(',', '.')) if Restaurantes_e_similares != None else 0}', 
        '{float(Abrangencia_tiragem_de_publicacoes_locais.replace(',', '.')) if Abrangencia_tiragem_de_publicacoes_locais != None else 0}', 
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
