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
    Bens_Tombados,
	Distribuicao_de_equipamentos_culturais,
	Livrarias_e_papelarias_M2_hab,
	Locadoras,
	Banca_de_revista,
    IOL_2_Cultura,
    IQVU_2_Cultura
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


arquivo = open('cria_insert_cultura/cria_insert_cultura.sql', 'w+', encoding="utf-8")

i = 0
for bairro, Bens_Tombados, Distribuicao_de_equipamentos_culturais, Livrarias_e_papelarias_M2_hab, Locadoras, Banca_de_revista, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        cultura_bairro(
            id_bairro,
            bens_tombados,
            distribuicao_de_equipamentos_culturais,
            livraria_papelaria_m2_hab,
            locadoras,
            banca_de_revista,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(Bens_Tombados.replace(',', '.')) if Bens_Tombados != None else 0}', 
        '{float(Distribuicao_de_equipamentos_culturais.replace(',', '.')) if Distribuicao_de_equipamentos_culturais != None else 0}', 
        '{float(Livrarias_e_papelarias_M2_hab.replace(',', '.')) if Livrarias_e_papelarias_M2_hab != None else 0}', 
        '{float(Locadoras.replace(',', '.')) if Locadoras != None else 0}', 
        '{float(Banca_de_revista.replace(',', '.')) if Banca_de_revista != None else 0}', 
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
