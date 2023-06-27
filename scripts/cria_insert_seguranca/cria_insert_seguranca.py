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
    Ausencia_de_criminalidade_homicidios,
	Ausencia_de_tentativas_de_homicidio,
	Ausencia_de_crimes_contra_a_pessoa_tentados_e_consumados,
	Ausencia_de_crimes_contra_o_patrimonio_roubo_e_furto,
	Ausencia_de_furto_de_veiculos,
	Ausencia_de_acidentes_de_tr�nsito,
    IOL_10_Seguranca_urbana,
    IQVU_10_Seguranca_urbana
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


arquivo = open('cria_insert_seguranca/cria_insert_seguranca.sql', 'w+', encoding="utf-8")

i = 0
for bairro, Ausencia_de_criminalidade_homicidios, Ausencia_de_tentativas_de_homicidio, Ausencia_de_crimes_contra_a_pessoa_tentados_e_consumados, Ausencia_de_crimes_contra_o_patrimonio_roubo_e_furto, Ausencia_de_furto_de_veiculos, acidente_transito, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        seguranca_bairro(
            id_bairro,
            ausencia_homicidio,
            ausencia_tentativa_homicidio,
            ausencia_crimes_contra_pessoa_tentato_ou_consumado,
            ausencia_crimes_contra_patrimonio_roubo_e_furto,
            ausencia_furto_veiculos,
            ausencia_acidente_transito,  
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(Ausencia_de_criminalidade_homicidios.replace(',', '.')) if Ausencia_de_criminalidade_homicidios != None else 0}', 
        '{float(Ausencia_de_tentativas_de_homicidio.replace(',', '.')) if Ausencia_de_tentativas_de_homicidio != None else 0}', 
        '{float(Ausencia_de_crimes_contra_a_pessoa_tentados_e_consumados.replace(',', '.')) if Ausencia_de_crimes_contra_a_pessoa_tentados_e_consumados != None else 0}', 
        '{float(Ausencia_de_crimes_contra_o_patrimonio_roubo_e_furto.replace(',', '.')) if Ausencia_de_crimes_contra_o_patrimonio_roubo_e_furto != None else 0}', 
        '{float(Ausencia_de_furto_de_veiculos.replace(',', '.')) if Ausencia_de_furto_de_veiculos != None else 0}', 
        '{float(acidente_transito.replace(',', '.')) if acidente_transito != None else 0}', 
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
