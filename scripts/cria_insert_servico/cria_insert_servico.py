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
    APVP,
	Agencias_bancarias,
	Postos_de_gasolina,
	Farmacias,
	Espacos_publicos_para_inclusao_digital,
	Correios,
	Telefones_Publicos,
	Bancas_de_revista,
    IOL_9_Servicos_Urbanos,
    IQVU_9_Servicos_Urbanos
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


arquivo = open('cria_insert_servico/cria_insert_servico.sql', 'w+', encoding="utf-8")

i = 0
for bairro, APVP, Agencias_bancarias, Postos_de_gasolina, Farmacias, Espacos_publicos_para_inclusao_digital, Correios, Telefones_Publicos, Bancas_de_revista, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        servicos_urbanos_bairro(
            id_bairro,
            apvp,
            agencias_bancarias,
            postos_gasolina,
            farmacias,
            espacos_publicos_inclusao_digital,
            correios,
            telefone_publico,
            banca_de_revista,   
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(APVP.replace(',', '.')) if APVP != None else 0}', 
        '{float(Agencias_bancarias.replace(',', '.')) if Agencias_bancarias != None else 0}', 
        '{float(Postos_de_gasolina.replace(',', '.')) if Postos_de_gasolina != None else 0}', 
        '{float(Farmacias.replace(',', '.')) if Farmacias != None else 0}', 
        '{float(Espacos_publicos_para_inclusao_digital.replace(',', '.')) if Espacos_publicos_para_inclusao_digital != None else 0}', 
        '{float(Correios.replace(',', '.')) if Correios != None else 0}', 
        '{float(Telefones_Publicos.replace(',', '.')) if Telefones_Publicos != None else 0}', 
        '{float(Bancas_de_revista.replace(',', '.')) if Bancas_de_revista != None else 0}', 
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
