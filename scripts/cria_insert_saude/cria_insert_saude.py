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
    Disponibilidade_de_leitos,
	Centros_de_saude,
	Outros_equipamentos_de_assistencia_medica_m2_habitante,
	Equipamentos_odontologicos_m2_habitante,
	Outros_equipamentos_de_assistencia_medica_estabelecimento_hab,
	Equipamentos_odontologicos_estabelecimento_habitante,
    IOL_8_Saude,
    IQVU_8_Saude
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


arquivo = open('cria_insert_saude/cria_insert_saude.sql', 'w+', encoding="utf-8")

i = 0
for bairro, Disponibilidade_de_leitos, Centros_de_saude, Outros_equipamentos_de_assistencia_medica_m2_habitante, Equipamentos_odontologicos_m2_habitante, Outros_equipamentos_de_assistencia_medica_estabelecimento_hab, Equipamentos_odontologicos_estabelecimento_habitante, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        saude_bairro(
            id_bairro,
            disponibilidade_leitos,
            centro_saude,
            demais_equipamentos_medico_m2_hab,
            equipamentos_odontologicos_m2_hab,
            demais_equipamentos_medico_hab,
            equipamentos_odontologicos_hab,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(Disponibilidade_de_leitos.replace(',', '.')) if Disponibilidade_de_leitos != None else 0}', 
        '{float(Centros_de_saude.replace(',', '.')) if Centros_de_saude != None else 0}', 
        '{float(Outros_equipamentos_de_assistencia_medica_m2_habitante.replace(',', '.')) if Outros_equipamentos_de_assistencia_medica_m2_habitante != None else 0}', 
        '{float(Equipamentos_odontologicos_m2_habitante.replace(',', '.')) if Equipamentos_odontologicos_m2_habitante != None else 0}', 
        '{float(Outros_equipamentos_de_assistencia_medica_estabelecimento_hab.replace(',', '.')) if Outros_equipamentos_de_assistencia_medica_estabelecimento_hab != None else 0}', 
        '{float(Equipamentos_odontologicos_estabelecimento_habitante.replace(',', '.')) if Equipamentos_odontologicos_estabelecimento_habitante != None else 0}', 
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
