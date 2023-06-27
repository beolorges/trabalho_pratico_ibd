import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
SELECT 
    DISTINCT (NUMERO_BOLETIM),
	DATA_HORA_BOLETIM,
	DATA_INCLUSAO, 
	DESC_TIPO_ACIDENTE, 
	DESC_TEMPO, 
	PAVIMENTO, 
	DESC_REGIONAL, 
	ORIGEM_BOLETIM, 
	LOCAL_SINALIZADO, 
	VELOCIDADE_PERMITIDA, 
	HORA_INFORMADA, 
	INDICADOR_FATALIDADE
FROM boletim_acidentes;
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


i = 0
for NUMERO_BOLETIM, DATA_HORA_BOLETIM, DATA_INCLUSAO,  DESC_TIPO_ACIDENTE,  DESC_TEMPO,  PAVIMENTO,  DESC_REGIONAL, ORIGEM_BOLETIM, LOCAL_SINALIZADO, VELOCIDADE_PERMITIDA, HORA_INFORMADA, INDICADOR_FATALIDADE in recset:
    if NUMERO_BOLETIM == None or DATA_HORA_BOLETIM == None or DATA_INCLUSAO == None or DESC_REGIONAL == None or DESC_TIPO_ACIDENTE == None or DESC_TEMPO == None or PAVIMENTO == None or ORIGEM_BOLETIM == None or DESC_REGIONAL.replace(' ', '') == "":
        continue
    
    SQL_STRING = f'''
    INSERT INTO 
        boletim_acidentes_bairro(
            numero_boletim,
            data_hora_boletim,
            data_hora_inclusao,
            codigo_acidente,
            id_tipo_tempo,
            id_tipo_pavimento,
            id_regiao,
            id_tipo_agente,
            local_sinalizado,
            velocidade_permitida,
            hora_informada,
            indicador_fatalidade
        )
    SELECT '{NUMERO_BOLETIM}', '{DATA_HORA_BOLETIM}', '{DATA_INCLUSAO}', ta.codigo, tt.id, tp.id, r.id, tag.id, '{LOCAL_SINALIZADO == 'SIM' }', '{VELOCIDADE_PERMITIDA}', '{HORA_INFORMADA == 'SIM'}', '{INDICADOR_FATALIDADE == 'SIM'}'
    FROM 
        tipo_acidente ta,
        tipo_tempo tt,
        tipo_pavimento tp,
        regiao r,
        tipo_agente tag
    WHERE 
        unaccent(ta.descricao) ilike unaccent('%{DESC_TIPO_ACIDENTE}%') 
        AND unaccent(tt.descricao) ilike unaccent('%{DESC_TEMPO}%') 
        AND unaccent(tp.descricao) ilike unaccent('%{PAVIMENTO}%') 
        AND trim(unaccent(r.nome)) ilike trim(unaccent('{DESC_REGIONAL.replace("-", " ")}'))
        AND unaccent(tag.nome) ilike unaccent('%{ORIGEM_BOLETIM}%')
    RETURNING numero_boletim;'''

    cursor.execute(SQL_STRING)
    res = cursor.fetchall()

    if (res == None  or len(res) != 1):
        arquivo = open('cria_insert_boletim/error.sql', 'w+', encoding="utf-8")
        arquivo.write(SQL_STRING)

        raise Exception

    i = i + 1
    print(f'Número de inserções: {i}')

con.commit()
con.close()
