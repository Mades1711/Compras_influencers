import fdb
import pandas as pd 
import warnings
from decouple import config 
warnings.filterwarnings('ignore')
import mysql.connector
import traceback
from mysql.connector import Error

query = """
  WITH
FINANCEIRO AS (
SELECT
	F.COD_FATURATRANSACAO,
	SUM(F.TOTAL) TOTAL
FROM
	FINLANCAMENTO F
GROUP BY
	F.COD_FATURATRANSACAO
),
TOTAL_PRODUTO AS (
SELECT  
	OP.COD_ORDEMSERVICOCAIXA ,
	SUM(OP.VALORORIGINAL * OP.QUANTIDADE) VALORORIGINAL,
	SUM(OP.VALORUNITARIO * OP.QUANTIDADE) VALORUNITARIO
FROM
	ORDEMSERVICOCAIXA_PRODUTO OP
GROUP BY
	OP.COD_ORDEMSERVICOCAIXA 
)
SELECT
	E.NOME AS LOJA,
	T.NUMEROTRANSACAO AS "Nº VENDA",
	O.COD_ORDEMSERVICOCAIXA AS OS,
	TRIM(P.NOME) AS CLIENTE,
	T.DATAEMISSAO AS "DATA_VENDA",
	CASE
		WHEN O.OBSERVACAO IS NULL THEN ''
		ELSE CAST(SUBSTRING(O.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320))
	END AS OBSERV,
	CASE
		WHEN S2.DESCRICAO = '1_CAMPANHA SEM CUPOM DESCONTO' THEN ' '
		ELSE S2.DESCRICAO
	END AS DIVULDGADOR,
	(
      CASE
		WHEN S2.DESCRICAO = 'EXAGERADO' THEN OP.VALORORIGINAL 
		WHEN F.TOTAL IS NOT NULL AND (S2.DESCRICAO <> 'EXAGERADO' OR S2.DESCRICAO <> 'BLACKFRIDAY')  THEN OP.VALORUNITARIO - f.TOTAL
		ELSE OP.VALORORIGINAL 
	END
    ) AS TOTAL
FROM
	ORDEMSERVICOCAIXA O
JOIN TRANSACAO T 
    ON
	(T.COD_TRANSACAO = O.COD_TRANSACAO
		AND T.COD_TRANSACAOORIGEM IS NULL)
JOIN PESSOA E
    ON
	(E.COD_PESSOA = T.COD_EMPRESA)
JOIN PESSOA P 
    ON
	(P.COD_PESSOA = T.COD_PESSOA)
JOIN TOTAL_PRODUTO OP  
    ON
	(OP.COD_ORDEMSERVICOCAIXA = O.COD_ORDEMSERVICOCAIXA)
LEFT JOIN SAIDA S 
    ON
	(S.COD_SAIDA = T.COD_TRANSACAO)
LEFT JOIN SAIDAMOTIVO S2
    ON
	(S2.COD_SAIDAMOTIVO = S.COD_SAIDAMOTIVO)
LEFT JOIN FINANCEIRO F
  	ON
	(F.COD_FATURATRANSACAO = T.COD_FATURATRANSACAO)
WHERE
	P.CPF IN ({cpf_inf})
	AND O.REPARO = 'F'
	AND S2.DESCRICAO NOT LIKE '%CORTESIA%'
	AND     
    	CASE
		WHEN O.OBSERVACAO IS NULL THEN ''
		ELSE CAST(SUBSTRING(O.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320))
	END 
      	NOT LIKE '%CORTESIA%'

"""
query_inf =  """
  select 
    cpf
  from influencer.cpf_inf
"""
query1 = """
  select 
    id_contrato Contrato,
    nome_influencer Influencer,
    `user` "Usuário",
    data_inclusao "Data Inclusão",
    data_inicio "Data Inicio",
    data_final "Data Final",
    valor "Valor Permuta",
    ifnull(observacao,'') "Observação"
  from influencer.contratos
"""

def Connect():
  conn =fdb.connect(
      host=config('host'), 
      database=config('database'),
      user=config('user'), 
      password=config('password'),
      charset = 'WIN1252'
    )
  return conn

def Connect_mysql():
  conn =mysql.connector.connect(
      host=config('host1'), 
      database=config('database1'),
      user=config('user1'), 
      password=config('password1')
    )
  return conn

def consulta(cpf_inf):
    try:
      conn = Connect()
      cursor = conn.cursor()
      df= pd.read_sql(query.format(cpf_inf=cpf_inf), Connect())
      df['Data_venda'] = pd.to_datetime(df['Data_venda'])      
      cursor.close()
      conn.close()
      
      return df
    except Exception as e:
        traceback.print_exc()
    return None
    
def cpf_inf():
    conn = Connect_mysql()
    cursor = conn.cursor()
    try:
      df= pd.read_sql(query_inf, conn)   
      cursor.close()
      conn.close()
      cpf_list = df['cpf'].tolist()
      cpf_list = ', '.join(f"'{cpf}'" for cpf in cpf_list)
      return cpf_list
    except Exception as e:
       return None

def contratos():
    conn = Connect_mysql()
    cursor = conn.cursor()
    try:
      df= pd.read_sql(query1, conn)
      df['Data Inclusão'] = pd.to_datetime(df['Data Inclusão'])
    
      df['Data Inicio'] = pd.to_datetime(df['Data Inicio'])
     
      df['Data Final'] = pd.to_datetime(df['Data Final'])      
      cursor.close()
      conn.close()
      
      return df
    except Exception as e:
       return None


def insert_data(perm):
  try:
      conn = Connect_mysql()
      cursor = conn.cursor()
      sql_insert_query = """
        INSERT INTO contratos (id_contrato, nome_influencer, user, data_inclusao, data_inicio, data_final, valor, observacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
      """
      valores = perm
      cursor.execute(sql_insert_query, valores)
      conn.commit()
      return True
  except mysql.connector.Error as error:
      msg = "Falha ao inserir dados no MySQL: {}".format(error)
      return msg
  finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        


def delete_data(del_contrato):
  try:
      conn = Connect_mysql()
      cursor = conn.cursor()
      sql_insert_query = """
        DELETE FROM influencer.contratos
        WHERE id_contrato = %s
      """
      valores = del_contrato
      cursor.execute(sql_insert_query, (valores,))
      conn.commit()
      return True
  except mysql.connector.Error as error:
      msg = "Falha ao inserir dados no MySQL: {}".format(error)
      return msg
  finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        


def insert_cpf(new_inf):
  try:
      conn = Connect_mysql()
      cursor = conn.cursor()
      sql_insert_query = """
        INSERT INTO influencer.cpf_inf (cpf)
        VALUES (%s)
      """
      cursor.execute(sql_insert_query, (new_inf,))
      conn.commit()
      
      return True
  except mysql.connector.Error as error:
      msg = "Falha ao inserir dados no MySQL: {}".format(error)
      return msg
  finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        


def delete_cpf(new_inf):
  try:
      conn = Connect_mysql()
      cursor = conn.cursor()
      sql_insert_query = """
        DELETE FROM influencer.cpf_inf
        WHERE cpf = %s
      """
      cursor.execute(sql_insert_query, (new_inf,))
      conn.commit()
      
      return True
  except mysql.connector.Error as error:
      msg = "Falha ao inserir dados no MySQL: {}".format(error)
      print(msg)
      return msg
  finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        
def extrato_tot(contrato,data):
  extrato_contratos = contrato.copy()
  extrato_contratos = extrato_contratos[['Contrato','Observação','Influencer','Data Inicio','Valor Permuta']]
  extrato_contratos = extrato_contratos.rename(columns={
      'Data Inicio': 'Data',
      'Valor Permuta': 'Valor',
      'Contrato' : 'Documento'
  })
  extrato_contratos['Tipo'] = 'Permuta'
  extrato_compras = data
  extrato_compras = extrato_compras[['Nº Venda', 'OBSERV','CLIENTE','Data_venda','TOTAL']]
  extrato_compras['TOTAL'] = extrato_compras['TOTAL']*-1
  extrato_compras = extrato_compras.rename(columns={
      'CLIENTE' : 'Influencer',
      'Data_venda' : 'Data',
      'TOTAL' : 'Valor',
      'Nº Venda': 'Documento',
      'OBSERV': 'Observação'
  })
  extrato_compras['Tipo'] = 'Compra'
  extrato = pd.concat([extrato_compras,extrato_contratos], ignore_index=True)
  extrato = extrato[['Tipo','Documento','Observação','Influencer','Data','Valor']]
  return extrato

