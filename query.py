import fdb
import pandas as pd 
import warnings
from decouple import config 
warnings.filterwarnings('ignore')
import mysql.connector
import traceback
from mysql.connector import Error

query = """
WITH TRANS AS (
	SELECT
		COD_TRANSACAO,
		COD_TRANSACAOORIGEM,
		T.DATAEMISSAO,
		T.NUMEROTRANSACAO,
		T.TOTAL
	FROM TRANSACAO t 
	WHERE
		T.TIPOTRANSACAO = 'VEN'
		AND T.COD_NATUREZAOPERACAO IN ('5.102', '5.922', '5910-1')
		AND T.DATAEMISSAO >= '2023-01-01'
)
SELECT 
	E.NOME AS LOJA,
	T.NUMEROTRANSACAO AS "Nº Venda",
	O.NUMEROORDEMSERVICO AS OS,
	TRIM(P.NOME) AS CLIENTE,
	T.DATAEMISSAO AS "Data_venda",
	CASE
	    WHEN O.OBSERVACAO IS NULL THEN ''
	    ELSE CAST(SUBSTRING(O.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320))
    END AS OBSERV,
    CASE
    	WHEN S.DESCRICAO = '1_CAMPANHA SEM CUPOM DESCONTO' THEN ''
    	ELSE S.DESCRICAO
    END AS DIVULGADOR,
	SUM(OP.VALORORIGINAL * OP.QUANTIDADE) AS TOTAL
FROM ORDEMSERVICOCAIXA o
INNER JOIN ORDEMSERVICOCAIXA_PRODUTO op 
	ON O.COD_ORDEMSERVICOCAIXA = OP.COD_ORDEMSERVICOCAIXA 
INNER JOIN TRANS T
 	ON T.COD_TRANSACAO = O.COD_TRANSACAO AND T.COD_TRANSACAOORIGEM IS NULL
INNER JOIN PESSOA AS P
	ON P.COD_PESSOA = O.COD_CLIENTE 
INNER JOIN PESSOA AS E
	ON E.COD_PESSOA = O.COD_EMPRESA AND E.PESSOAEMPRESA = 'T'
LEFT JOIN SAIDAMOTIVO AS S
	ON O.COD_SAIDAMOTIVO = S.COD_SAIDAMOTIVO
 WHERE
	P.CPF in ({cpf_inf}) 
	AND O.REPARO = 'F'
	AND S.DESCRICAO NOT LIKE '%CORTESIA%'
GROUP BY
	E.NOME,
	T.NUMEROTRANSACAO,
	O.NUMEROORDEMSERVICO,
	TRIM(P.NOME),
	T.DATAEMISSAO,
	CASE
	    WHEN O.OBSERVACAO IS NULL THEN ''
	    ELSE CAST(SUBSTRING(O.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320))
    END,
    CASE
    	WHEN S.DESCRICAO = '1_CAMPANHA SEM CUPOM DESCONTO' THEN ''
    	ELSE S.DESCRICAO
    END

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

