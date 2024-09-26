import fdb
import pandas as pd 
import warnings
from decouple import config 
warnings.filterwarnings('ignore')
import mysql.connector
import traceback
from mysql.connector import Error

query = """
  SELECT 
    e.NOME AS LOJA,
    t.NUMEROTRANSACAO AS "Nº Venda",
    o.COD_ORDEMSERVICOCAIXA AS OS,
    trim(p.NOME) AS Cliente,
    t.DATAEMISSAO AS "Data_venda",
    CASE 
      WHEN o.OBSERVACAO  IS NULL THEN ''
      ELSE CAST(SUBSTRING(o.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320)) END AS OBSERV,
      CASE 
      WHEN s2.descricao = '1_CAMPANHA SEM CUPOM DESCONTO' THEN ' '
      ELSE s2.descricao 
    END AS DIVULDGADOR,
    SUM(
      CASE
        WHEN s2.descricao = 'EXAGERADO' THEN OP.VALORUNITARIO 
        ELSE op.VALORORIGINAL
      END
    ) AS TOTAL
  FROM ORDEMSERVICOCAIXA o 
  JOIN TRANSACAO t 
    ON (t.COD_TRANSACAO = o.COD_TRANSACAO AND t.COD_TRANSACAOORIGEM IS NULL)
  JOIN PESSOA e
    ON (e.COD_PESSOA = t.COD_EMPRESA)
  JOIN PESSOA p 
    ON (p.COD_PESSOA = t.COD_PESSOA)
  JOIN  ORDEMSERVICOCAIXA_PRODUTO op  
    ON (op.COD_ORDEMSERVICOCAIXA  = o.COD_ORDEMSERVICOCAIXA)
  LEFT JOIN SAIDA s 
    ON (s.COD_SAIDA = t.COD_TRANSACAO)
  LEFT JOIN  SAIDAMOTIVO s2 
    ON (s2.COD_SAIDAMOTIVO = s.COD_SAIDAMOTIVO)
  WHERE 
    p.CPF IN ({cpf_inf})
    AND o.REPARO = 'F'
    AND s2.descricao not like '%CORTESIA%'
    AND CAST(SUBSTRING(o.OBSERVACAO FROM 1 FOR 320) AS VARCHAR(320)) NOT LIKE '%CORTESIA%'
  GROUP BY 
    e.NOME,
    t.NUMEROTRANSACAO,
    o.COD_ORDEMSERVICOCAIXA,
    p.NOME,
    t.DATAEMISSAO,
    s2.descricao,
    o.OBSERVACAO
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
    valor "Valor Permuta"
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
        INSERT INTO contratos (id_contrato, nome_influencer, user, data_inclusao, data_inicio, data_final, valor)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
        

             
