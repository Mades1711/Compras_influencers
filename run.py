import streamlit as st
import query
import pandas as pd
import datetime


css = """
<style>
* {
margin: 0;  
padding: 1.5px;

}
.block-container { 
    display: block;
    justify-content: center;
    padding: 0px;
    }

[data-testid="StyledLinkIconContainer"]{
    font-size: 1.4em; 
    text-align: center;
    padding: 10px;
}

[data-testid="stMetric"] {
    display: grid;
    place-items: center;
}
  
[data-testid="stMetricValue"] {
    font-size: 2.3em; 
}

[data-testid="stMetricDelta"]{
    font-size: 1.35em; 
}

[data-testid="stMetricLabel"]{   
    p{
        font-size: 1.2em; 
    }
}

.main .block-container {
    max-width: 80%;
        
}
[data-testid="stMetricDelta"] svg {
        display: none;
    }


[data-testid="stFullScreenFrame"]{
    display: grid;
    place-items: center;
   }

section[data-testid="stSidebar"] {
    width: 450px !important; # Set the width to your desired value
}
</style>
"""

@st.cache_data(show_spinner="Atualizando os dados...",ttl='120m')
def consulta_contratos():
    return query.contratos()

@st.cache_data(show_spinner="Atualizando os dados...",ttl='120m')
def consulta_compras():
    cpf_inf = query.cpf_inf()
    return query.consulta(cpf_inf)

def apply_css(css):
   st.markdown(css, unsafe_allow_html=True)

def validar_cpf(cpf):
    cpf = ''.join(filter(str.isdigit, cpf))

    if len(cpf) != 11:
        return False

    if cpf == cpf[0] * 11:
        return False

    def calcular_digito(cpf, peso):
        soma = 0
        for i in range(peso):
            soma += int(cpf[i]) * (peso + 1 - i)
        digito = (soma * 10) % 11
        return digito if digito < 10 else 0

    primeiro_digito = calcular_digito(cpf, 9)
    segundo_digito = calcular_digito(cpf, 10)

    return cpf[-2:] == f'{primeiro_digito}{segundo_digito}'

st.set_page_config(
        page_title="Compras Influencers",
        page_icon= "Logo_DI.png",
        initial_sidebar_state='collapsed'
    )

def run():
    apply_css(css) 
    st.title("Relação compras de Influencer")
       
    data = consulta_compras()
    if data is None:
        st.error('No momento nao foi possivel conectar ao banco de dados, favor tente novamente mais tarde')
    else:
        influencer = data['CLIENTE'].unique().tolist()
            
        with st.sidebar:
              
            new_inf = st.text_input('Insira o CPF do Influencer', value= None)
            col1, col2 = st.columns(2)
            incluir_influ = col1.button('Incluir influencer')
            excluir_influ = col2.button('Excluir influencer')

            if incluir_influ :
                if new_inf == None:
                    st.warning('Insira um CPF')
                elif validar_cpf(new_inf) == False:
                    st.warning('CPF INVALDIDO')
                else:   
                    print(new_inf)               
                    insert_cpf = query.insert_cpf(new_inf)
                    if insert_cpf == True:                            
                        st.cache_data.clear()
                        data = consulta_compras()
                        st.warning('Influencer cadastrado')
                        influencer = data['CLIENTE'].unique().tolist()
                        st.rerun()  
                    else:        
                        st.warning(insert_cpf)                  
                
            if excluir_influ:
                if new_inf == None:
                    st.warning('Insira um CPF')
                else: 
                    query.delete_cpf(new_inf) 
                    if query.delete_cpf(new_inf)  == True:                                             
                        st.cache_data.clear()
                        consulta_compras()   
                        st.warning('Influencer excluido')
                        
                        st.rerun()
                    else:
                        st.warning(query.delete_cpf(new_inf) ) 
      
            # INCLUINDO NOVO CONTRATO

            contratos = consulta_contratos()
            contratos_list = contratos['Contrato'].tolist()

            influ = st.selectbox('Selecione os Influencers', influencer, index=None, placeholder='Selecione o influencer')
            number = st.number_input('Insira um valor', value=0, placeholder='Insira um valor...', step = 1000)
            contratos_list_numeros = [int(contrato) for contrato in contratos_list]
            valor_maximo = max(contratos_list_numeros)

            id_contrato = valor_maximo + 1
            user = st.text_input('Insira seu nome', value= None)
            
            first_date = datetime.date(2023, 1, 1)
            las_date = datetime.date(2050, 12, 31)

            d = st.date_input(
                "Selecione o periodo",
                (first_date, datetime.date(2023, 1, 1)),
                first_date,
                las_date,
                format="DD.MM.YYYY",           
            )
            agree = st.checkbox('Conferi os dados') 

            if st.button('Incluir contrato', key= 4):

                if number == None:
                    st.warning('Por favor, insira um valor.')

                elif user == "":
                    st.warning('Por favor, insira seu nome.')

                elif user.isnumeric():
                    st.warning('Por favor, insira seu nome.')

                elif influ == None:
                    st.warning('Por favor, selecione um influencer.')
                
                elif id_contrato in contratos_list:
                    st.warning('Numero de contrato ja utilizado')

                else:
                    if agree:                    
                        data_hora_atual = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') 
                        data_inicial = d[0].strftime('%Y-%m-%d')
                        data_final = d[1].strftime('%Y-%m-%d')                                           
                        perm = (id_contrato, influ, user, data_hora_atual, data_inicial, data_final, round(number,2) )                          
                        insert_data = query.insert_data (perm)  
                        if insert_data == True:
                            st.warning(f"Valor de  {number} para {influ} cadastrado!")
                            consulta_contratos.clear()

                            contratos = consulta_contratos()
                            contratos_list = contratos['Contrato'].tolist()
                           
                        else: 
                            st.warning(insert_data)
                    else:
                        st.warning('Marque a opção "Conferi os dados"')
            
            #DELETAR CONTRATO
            del_contrato = st.selectbox('Selecione um contrato para excluir',contratos_list, index=None, placeholder='Selecione um contrato')
            if del_contrato is not None :
                agree_del = st.checkbox("Deletar contrato: {del_contrato}".format(del_contrato=del_contrato))            
                if agree_del:
                    
                    ct_filtred = contratos[contratos['Contrato']== del_contrato]
                    inf_del = ct_filtred.iloc[0]['Influencer']
                    contrato_del = ct_filtred.iloc[0]['Contrato']
                    valor_del = ct_filtred.iloc[0]['Valor Permuta']

                    st.warning('Atenção! O contrato {contrato_del} de {inf_del} no valor {valor_del} irá ser deletado'.format(
                        contrato_del=contrato_del,
                        inf_del=inf_del,
                        valor_del=valor_del
                        )
                    )    

            if st.button('Deletar contrato', key= 5):     
                         
                if del_contrato is None: 
                    st.warning('Selecione um contrato')
                elif agree_del == False:                                                          
                    st.warning('Marque a opção para deletar o contrato')
                else:
                    delete_contrato = query.delete_data(del_contrato)
                    if delete_contrato == True:
                        st.warning('Contrato excluído com sucesso')
                        consulta_contratos.clear()
                        contratos = consulta_contratos()
                        contratos_list = contratos['Contrato'].tolist()                        
                    else:
                        st.warning(delete_contrato)

        #VISUALIZAÇÃO TABELAS            
        task = st.selectbox('Influencer', ['Contratos']+['Todos'] + influencer, index=None,placeholder='Selecione o influencer')      

        if data is None:
            st.error('No momento nao foi possivel conectar ao banco de dados, favor tente novamente mais tarde')
            if st.button("Atualizar dados"):
                st.cache_data.clear()
        else:
            
            if task == 'Todos':
                    st.dataframe(
                        data, 
                        hide_index= True,
                        width = 1300,
                        height = 300,
                        column_config = {
                            "Data_venda": st.column_config.DatetimeColumn(
                                "Data Venda",
                                format="DD/MM/YYYY",
                            ),
                            "OBSERV": 'OBSERVAÇÃO'     
                        }
                        
                        )

            elif task in influencer:
                col1, col2, col3 = st.columns(3)
                valid = col1.selectbox('Contratos', ['Todos']+['Contratos ativos'],placeholder='Selecione o influencer')
                credito_permuta = contratos[contratos['Influencer']==task]
                
                data = data[data['CLIENTE']==task]
                if valid == 'Todos':
                    st.dataframe(
                        data, 
                        hide_index= True,
                        width = 1300,
                        height = 300,
                        column_config = {
                            "Data_venda": st.column_config.DatetimeColumn(
                                "Data Venda",
                                format="DD/MM/YYYY",
                            ),
                            "OBSERV": 'OBSERVAÇÃO'
                        }
                        
                        )

                elif  valid == 'Contratos ativos' and credito_permuta.empty == True:
                    st.warning('Sem contratos ativos')

                else:           
                    data['Data_venda'] = pd.to_datetime(data['Data_venda'])
                      
                    hoje =  pd.to_datetime(datetime.datetime.now().date())
                    credito_permuta = credito_permuta[credito_permuta['Data Final']>= hoje]
                    ult_data_contrato = credito_permuta['Data Inicio'].min()
                    
                    data = data[data['Data_venda'] >= ult_data_contrato]
                    total_compra = int(data.groupby('CLIENTE').agg({'TOTAL':'sum'}).values)               
                    credito_ativo = int(credito_permuta.groupby('Influencer').agg({'Valor Permuta':'sum'}).values)

                    col1, col2, col3  = st.columns(3)
                    col1.metric(label="Total comprado",value=total_compra)
                    col2.metric(label="Total credito ativo",value=credito_ativo)
                    col3.metric(label="Credito restante",value=credito_ativo-total_compra)
                   
                    st.dataframe(
                        data, 
                        hide_index= True,
                        width = 1300,
                        height = 300,
                        column_config = {
                            "Data_venda": st.column_config.DatetimeColumn(
                                "Data Venda",
                                format="DD/MM/YYYY",
                            ),
                            "OBSERV": 'OBSERVAÇÃO'
                        }
                        
                        )
                
            elif task == 'Contratos':
                contratos = consulta_contratos()
                st.dataframe(
                    contratos, 
                    hide_index=True,
                    width = 1500,
                    height = 300,
                    column_config = {
                        "Data Inicio": st.column_config.DatetimeColumn(
                            "Data Inicio",
                            format="DD/MM/YYYY",
                        ),
                        "Data Final": st.column_config.DatetimeColumn(
                            "Data Final",
                            format="DD/MM/YYYY",
                        ),
                        "OBSERV": 'OBSERVAÇÃO'
                        }
                    ) 

            else:
                st.write('Selecione um influencer')
                
run()