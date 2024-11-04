import pandas as pd
import streamlit as st
import psycopg2
import base64
from bitrix24 import Bitrix24
import re
from requests import get
import plotly.express as px
from bitrix24 import Bitrix24
import re
from requests import get
import functools

from controller import dbController

controler = dbController('ECF')
st.set_page_config(layout='wide')
background_image ="Untitleddesign.jpg"
st.markdown(
     f"""
     <iframe src="data:image/jpg;base64,{base64.b64encode(open(background_image, 'rb').read()).decode(

    )}" style="width:4000px;height:3000px;position: absolute;top:-3vh;right:-1250px;opacity: 0.5;background-size: cover;background-position: center;"></iframe>
     """,
     unsafe_allow_html=True )

class RegistroContatoClientes():


    @functools.lru_cache()
    def get_data(self):
    
        url = 'postgresql+psycopg2://postgres:djgr27041965@localhost:5432/db_compensacao'

        bx24 = Bitrix24(f'{st.secrets['Bitrix']["api_con"]}')

       
        data = bx24.callMethod('crm.company.list', select=['ID', 'TITLE', 'UF_CRM_1634750723', 'UF_CRM_63580C7CDFB5C', 'UF_CRM_630FC29A707BC', 'UF_CRM_1691072587'], filter={'UF_CRM_1727790100': '360938'})
        self.df = pd.DataFrame(data)

        return self.df

    def  clean_data(self):


            self.df['UF_CRM_63580C7CDFB5C'] = self.df['UF_CRM_63580C7CDFB5C'].str.replace('1379','MEI - Micro Empreendedor').str.replace('1381','Simples Nacional').str.replace('1383','Lucro Presumido').str.replace('1385','Lucro Real').str.replace('13225','Inapta').str.replace('174146','N/A').fillna('---')
            self.df.rename(columns={'TITLE': 'nome_cliente', 'UF_CRM_1634750723': 'cod_cnpj', 'UF_CRM_63580C7CDFB5C': 'regime_tributario', 'UF_CRM_630FC29A707BC':'grupo_empresarial', 'UF_CRM_1691072587':'atividades_economicas'}, inplace=True)
            self.df= self.df.dropna()
            self.df['nome_cliente'] = self.df['nome_cliente'].astype(str).str.upper().apply(lambda x: re.sub(r'^\d+\s-\s', '', x) if isinstance(x, str) and re.match(r'^\d+\s-\s', x) else x).str.replace('Ç', 'C').str.replace('Ã', 'A').str.replace('Á', 'A').str.replace('Â', 'A').str.replace('É','E').str.replace('Ô','O').str.replace('Õ','O').str.replace('Í','I')
            self.df['grupo_empresarial'] = self.df['grupo_empresarial'].str.replace('0', 'Não').str.replace('1', 'Sim')
            self.df['atividades_economicas'] = self.df['atividades_economicas'].str. strip()
            self.df = self.df.drop(columns=['ID'])
            self.df = self.df.sort_values(by = 'nome_cliente')
            self.df['cod_cnpj'] = self.df['cod_cnpj'].str.replace('.','').str.replace('/','').str.replace('-','')
            


    def generating_data(self):

        tabelaDeNomes = self.df
        listaDosNomesDasEmpresas = list(tabelaDeNomes['nome_cliente'])
        nome_para_cnpj = dict(zip(tabelaDeNomes['nome_cliente'], tabelaDeNomes['cod_cnpj']))

        dataframesParaDownload = []
        tabelaParaRelatorio = []
        col1, col2 = st.columns(2)

        with col1:
            nomeEmpresaSelecionada = st.selectbox('Selecione a empresa para registro de informações',listaDosNomesDasEmpresas)
            cnpj_selecionado = nome_para_cnpj.get(nomeEmpresaSelecionada, "")
            st.write(f'CNPJ : {cnpj_selecionado}')
            with st.form('formulario',clear_on_submit=False,border=False):

                data = st.date_input('Data')
                data_formatada = data.strftime('%d/%m/%Y')
                horario = st.text_input('Horario do contato')
                motivo = st.selectbox('Motivo:', options=['Cobrança', 'Reclamação', 'Duvidas', 'Solicitação','Retorno'], placeholder='')
                responsavel = st.text_input('Responsavel pelo contato')
                forma_de_contato = st.selectbox('Forma de contato ',options=['Bitrix','Whatsapp','E-mail'])
                observacoes = st.text_area('Observações')

                tabela = {
                    'Data': [data],
                    'Horario': [horario],
                    'Motivo': [motivo],
                    'Responsavel': [responsavel],
                    'Observacoes': [observacoes],
                    'Forma_de_contato':[forma_de_contato],
                    'cnpj': [cnpj_selecionado]
                }
                tabela_df = pd.DataFrame(tabela)

            
                registrar = st.form_submit_button('Registrar informações')
                if registrar:
                    try:
                        controler.registrar_contato_clientes(tabela_df, 'contato_clientes')
                        st.warning('Dados salvos')
                    except Exception as e:
                        st.warning(e)


        with col2:
            # Create a second form for filtering and displaying data
            with st.form('tabela_para_visualizacao'):
                filtro_empresa = st.selectbox('Filtro Empresa', listaDosNomesDasEmpresas)

                # Submit button inside the form
                if st.form_submit_button('Filtrar'):
                    dados_totais_da_tabela = controler.get_all_data('contato_clientes')
                    dados_totais_da_tabela['cnpj'] = dados_totais_da_tabela['cnpj'].astype(str).str[:-2]
                    cnpj_selecionado_filtro = nome_para_cnpj.get(filtro_empresa, "")
                    dados_totais_da_tabela = dados_totais_da_tabela[dados_totais_da_tabela['cnpj'] == cnpj_selecionado_filtro]
                else:
                    dados_totais_da_tabela = controler.get_all_data('contato_clientes')
                dados_totais_da_tabela = dados_totais_da_tabela.iloc[:,1:].set_index('id')
                st.dataframe(dados_totais_da_tabela)

            with st.form('Alterações',border=False):
                id = st.text_input('Coloque o ID')
                if st.form_submit_button('Deletar informação'):
                    controler.deletarDadosDaTabelaPor_Id(id,'contato_clientes')


        
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            motivo_grafico = dados_totais_da_tabela['Motivo'].value_counts()
            fig_motivo = px.pie(
                values=motivo_grafico.values,
                names=motivo_grafico.index,
                title="Distribuição de Motivos de Contato"
            )
            fig_motivo.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",  # Fundo do gráfico
                plot_bgcolor="rgba(0,0,0,0)"    # Fundo da área de plotagem
            )
            st.plotly_chart(fig_motivo)
            
            with col1:
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                forma_grafico = dados_totais_da_tabela['Forma_de_contato'].value_counts()

                # Gráfico de barras
                fig_forma = px.bar(
                    x=forma_grafico.index,
                    y=forma_grafico.values,
                    title="Distribuição das Formas de Contato",
                    labels={'x': 'Forma de Contato', 'y': 'Contagem'}
                )

                # Remover a cor de fundo
                fig_forma.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",  # Fundo do gráfico
                    plot_bgcolor="rgba(0,0,0,0)"    # Fundo da área de plotagem
                )

                st.plotly_chart(fig_forma)



if __name__=='__main__':

    registroClientes = RegistroContatoClientes()
    registroClientes.get_data()
    registroClientes.clean_data()
    registroClientes.generating_data()