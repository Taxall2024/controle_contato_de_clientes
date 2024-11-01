import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy as sa
import streamlit as st
from sqlalchemy.exc import DBAPIError
import time
import functools
from psycopg2 import pool
from sqlalchemy import create_engine
import threading


import psycopg2
from psycopg2 import sql


class dbController():
    
    def __init__(self,banco):
        
        username = st.secrets["apiAWS"]["username"]
        password = st.secrets["apiAWS"]["password"]
        host = st.secrets["apiAWS"]["host"]
        port = st.secrets["apiAWS"]["port"]
        self.engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/jcp', 
                        pool_size=2, max_overflow=1, pool_recycle=5, pool_timeout=10, pool_pre_ping=True, pool_use_lifo=True)                                
        

        self.conn = self.engine.connect()



    def closeCons(self):
 
        self.engine.dispose()
        self.conn.close()

    def registrar_contato_clientes(self,df,tabela):
        df.to_sql(tabela, self.engine, if_exists='append', index=False)


    def inserirTabelas(self, tabela, df):

        try:
            verificacaoCNPJ = df.iloc[0]['CNPJ']
            verificacaoAno = df.iloc[0]['Ano']
            query = text(f"""SELECT * 
                        FROM {tabela} 
                        WHERE \"CNPJ\" = :CNPJ AND \"Ano\" = :ano""")
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {'CNPJ': verificacaoCNPJ, 'Ano': verificacaoAno})
                rows = result.fetchall()
        except:
            pass
                    
        try:    
            verificacaoCNPJ = df.iloc[0]['CNPJ']
            verificacaoAnoTanalise = df.iloc[0]['PeriodoDeAnalise']
            
            query = text(f"""SELECT * 
                            FROM {tabela} 
                            WHERE \"CNPJ\" = :CNPJ AND \"PeriodoDeAnalise\" = :PeriodoDeAnalise""")

            with self.engine.connect() as conn:
                result = conn.execute(query, {'CNPJ': verificacaoCNPJ, 'PeriodoDeAnalise': verificacaoAnoTanalise})
                rows = result.fetchall()
        except:
            pass
            
        try:
            verificacaoCNPJ = df.iloc[0]['CNPJ']
            
            query = text(f"""SELECT * 
                            FROM {tabela} 
                            WHERE \"CNPJ\" = :CNPJ """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {'CNPJ': verificacaoCNPJ})
                rows = result.fetchall()
        except Exception as e:
            st.warning(e)
            pass
            
        if rows:
            st.warning(f"CNPJ {verificacaoCNPJ}  já existe na tabela {tabela}. Não inserindo dados.")
        else:
            df.to_sql(tabela, self.engine, if_exists='append', index=False)  
                

        self.closeCons()


    def get_data_by_cnpj(self, cnpj,tabela):
        query = f"SELECT * FROM {tabela} WHERE \"CNPJ\" = '{cnpj}'"
        with self.engine.connect():
            
            df = pd.read_sql_query(query, self.engine)  
        self.closeCons()
        return df

    @functools.lru_cache
    def get_jcp_value(self, cnpj: str, tabela: str, ano: int, operation: str) -> pd.DataFrame:
        query = f"""
            SELECT "CNPJ", "Ano", "Value","Operation"
            FROM {tabela}
            WHERE "CNPJ" = '{cnpj}' AND "Ano" = '{ano}' AND "Operation" = '{operation}'
        """
        with self.engine.connect():
            
            df = pd.read_sql_query(query, self.engine)
            self.closeCons()
        return df

    @functools.lru_cache
    def get_jcp_value_trimestral(self, cnpj: str, tabela: str, ano: int, operation: str) -> pd.DataFrame:
        query = f"""
            SELECT "CNPJ", "Ano", "Value 1º Trimestre","Value 2º Trimestre","Value 3º Trimestre","Value 4º Trimestre",
            "Operation 1º Trimestre",
            "Operation 2º Trimestre",
            "Operation 3º Trimestre",
            "Operation 4º Trimestre"
            FROM {tabela}
            WHERE "CNPJ" = '{cnpj}' AND "Ano" = '{ano}' 
            AND "Operation 1º Trimestre" = '{operation}' 
            AND "Operation 2º Trimestre" = '{operation}' 
            AND "Operation 3º Trimestre" = '{operation}' 
            AND "Operation 4º Trimestre" = '{operation}'  """
        
        with self.engine.connect():
            
            df = pd.read_sql_query(query, self.engine) 

        self.closeCons()
        return df
    
    def deletarDadosDaTabela(self,tabela):
        query = text("DELETE FROM {}".format(tabela))
        
        self.conn.execute(query)
        print(f'Os valores para tabela {tabela} foram DELETADOS!')
        self.conn.commit()
   
    def deletarDadosDaTabelaPorCnpj(self, cnpj, tabela):
        # Use parameterized query to avoid SQL injection and properly handle data types
        self.engine
        self.conn = self.engine.connect()
        query = text(f"DELETE FROM {tabela} WHERE \"CNPJ\" = :cnpj")
        self.conn.execute(query, {"cnpj": cnpj})
        print(f'Os valores para tabela {tabela} e CNPJ {cnpj} foram DELETADOS!')
        self.conn.commit()
        self.closeCons()


    def deletarDadosDaTabelaPor_Id(self, id, tabela):
        # Use parameterized query to avoid SQL injection and properly handle data types
        self.engine
        self.conn = self.engine.connect()
        query = text(f"DELETE FROM {tabela} WHERE \"id\" = :id")
        self.conn.execute(query, {"id": id})
        print(f'Os valores para tabela {tabela} e CNPJ {id} foram DELETADOS!')
        self.conn.commit()
        self.closeCons()    


    def get_all_data(self,tabela):
        query = f"SELECT * FROM {tabela}"
        
        with self.engine.connect():
                df = pd.read_sql_query(query, self.engine)
        self.closeCons()
        return df

    @functools.lru_cache
    def queryResultadoFinal(self, cnpj,tabela,ano):
        
        query = f""" 
        SELECT "CNPJ", "Ano", "Value","Operation","index"
        FROM {tabela}
        WHERE \"CNPJ\" = '{cnpj}' AND \"Ano\" = '{ano}'"""
        query2 = f""" 
        SELECT "CNPJ", "Ano", "Value","Operation"
        FROM {tabela}
        WHERE \"CNPJ\" = '{cnpj}' AND \"Ano\" = '{ano}'"""
        with self.engine.connect():
            
            try:
                df = pd.read_sql_query(query, self.engine)
            except:
                df = pd.read_sql_query(query2, self.engine)

        self.closeCons()        
        return df

    @functools.lru_cache
    def queryResultadoFinalTrimestral(self, cnpj,tabela,ano):
        query = f"""
        SELECT "CNPJ", "Ano","Operation 1º Trimestre" ,"Value 1º Trimestre","Operation 2º Trimestre" ,"Value 2º Trimestre",
        "Operation 3º Trimestre" ,"Value 3º Trimestre","Operation 4º Trimestre" ,"Value 4º Trimestre","index"
            FROM {tabela}
            WHERE "CNPJ" = '{cnpj}' AND "Ano" = '{ano}' """
        
        query2 = f"""
        SELECT "CNPJ", "Ano","Operation 1º Trimestre" ,"Value 1º Trimestre","Operation 2º Trimestre" ,"Value 2º Trimestre",
        "Operation 3º Trimestre" ,"Value 3º Trimestre","Operation 4º Trimestre" ,"Value 4º Trimestre"
            FROM {tabela}
            WHERE "CNPJ" = '{cnpj}' AND "Ano" = '{ano}' """

        with self.engine.connect(): 
        
            try:
                df = pd.read_sql_query(query, self.engine)
            except:
                df = pd.read_sql_query(query2, self.engine)
        self.closeCons()
        return df
    
    def inserirTabelasFinaisJCP(self, tabela, df):

        self.engine
        self.conn = self.engine.connect()
        verificacaoAno = int(df.iloc[0]['Ano'])
        verificacaoCNPJ = df.iloc[0]['CNPJ']

        query = text(f"SELECT * FROM {tabela} WHERE \"Ano\" = :Ano AND \"CNPJ\" = :CNPJ")
        
        result = self.conn.execute(query, {'Ano': verificacaoAno, 'CNPJ': float(verificacaoCNPJ)})

        rows = result.fetchall()

        if rows:
            st.warning(f"Ano {verificacaoAno} e CNPJ {verificacaoCNPJ} já existem na tabela {tabela}. Não inserindo dados.")
        else:
            with self.engine.connect():
                
                df.to_sql(tabela, self.engine, if_exists='append', index=False)
            
        self.closeCons()

    def update_table(self, tabela, df, cnpj, ano):
        operations = df['Operation'].unique()
        self.engine
        self.conn = self.engine.connect()
        
        # Gerenciador de contexto para a transação
        with self.conn.begin() as transaction:
           
                for operation in operations:
                    value = float(df.loc[df['Operation'] == operation, 'Value'].iloc[0])
                    query = text(f"UPDATE {tabela} SET \"Value\" = :Value WHERE \"CNPJ\" = :CNPJ AND \"Ano\" = :Ano AND \"Operation\" = :Operation")
                    params = {'Value': value, 'CNPJ': cnpj, 'Ano': ano, 'Operation': operation}
                    self.conn.execute(query, params)
                
        self.closeCons()

    def update_table_trimestral(self, tabela, df, cnpj, ano):
        self.engine
        self.conn = self.engine.connect()
        operations = [op for trimestre in [1,2,3,4] for op in df[f'Operation {trimestre}º Trimestre'].unique()]
    # Gerenciador de contexto para a transação
        with self.conn.begin() as transaction:
            
                for trimestre in [1,2,3,4]:
                    for operation in operations:
                        filtered_df = df.loc[df[f'Operation {trimestre}º Trimestre'] == operation]
                        if not filtered_df.empty:
                            value = float(filtered_df[f'Value {trimestre}º Trimestre'].iloc[0])
                            query = text(f"UPDATE {tabela} SET \"Value {trimestre}º Trimestre\" = :Value WHERE \"CNPJ\" = :CNPJ AND \"Ano\" = :Ano AND \"Operation {trimestre}º Trimestre\" = :Operation")
                            params = {'Value': value, 'CNPJ': cnpj, 'Ano': ano, 'Operation': operation}
                            self.conn.execute(query, params)
                        else:
                            print(f'Os valores para {trimestre}º trimestre foram atualizados')

        self.closeCons()




if __name__ =="__main__":

    
    controler = dbController('taxall')
    

    controler.deletarDadosDaTabelaPorCnpj('09302262000185','l100')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','l300')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','m300')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','m350')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','n630')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','n670')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','resultadosjcp')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','resultadosjcptrimestral')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','tipodaanalise')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','cadastrodasempresas')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','lacslalur')
    controler.deletarDadosDaTabelaPorCnpj('09302262000185','lacslalurtrimestral')


    # controler.deletarDadosDaTabela('l100')
    # controler.deletarDadosDaTabela('l300')
    # controler.deletarDadosDaTabela('m300')
    # controler.deletarDadosDaTabela('m350')
    # controler.deletarDadosDaTabela('n630')
    # controler.deletarDadosDaTabela('n670')
    # controler.deletarDadosDaTabela('resultadosjcp')
    # controler.deletarDadosDaTabela('resultadosjcptrimestral')
    # controler.deletarDadosDaTabela('tipodaanalise')
    # controler.deletarDadosDaTabela('cadastrodasempresas')
    # controler.deletarDadosDaTabela('lacslalur')
    # controler.deletarDadosDaTabela('lacslalurtrimestral')


