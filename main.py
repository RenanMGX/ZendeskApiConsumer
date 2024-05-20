from Entities.api import Consume
from Entities.crenciais import Credential
import os
from datetime import datetime
#import json
import traceback
import sys
import multiprocessing
#import asyncio
import pandas as pd
from typing import List,Dict,Literal

class URLPattern:
    @property
    def url(self) -> str:
        return self.__url
    
    def __init__(self, url:str) -> None:
        url = url.replace('\\', '/')
        while url.endswith('/'):
            url = url[:-1]
        self.__url:str = url

class Register:
    @property
    def file_path(self):
        return self.__file_path
    
    def __init__(self, file_name:str) -> None:
        if not isinstance(file_name, str):
            raise TypeError(f"é aceito apenas Strings, {type(file_name)=}")
        if not file_name.endswith(".csv"):
            file_name += ".csv"
        
        path:str = "logs/"
        if not os.path.exists(path):
            os.makedirs(path)
        
        self.__file_path = path + file_name
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding="utf-8")as _file:
                _file.write("data;hora;status;descri\n")
            print(f"Arquivo {file_name=} foi criado!")
    
    def save(self, *, status:str, descri:str):
        if (not isinstance(status, str)) or (not isinstance(descri, str)):
            raise TypeError(f"é aceito apenas Strings, {type(status)=}; {type(descri)}")
        
        date:datetime = datetime.now()
        dateSTR:str = date.strftime("%d/%m/%Y")
        hourSTR:str = date.strftime("%H:%M:%S")
        
        status = status.replace(";", " <br> ").replace(",", " <br> ")
        descri = descri.replace(";", " <br> ").replace(",", " <br> ")
        
        with open(self.file_path, 'a', encoding="utf-8")as _file:
            _file.write(f"{dateSTR};{hourSTR};{str(status)};{str(descri)}".replace("\n","") + "\n")
        print(f"{dateSTR} - Log Registrado com {status=}; {descri=}")
        
class SaveJson:
    @property
    def path(self):
        return self.__path
    
    def __init__(self, api_json_file_path:str) -> None:
        if not isinstance(api_json_file_path, str):
            raise TypeError(f"é aceito apenas Strings, {type(api_json_file_path)=}")
        if "\\" in api_json_file_path:
            if not api_json_file_path.endswith("\\"):
                api_json_file_path += "\\"
        elif '/' in api_json_file_path:
            if not api_json_file_path.endswith('/'):
                api_json_file_path += '/'
                
        if os.path.exists(api_json_file_path):
            self.__path:str = api_json_file_path
        else:
            raise FileNotFoundError(f"Caminho não encontrado {api_json_file_path=}")
        
    def save(self, *, file_name:str, content:list|dict) -> None:
        if (not isinstance(content, list)) and (not isinstance(content, dict)):
            raise TypeError(f"é aceito apenas List ou Dict, {type(content)=}")
        if not isinstance(file_name, str):
            raise TypeError(f"é aceito apenas Strings, {type(file_name)=}")
        
        if not file_name.endswith(".json"):
            file_name += ".json"
        
        if not content:
            raise ValueError(f"{content=} está vazio")
        
        df:pd.DataFrame = pd.DataFrame(content)
        
                
                
        df.to_json((self.path + file_name), orient='records')
        
        # with open((self.path + file_name), 'w', encoding='utf-8')as _file:
        #     json.dump(content, _file)
    
    def tratar_tickets(self) -> None:
        file_name:str="tickets.json"
        if os.path.exists(self.path + file_name):
            df:pd.DataFrame = pd.read_json(self.path + file_name)
            
            try:
                jc_diretoria_gerencia_ou_gerencia_de_obra = []
                    
                for row, df_value in df.iterrows():
                    #jc_diretoria_gerencia_ou_gerencia_de_obra
                    for fields_value in df_value["fields"]:
                        if fields_value["id"] == 11062650498327:
                            jc_diretoria_gerencia_ou_gerencia_de_obra.append(fields_value["value"])
                                
                df["jc_diretoria_gerencia_ou_gerencia_de_obra"] = jc_diretoria_gerencia_ou_gerencia_de_obra
            except:
                pass
            
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['created_at'] = df['created_at'].apply(lambda x: x.isoformat())
            
            df['updated_at'] = pd.to_datetime(df['updated_at'])
            df['updated_at'] = df['updated_at'].apply(lambda x: x.isoformat())
            
            nomeclatura = {
                '' : 'Não Identificado',
                'gerência_de_incorporação' : 'Incorporação',
                'gerência_de_novos_negócios_mg' : 'Novos Neg. MG',
                'gerência_de_controladoria' : 'Controladoria',
                'diretoria_de_produtos' : 'Diretoria de PRodutos',
                'gerência_de_novos_negócios_rj' : 'Novos Neg. RJ',
                'gerência_de_marketing' : 'Marketing',
                'assessoria_de_novo_negócios' : 'Assessoria de Novos Negócios',
                'gerência_administrativa' : 'Administrativo',
                'gerência_financeira' : 'Financeiro',
                'gerência_de_qualidade_e_sustentabilidade' : 'Qualidade e Sustentabilidade',
                'gerência_jurídica_e_de_compliance' : 'Jurídico e Compliance',
                'gerência_de_novos_negócios_sp' : 'Novos Neg. SP',
                'gerência_de_tecnologia_e_inovação' : 'TI e Transformação Digital',
                'gerência__de_controle_de_obras' : 'Controle Obras',
                'gerência_de_planejamento_e_controle_novolar' : 'Planej. Novolar',
                'gerência_de_auditoria_e_risco' : 'Auditoria e Risco',
                'gerência_de_contratos' : 'Contratos',
                'diretoria_administrativa_financeira_e_relações_com_investidores' : 'Diretoria Administrativa,Financeira e R.I.',
                'gerência_de_crédito_imobiliário' : 'Crédito Imob.',
                'diretoria_regional_rj' : 'Diretoria Regional RJ',
                'gerência_de_segurança_e_saúde_do_trabalho' : 'Seg. Trabalho',
                'presidência' : 'Presidência',
                'diretoria_de_desenvolvimento_imobiliario' : 'Diretoria Desenvolvimento Imobiliario',
                'diretoria_comercial_2' : 'Diretoria Comercial',
                'conselho_de_administração' : 'Conselho Adm.',
                'gerência_de_planejamento_financeiro_e_ri' : 'R.I.',
                'construtora_real' : 'Const. Real',
                'novos_neg._rj_255' : 'Novos Neg. RJ',
                'gerência_de_obras_patrimar_rj' : 'Obras Patrimar RJ',
                'assuntos_acionistas/grupo_patrimar' : 'Acionistas Grupo Patrimar',
                'desenvolvimento_humano_e_organizacional' : 'DHO',
                'diretoria_geral_novolar' : 'Diretoria Geral Novolar',
                'diretoria_de_obras_patrimar' : 'Diretoria Obras Patrimar',
                'diretoria_de_obras_novolar' : 'Diretoria Obras Novolar',
                'gerência_de_relacionamento' : 'Relacionamento',
                'gerência_de_obras_novolar_rj' : 'Obras Novolar RJ',
                'gerência__de_suprimentos' : 'Suprimentos',
                'diretoria_adjunta_de_novos_negócios_sp' : 'Diretoria Adjunta de Novos Negócios SP',
                'gerência_de_projetos_novolar' : 'Planej. Novolar',
                'gerência_de_produtos_pcva' : 'Produtos PCVA',
                'gerência_de_produtos_' : 'Produtos',
                'gerência_de_obras_patrimar_mg' : 'Obras Patrimar MG',
                'gerência_de_obras_novolar_mg' : 'Obras Novolar MG',
                'gerência_executiva_de_gestão_de_pessoas' : 'DHO',
                'gerência_de_assistência_técnica_patrimar' : 'SAT Patrimar',
                'gerência_de_projetos_executivos' : 'Projetos Executivos',
                'gerência_de_projetos_executivos_novolar' : 'Projetos Executivos Novolar',
                'gerência_comercial_novolar' : 'Comercial Novolar',
                'diretoria_técnica' : 'Diretoria Técnica',
                'diretoria_comecial_adjunta_rj' : 'Diretoria Comercial Adjunta RJ',
                'gerência_administrativa_55' : 'Administrativo',
                'gerência_de_segurança_e_saúde_do_trabalho_55' : 'Seg. Trabalho',
                'gerência_de_marketing_55' : 'Marketing',
                'gerência__de_controle_de_obras_55' : 'Controle Obras',
                'gerência_de_novos_negócios_sp_55' : 'Novos Neg. SP',
                'gerência_jurídica_e_de_compliance_55' : 'Jurídico e Compliance',
                'desenvolvimento_humano_e_organizacional_55' : 'DHO',
                'gerência_comercial_novolar_55' : 'Comercial Novolar',
                'gerência_financeira_55' : 'Financeiro',
                'diretoria_de_desenvolvimento_imobiliario_55' : 'Desenvolvimento Imobiliario',
                'gerência_de_novos_negócios_rj_55' : 'Novos Neg. RJ',
                'diretoria_de_obras_novolar_55' : 'Diretoria Obras Novolar',
                'gerência_de_relacionamento_55' : 'Relacionamento',
                'gerência_de_auditoria_e_risco_55' : 'Auditoria e Risco',
                'gerência_de_qualidade_e_sustentabilidade_55' : 'Qualidade e Sustentabilidade',
                'diretoria_administrativa_financeira_e_relacionamentos_com_investidores_55' : 'Diretoria Administrativa,Financeira e R.I.',
                'conselho_de_administração_55' : 'Conselho Adm.',
                'gerência_de_contratos_55' : 'Contratos',
                'assessoria_de_novo_negócios_55' : 'Assessoria de Novos Negócios',
                'diretoria_de_obras_patrimar_55' : 'Diretoria Obras Patrimar',
                'gerência_de_obras_patrimar_rj_55' : 'Obras Patrimar RJ',
                'gerência_de_incorporação_55' : 'Incorporação',
                'gerência_de_controladoria_55' : 'Controladoria',
                'diretoria_adjunta_de_novos_negócios_sp_55' : 'Diretoria Adjunta de Novos Negócios SP',
                'gerência_de_crédito_imobiliário_55' : 'Crédito Imob.',
                'novos_neg._mg_255' : 'Novos Neg. MG',
                'controladoria_255' : 'Controladoria',
                'financeiro_255' : 'Financeiro',
                'controle_obras_255' : 'Controle Obras',
                'jurídico_255' : 'Jurídico',
                'qualidade_255' : 'Qualidade e Sustentabilidade',
                'crédito_imob._255' : 'Crédito Imob.',
                'relacionamento_255' : 'Relacionamento',
                'incorporação_255' : 'Incorporação',
                'administrativo_255' : 'Administrativo',
                'obras_novolar_rj_255' : 'Obras Novolar RJ',
                't.i._255' : 'TI e Transformação Digital',
                'dho_255' : 'DHO',
                'produtos_patrimar_255' : 'Produtos Patrimar',
                'obras_patrimar_mg_255' : 'Obras Patrimar MG',
                'auditoria_255' : 'Auditoria',
                'contratos_255' : 'Contratos',
                'projetos_novolar_255' : 'Projetos Novolar',
                'obras_novolar_sp_255' : 'ObrasPatrimar SP',
                'marketing_255' : 'Marketing',
                'conselho_adm._255' : 'Conselho Adm.',
                'novos_neg._sp_255' : 'Novos Neg. SP',
                'obras_patrimar_rj_255' : 'Obras Novolar RJ',
                'produtos_pcva_255' : 'Produtos PCVA',
                'seg._trabalho_255' : 'Seg. Trabalho',
                'suprimentos_255' : 'Suprimentos',
                'obras_novolar_mg_255' : 'Obras Novolar MG',
                'planej._patrimar_255' : 'Planej. Patrimar',
                'sat_patrimar_255' : 'SAT Patrimar',
                'presidência_255' : 'Presidência',
                'inteligência_comercial_255' : 'Inteligência Comercial',
                'ri_255' : 'R.I.',
                'projetos_patrimar_255' : 'Projetos Patrimar',
                'const._real_255' : 'Const. Real',
                'sat_novolar_255' : 'SAT Novolar',
                'comercial_novolar_255' : 'Comercial Novolar'
            }         
            df["jc_diretoria_gerencia_ou_gerencia_de_obra"] = df["jc_diretoria_gerencia_ou_gerencia_de_obra"].map(nomeclatura)
               
            df.to_json((self.path + file_name), orient='records')
    

        
class MultiProcessos:
    @staticmethod
    def execut(register:Register, saving_api_json:SaveJson, api_consume_admin:Consume, file_name:str, url:str):
        try:        
            saving_api_json.save(file_name=file_name, content=api_consume_admin.all_pages(url))
            register.save(status="OK",descri=f"{file_name} are saved!")
        except Exception as error_exception:
            error:str = str(traceback.format_exc()).replace("\n", " <br> ")
            register.save(status="Error",descri=f"{type(error_exception)} -> {error_exception} <br> {error}")

    @staticmethod
    def execut_all_tickets(register:Register, saving_api_json:SaveJson, api_consume_admin:Consume, url_pattern:str, file_name:str="tickets", tickets_inicial:int=0):
        try:
            saving_api_json.save(file_name=file_name, content=api_consume_admin.all_tickets(url_patter=url_pattern, contador=tickets_inicial))
            saving_api_json.tratar_tickets()
            register.save(status="OK",descri=f"{file_name} are saved!")
        except Exception as error_exception:
            error:str = str(traceback.format_exc()).replace("\n", " <br> ")
            register.save(status="Error",descri=f"{type(error_exception)} -> {error_exception} <br> {error}")

        
if __name__ == "__main__":
    try:
        multiprocessing.freeze_support()
        
        url_pattern:str = URLPattern("https://patrimar.zendesk.com/").url
        
        register:Register = Register("logs")
        
        crd:dict = Credential("API_ZENDESK").load()

        try:    
            api_consume_admin:Consume = Consume(email=crd['user'], token=crd['password'])
            saving_api_json = SaveJson("C:\\Users\\renan.oliveira\\PATRIMAR ENGENHARIA S A\\RPA - Documentos\\RPA - Dados\\Zendesk\\API\\json\\")
        except Exception as error:
            error_when_instances:str = str(traceback.format_exc()).replace("\n", " <br> ")
            print(traceback.format_exc())
            register.save(status="Error",descri=error_when_instances)
            raise error
        
        
        agora = datetime.now()
        
        
        thread_alltickets = multiprocessing.Process(target=MultiProcessos.execut_all_tickets, args=(register, saving_api_json, api_consume_admin, url_pattern, "tickets"))
        thread_alltickets.start()
        
        list_for_execute:List[dict] = [
            {"file_name" : "users", "url" : f"{url_pattern}/api/v2/users/search.json"},
            {"file_name" : "groups", "url" : f"{url_pattern}/api/v2/groups.json"},
            {"file_name" : "organizations", "url" : f"{url_pattern}/api/v2/users/search.json"},
            {"file_name" : "slas_policies", "url" : f"{url_pattern}/api/v2/slas/policies.json"},
            {"file_name" : "ticket_audits", "url" : f"{url_pattern}/api/v2/ticket_audits.json"},
            {"file_name" : "ticket_forms", "url" : f"{url_pattern}/api/v2/ticket_forms.json"},
            {"file_name" : "ticket_fields", "url" : f"{url_pattern}/api/v2/ticket_fields.json"},
            {"file_name" : "requests", "url" : f"{url_pattern}/api/v2/requests.json"},
            {"file_name" : "activities", "url" : f"{url_pattern}/api/v2/activities.json"},
            {"file_name" : "brands", "url" : f"{url_pattern}/api/v2/brands.json"},
            {"file_name" : "custom_statuses", "url" : f"{url_pattern}/api/v2/custom_statuses.json"},
            {"file_name" : "ticket_metrics", "url" : f"{url_pattern}/api/v2/ticket_metrics"},
            {"file_name" : "incremental_ticket_metric_events", "url" : f"{url_pattern}/api/v2/incremental/ticket_metric_events.json?start_time=1"},   
        ]
        
        threads:list[multiprocessing.Process] = []
        
        for request in list_for_execute:
            threads.append(multiprocessing.Process(target=MultiProcessos.execut, args=(register, saving_api_json, api_consume_admin, request["file_name"], request["url"])))
            
        for process in threads:
            process.start()
        
        for process in threads:
            process.join()
        
        
        thread_alltickets.join()
        print(datetime.now() - agora)
    
    except Exception as error:
        path:str = "logs/"
        if not os.path.exists(path):
            os.makedirs(path)
        file_name = path + f"LogError_{datetime.now().strftime('%d%m%Y%H%M%Y')}.txt"
        with open(file_name, 'w', encoding='utf-8')as _file:
            _file.write(traceback.format_exc())
        raise error
    
    