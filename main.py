from Entities.api import Consume
from Entities.crenciais import Credential
from Entities.tratarDados import Tratar
import os
from datetime import datetime
#import json
import traceback
#import sys
import multiprocessing
#import asyncio
import pandas as pd
from typing import List
from shutil import copy2

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
        
    def save(self, *, file_name:str, content:list|dict|str) -> None:
        if isinstance(content, str):
            if not file_name.endswith(".json"):
                file_name += ".json"
            pd.read_json(content, orient='records', lines=True).to_json((self.path + file_name), orient='records', date_format="iso")
            #copy2(content, (self.path + file_name))
            return
            
        if (not isinstance(content, list)) and (not isinstance(content, dict)):
            raise TypeError(f"é aceito apenas List ou Dict, {type(content)=}")
        if not isinstance(file_name, str):
            raise TypeError(f"é aceito apenas Strings, {type(file_name)=}")
        
        if not file_name.endswith(".json"):
            file_name += ".json"
        
        if not content:
            raise ValueError(f"{content=} está vazio")
        
        df:pd.DataFrame = pd.DataFrame(content)
        
                
                
        df.to_json((self.path + file_name), orient='records', date_format="iso")
        
        # with open((self.path + file_name), 'w', encoding='utf-8')as _file:
        #     json.dump(content, _file)
    
    def tratar_tickets(self) -> None:
        file_name:str="tickets.json"
        if os.path.exists(path + file_name):
            df:pd.DataFrame = pd.read_json(path + file_name)
            df = Tratar.start(df)
        df.to_json((self.path + file_name), orient='records', date_format='iso')
    

        
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
            #saving_api_json.tratar_tickets()
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
        
        list_for_execute:List[dict] = []
            
        list_for_execute.append({"file_name" : "users", "url" : f"{url_pattern}/api/v2/users/search.json"})
        list_for_execute.append({"file_name" : "groups", "url" : f"{url_pattern}/api/v2/groups.json"})
        list_for_execute.append({"file_name" : "slas_policies", "url" : f"{url_pattern}/api/v2/slas/policies.json"})
        list_for_execute.append({"file_name" : "ticket_audits", "url" : f"{url_pattern}/api/v2/ticket_audits.json"})
        list_for_execute.append({"file_name" : "ticket_forms", "url" : f"{url_pattern}/api/v2/ticket_forms.json"})
        list_for_execute.append({"file_name" : "ticket_fields", "url" : f"{url_pattern}/api/v2/ticket_fields.json"})
        list_for_execute.append({"file_name" : "requests", "url" : f"{url_pattern}/api/v2/requests.json"})
        list_for_execute.append({"file_name" : "activities", "url" : f"{url_pattern}/api/v2/activities.json"})
        list_for_execute.append({"file_name" : "brands", "url" : f"{url_pattern}/api/v2/brands.json"})
        list_for_execute.append({"file_name" : "custom_statuses", "url" : f"{url_pattern}/api/v2/custom_statuses.json"})
        list_for_execute.append({"file_name" : "ticket_metrics", "url" : f"{url_pattern}/api/v2/ticket_metrics"})
        list_for_execute.append({"file_name" : "incremental_ticket_metric_events", "url" : f"{url_pattern}/api/v2/incremental/ticket_metric_events.json?start_time=1"},  )
        
        
        if datetime.now().strftime("%A") == 'Sunday':
            list_for_execute.append({"file_name" : "organizations", "url" : f"{url_pattern}/api/v2/users/search.json"})

        
        threads:List[multiprocessing.Process] = []
        
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
    
    