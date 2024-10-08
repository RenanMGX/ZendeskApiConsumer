import requests
import base64
#from typing import List, Dict
#import json
from time import sleep
import pandas as pd
import os
import multiprocessing
#from datetime import datetime
#import random
#import string
from typing import List
from tratarDados import Tratar

class Consume:
    @property
    def token(self) -> str:
        return self.__token
    
    @property
    def keys(self) -> list:
        try:
            return self.__keys
        except AttributeError:
            return []
        
    @property
    def next_page(self):
        try:
            return self.__next_page
        except AttributeError:
            return None
        

    def __init__(self, *, email:str, token:str) -> None:
        if (not isinstance(email, str)) or (not isinstance(token, str)):
            raise TypeError(f"é aceito apenas Strings, {type(email)=}; {type(token)}")
        
        phrase:bytes = f"{email}/token:{token}".encode("utf-8")
        encode_phrase:str = base64.b64encode(phrase).decode()
        self.__token:str = encode_phrase
        #print(f"{self.token=}")
        
        
    def request_api(self, url:str) -> requests.models.Response:
        if not isinstance(url, str):
            raise TypeError(f"é aceito apenas Strings, {type(url)=}")
        
        headerList:dict = {
            "Authorization" : f"Basic {self.token}",
        }
        
        except_error:str = ""
        for _ in range(60):
            try:
                response:requests.models.Response = requests.request("GET", url, data="", headers=headerList)
                self.__keys:list = list(response.json().keys())
                self.__next_page:str = response.json().get('next_page')
                except_error = ""
                break
            except requests.exceptions.JSONDecodeError as error:
                print(f"{type(error)=}, {error=}, {response.status_code=}, {response.content=}")
                except_error = f"{str(type(error))} -> {str(error)}"                
            except Exception as error:
                except_error = f"{str(type(error))} -> {str(error)}"
                break
            sleep(5)
        
        if except_error != "":
            raise Exception(f"não foi possivel consumir a API motivo:\n{except_error}")            

        if response.status_code == 401:
            raise PermissionError("consulta não autorizada, revise o token e o email")
        elif response.status_code == 403:
            raise PermissionError("sem autorização para essa api")
        elif response.status_code == 404:
            raise Exception(f"Endpoint invalido! {url=}")
        
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"erro ao consumir api, {response.status_code=}; {response.content}")
    
    def request_api_multi(self, queue:multiprocessing.Queue,url:str):
        queue.put(self.request_api(url))
    
    def all_pages(self, url:str) -> list:# -> requests.models.Response:
        first_response:dict = self.request_api(url).json()
        keys:list = list(first_response.keys())

        content:list = first_response.get(keys[0]) #type: ignore
        last_next_page:str = "987456123987456132987465132"
        while self.next_page != None:
            print(self.next_page)
            content += self.request_api(self.next_page).json().get(keys[0])
            
            if last_next_page == self.next_page:
                print("Next page começou a repetir")
                break
            else:
                last_next_page = self.next_page
        
        return content
        
    
    def all_tickets(self, *, url_patter:str, contador:int=0) -> str|dict|list:
        while url_patter.endswith('/'):
            url_patter = url_patter[:-1]
            
        path_temp = "temp_file/"
        if not os.path.exists(path_temp):
            os.makedirs(path_temp)
        
        file_name_temp:str = path_temp + "tickets.json"
        if os.path.exists(file_name_temp):
            os.unlink(file_name_temp)
        content_variable:list = []
        
        while True:
            quant = 6
            threads:List[dict] = [{"fila":multiprocessing.Queue(),"processo": None} for _ in range(quant)]
            for thread in threads:
                list_ids:list = [str((num+1)+contador) for num in range(100)]
            
                url:str = f"{url_patter}/api/v2/tickets/show_many.json?ids="
                
                url_mod:str = url + ','.join(list_ids)
                print(url + list_ids[0] + ", " + list_ids[-1])
                
                thread["processo"] = multiprocessing.Process(target=self.request_api_multi, args=(thread["fila"],url_mod))
                thread["processo"].start()
                
                contador += 100
            
            vazio = False
            for thread in threads:
                temp_fila:multiprocessing.Queue = thread['fila']
                content:dict|list = temp_fila.get().json().get('tickets')# type: ignore
                if not content:
                    vazio = True
                content_variable += content
                
             #final_content += content    
            if (len(content_variable) >= 5000) or (vazio):
                #print("alimentou")
                df = pd.DataFrame(content_variable)
                df = Tratar.start(df)
                df.to_json(file_name_temp, orient='records', lines=True, mode='a')
                content_variable = []  
            
            if not content:
                break
            
            
        

        #final_content:dict = pd.read_json(file_name_temp, orient='records').to_dict()

        #os.unlink(file_name_temp)
        return file_name_temp
            


if __name__ == "__main__":
    pass
    #df = pd.read_excel('test.xlsx')
    #df.to_json('all_tickes_temp.json', orient='records')
    # crd:dict = Credential("API_ZENDESK").load()
    # bot = Consume(email=crd['user'], token=crd['password'])
    # bot.all_tickets(url_patter="https://patrimar.zendesk.com")

    #print(pd.DataFrame(api))
    