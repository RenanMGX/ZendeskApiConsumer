from typing import Dict
from getpass import getuser

default:Dict[str, Dict[str,object]] = {
    'credential': {
        'crd': 'API_ZENDESK'
    },
    'log': {
        'hostname': 'Patrimar-RPA',
        'port' : 80,
        'token': ''
    },
    "paths": {
        'sharepoint_path': f"C:\\Users\\{getuser()}\\PATRIMAR ENGENHARIA S A\\RPA - Documentos\\RPA - Dados\\Zendesk\\API\\json\\"
    }
}

