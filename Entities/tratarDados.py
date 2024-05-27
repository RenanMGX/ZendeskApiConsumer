#import os
import pandas as pd

class Tratar:
    @staticmethod
    def start(df:pd.DataFrame) -> pd.DataFrame:
        try:
            jc_diretoria_gerencia_ou_gerencia_de_obra = []
            jc_tipo_demanda_contratos = []
                    
            for row, df_value in df.iterrows():
                #jc_diretoria_gerencia_ou_gerencia_de_obra
                for fields_value in df_value["fields"]:
                    if fields_value["id"] == 11062650498327:
                        jc_diretoria_gerencia_ou_gerencia_de_obra.append(fields_value["value"])
                    elif fields_value["id"] == 11952743536919:
                        jc_tipo_demanda_contratos.append(fields_value["value"])
                                
            df["jc_diretoria_gerencia_ou_gerencia_de_obra"] = jc_diretoria_gerencia_ou_gerencia_de_obra
            df["jc_tipo_demanda_contratos"] = jc_tipo_demanda_contratos
            
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
               
        return df
            #df.to_json((self.path + file_name), orient='records', date_format='iso')

if __name__ == "__main__":
    pass