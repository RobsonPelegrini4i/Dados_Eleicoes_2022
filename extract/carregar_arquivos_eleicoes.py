import os
import sys
from pandas import json_normalize
import json as json
from urllib.request import urlopen


str_cod_eleicao = "544"
str_eleicao = "e000544"
str_tipo_eleicao = "c0001"
str_ano_eleicao = "2022"


# Faz o download do arquivos de cidades
# Para cada cidade, ele faz o donwload do resultado da eleicao
def main():
    # Faz o download do arquivo de cidades e salva os dados no storage
    url = f"https://resultados.tse.jus.br/oficial/ele{str_ano_eleicao}/{str_cod_eleicao}/config/mun-{str_eleicao}-cm.json"
    response = urlopen(url)
    json_str = response.read()
    json_mun = json.loads(json_str)    
    

    # Grava o arquivo de municipios bruto
    path = f"eleicoes_2022\{str_eleicao}\data"
    if not os.path.exists(os.path.join(path, "municipios")): 
        os.makedirs(os.path.join(path, "municipios"))

    with open(os.path.join(path, "municipios", f"mun-{str_eleicao}-cm.json") , "w", encoding='utf-8') as f:
       json.dump(json_mun, f)


    # deixa os estados minusculos
    df = json_normalize(json_mun["abr"], record_path="mu",meta=["cd", 'ds'], record_prefix='abr_')
    df["cd"] = df["cd"].str.lower()

    # Criar a coluna com a url
    df2 =df.assign(url=lambda x: "https://resultados.tse.jus.br/oficial/ele"+ str_ano_eleicao +"/"+ str_cod_eleicao +"/dados/" + x.cd + "/" + x.cd + x.abr_cd + "-"+ str_tipo_eleicao +"-"+ str_eleicao +"-v.json")


    # Grava o dataframe de cidades com a url para chamar o resultado das eleições
    df2.to_csv(path_or_buf=os.path.join(path, "municipios", f"mun-{str_eleicao}-cm.csv"))


    # Salva o arquivo de resultado de cada cidade
    for label, content in df2["url"].items():
        print(f'label: {label}')
        print(f'content: {content}', sep='\n')
        
        # Realizar o download dos arquivos de resultado de todos os municipios
        response = urlopen(content)
        json_str = response.read()
        json_eleicao = json.loads(json_str)   
        
        # Grava o json bruto para cada municipio
        if not os.path.exists(os.path.join(path, "resultado", "presidente")): 
            os.makedirs(os.path.join(path, "resultado", "presidente"))

        with open(os.path.join(path, "resultado", "presidente", content[-28:]), "w") as f:
            json.dump(json_eleicao, f)
        


# Start script
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(err)
        sys.exit(1)  # Retry Job Task by exiting the process
