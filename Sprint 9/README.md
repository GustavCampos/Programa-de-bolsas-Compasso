# Desafio

1. [Modelagem e Processamento para Camada Refined](Desafios/)

# Alterações das Atividades Anteriores

## Sprint 07: getDataFromTMDB

Todos os arquivos da nova função lambda criada podem ser encontrados na pasta [getDataFromTMDBv2](../Sprint%207/Desafios/getDataFromTMDBv2/).

### O que mudou?

A principal motivador para a mudança realizada foram os dados adquiridos nos endpoints [dicover/movie](https://developer.themoviedb.org/reference/discover-movie) e [dicover/tv](https://developer.themoviedb.org/reference/discover-tv). Apesar de permitir encontrar diversas mídias facilmente, esses endpoints oferecem relativamente pouca informação.

![discover_endpoint](Evidências/discover_endpoint.png)

O endpoint que melhor oferece informações sobre as mídias são os endpoints [movie details](https://developer.themoviedb.org/reference/movie-details) e [tv details](https://developer.themoviedb.org/reference/tv-series-details). Apesar de conseguir realizar a consulta de apenas uma mídia por vez, esses endpoints permitem trazer o resultado de outros endpoints juntos, facilitando a busca dos atores participantes de cada mídia. 

```json
"Endpoints utilizados": {
    "Filmes": "https://api.themoviedb.org/3/movie/<id_placeholder>?append_to_response=credits&language=en-US",
    "Series": "https://api.themoviedb.org/3/tv/<id_placeholder>?append_to_response=credits&language=en-US"
}
```

As principais atualizações que a função lambda teve foram no arquivo de configuração ***config.json*** e a função ```generate_json_files``` no arquivo ***request_functions.py***.

![sprint7_config_diff](Evidências/sprint7_config_diff.png)

![old_generate_json_files](Evidências/old_generate_json_files.png) 
![new_generate_json_files](Evidências/new_generate_json_files.png)

## Sprint 08: createTrustedDataTMDB

Todos os arquivos do novo Glue Job criado podem ser encontrados na pasta [createTrustedDataTMDBv2](../Sprint%208/Desafios/createTrustedDataTMDBv2/).

### O que mudou?

A maior parte das alterações se encontra na função ```main``` já que os dados obtidos pelo novo endpoint possuem uma estrutura diferente. Entretano, o fluxo de execução do script continua o mesmo.

```mermaid
---

title: Fluxo de createTrustedDataTMDBv2

---

flowchart LR
    start((Início))
    flow_end((Fim))
    load_params[["load_args()"]]
    ctx[Obtem contexto Glue e sessão Spark]
    job_init[Inicia Glue Job]
    s3_client[Obtem cliente S3]

    start --> load_params
    load_params --> ctx
    ctx --> job_init
    job_init --> s3_client

    subgraph movie["Fluxo executado para os Filmes e depois para as Séries"]
        direction TB

        get_df[["generate_unified_df()"]]
        drop_col[Cria novo DataFrame removendo colunas desnecessárias]
        rename_g[["rename_columns()"]]
        map[Cria novo DataFrame com valores da coluna 'genres' mapeados]
        save["Salva novo DataFrame em Parquet particionado por 'ingestion_date'"]

        get_df --> drop_col
        drop_col --> rename_g
        rename_g --> map
        map --> save
    end

    s3_client --> movie
    movie --> flow_end
```

# Evidências

* Logs de execução do AWS Glue Job ```createRefinedData```:

![job_log_1](Evidências/job_log_1.png)
![job_log_2](Evidências/job_log_2.png)

* Logs de execução do AWS Glue Crawler ```createRefinedDataCrawler```:

![crawler_log](Evidências/crawler_log.png)

* Estrutura de diretórios do S3 para os arquivos temporários de teste durante desenvolvimento dos Jobs do Glue.

![test_subsets_tree](Evidências/test_subsets_tree.png)
![test_results_tree](Evidências/test_results_tree.png)

* Estrutura de diretórios do S3 para os arquivos da camada ***Trusted***.

![trusted_tree](Evidências/trusted_tree.png)

* Estrutura de diretórios do S3 para os arquivos da camada ***Refined***.

![refined_tree](Evidências/refined_tree.png)