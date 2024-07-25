# Etapas

## Contextualização do Desafio Final:

* Como a Squad 1 deve abordar filmes/séries de comédia ou animação, tenho como idéia inicial verificar se o gênero de animação possuía algum tipo de estigma e caso verdadeiro, como ele evoluiu ao longo do tempo.
* Pretendo verificar quais genêros que mais aparecem em conjunto com animação em cada década. 
* Pretendo também verificar se os filmes mais conhecidos dos atores vindo dos dados do IMDB possuem o gênero animação.

## Processamento dos Dados para Camada Trusted:

* Script para Dados Locais (IMDB): [createTrustedDataLocal.py](createTrustedDataLocal.py)
* Script para Dados do TMDB: [createTrustedDataTMDB.py](createTrustedDataTMDB.py)
---

# Fluxos de Execução

## Funções Gerais
Aqui vamos explorar o fluxo de execução e a entrada/saída de algumas funções que aparecem em ambos os scripts feitos.

### Funções de manipulação de URIs do S3
```python
def uri_to_bucket_name(uri: str) -> str:
    # s3://gustavcampos/2024/07/01/movies.csv -> gustavcampos

def uri_to_s3_key(uri: str) -> str:
    # s3://gustavcampos/2024/07/01/movies.csv -> 2024/07/01/movies.csv

def s3_key_to_uri(obj_key: str, bucket_name: str) -> str:
    # 2024/07/01/movies.csv, gustavcampos -> s3://gustavcampos/2024/07/01/movies.csv

def s3_key_to_date(obj_key: str) -> str:
    # s3://gustavcampos/2024/07/01/movies.csv -> 2024-07-01
```

### Funções Usando PySpark

### Funções Para o Glue Job

#### *load_args*

```python
def load_args(arg_list: list=None, file_path: str=None) -> dict:
```

Função criada para verificar onde o Job está sendo rodado (AWS ou Local) e adquirir os parâmetros necessários de acordo com o ambiente. 
- **Parâmetros**
	- *arg_list*: ***list[str]*** onde cada valor indica um parâmetro definido para o Job.
	- *file_path*: ***str*** que indica o caminho relativo ao script para um arquivo JSON com os parâmetros definidos para o Job.
- **Retorno**: ***dict*** com chave:valor dos parâmetros lidos.

```mermaid
flowchart LR
    script_local["Encontra caminho absoluto do script"]
    open_file[Tenta abrir arquivo de parâmetros]
    file_exists{Arquivo local existe?}
    read_json[Lê o conteudo do arquivo]


    subgraph Catch
        error[FileNotFoundError] --> call[["getResolvedOptions()"]]
    end
    
    script_local --> open_file
    open_file --> file_exists
    file_exists -- Sim --> read_json
    file_exists -- Não --> Catch
    
    read_json --> return_dict[Retorna dados em Dict]
    Catch --> return_dict
```

#### *generate_unified_df*

```python
def generate_unified_df(glue_context: GlueContext, s3_client: boto3.client, s3_path: str,  
                        file_format: str, format_options: dict) -> DataFrame:
```

Gera um DataFrame a partir de múltiplos arquivos dentro de uma pasta especificada no S3.
- **Parâmetros**
	- *glue_context*: ***GlueContext*** criado para Job.
	- *s3_client*: Um ***boto3.client*** conectado ao s3.
	- *s3_path*: URI do bucket/pasta que deseja procurar os arquivos.
	- *file_format*: Parâmetro ***format*** de ***GlueContext.create_dynamic_frame.from_options***.
	- *format_options*: Parâmetro ***format_options*** de ***GlueContext.create_dynamic_frame.from_options***.
- **Retorno**: ***pyspark.sql.DataFrame*** com valores de todos os objetos encontrados.

```mermaid
flowchart TD
    start((Início))
    list_obj[Obtem caminho de todos os objetos em 's3_path']
    filter_obj[\Extensão do objeto == 'format'/]
    i_unified_df[Inicializa 'unified_df' como None]

    start --> list_obj
    list_obj --> filter_obj
    filter_obj --> i_unified_df

    subgraph Loop
        obj_loop{{Para cada 'Key' dos objetos filtrados}}
        key_to_date[["s3_key_to_date()"]]
        import_dyf[Importa objeto como Dynamic Frame]
        convert_dyf[Converte DynamicFrame para DataFrame]
        add_date[Cria novo DataFrame com data encontrada numa coluna adicional]
        is_none{'unified_df' é None?}
        attr[Atribui novo DataFrame a 'unified_df']
        unify[Unifica novo Dataframe com 'unified_df']

        obj_loop --> import_dyf
        import_dyf --> convert_dyf
        convert_dyf --> key_to_date
        key_to_date --> add_date
        add_date --> is_none
        is_none -->|Sim| attr
        is_none -->|Não| unify
        attr --> obj_loop
        unify --> obj_loop
    end

    return[/unified_df/]
    flow_end((Fim))

    i_unified_df --> Loop
    Loop --> return
    return --> flow_end
```

## Fluxo Glue Job createTrustedDataLocal

#### *map_columns_df*

```python
def map_columns_df(spark_df: DataFrame, mapping: list,  null_symbol: str="None") -> DataFrame:
```

Mapeia multiplas colunas de um DataFrame a partir de uma lista de mapeamento
- **Parâmetros**
	- *spark_df*: ***pyspark.sql.DataFrame*** a ser mapeado.
	- *mapping*: ***list*** onde cada item é uma tupla ***(<nome coluna>, <nome coluna desejado>, <tipo desejado>)***.
	- *null_symbol*: ***str*** valor a ser reconhecido como nulo.
- **Retorno**: novo ***pyspark.sql.DataFrame*** com colunas e valores mapeados. 

```mermaid
flowchart TD
    start((Início))

    i_evaluate_null[Inicializa UDF 'evaluate_null']
    i_return_df[Inicializa 'return_df' como 'spark_df']

    start --> i_evaluate_null
    i_evaluate_null --> i_return_df
    i_return_df --> sb_column


    subgraph sb_column[" "]
        column_loop{{Para cada coluna mapeada}}
        col_is_array{col_type é ArrayType?}
        evaluate_null[Cria nova coluna com evaluate_null]
        explode_values[Divide valores da nova coluna]
        cast_type[Converte nova coluna para col_type]
        drop_col[Remove coluna original]
        update_return[Atualiza return_df]

        column_loop --> evaluate_null
        evaluate_null --> col_is_array
        col_is_array -->|Sim| explode_values
        col_is_array -->|Não| cast_type
        explode_values --> cast_type
        cast_type --> drop_col
        drop_col --> update_return
        update_return --> column_loop
    end

    return[/Retorna return_df/]
    flow_end((Fim))

    sb_column --> return
    return --> flow_end
```

### Fluxo Principal
Fluxo que ocorre na função ***main()***:

```mermaid
flowchart TB
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
        map_genre[Cria novo DataFrame com valores de 'generoArtista' mapeados]
        map[["map_columns_df()"]]
        save["Salva novo DataFrame em Parquet particionado por 'ingestion_date'"]

        get_df --> map_genre
        map_genre --> map
        map --> save
    end

    s3_client --> movie
    movie --> flow_end
```


## Fluxo Glue Job createTrustedDataTMDB

---

# Passos para reexecução do desafio

## Criando Função Lambda

Dentro do console da AWS devemos fazer os seguintes passos:

* Acesse página de funções do AWS Lambda;
* Selecione criar função:
    * Escolha a opção **"author from scratch"**;
    * Coloque o nome da função como **getDataFromTMDB**;
    * Escolha **Python 3.11** como runtime;
    * Vá ao final da página e selecione criar função.
* Após a criação da função você devera ser redirecionado para a página da mesma, caso contrário acesse a função criada;
* Desça até a seção de código fonte (**code source**);
* Envie os arquivos de [lambda.zip](lambda.zip) na opção da parte superior direita (**Upload From**);
* Ao final você deverá ter um ambiente dev parecido com o seguinte:
![lambda_dev_env](../Evidências/lambda_dev_env.png)

## Configurando Variáveis, Permissões e Camadas

### Configurando acesso ao S3
