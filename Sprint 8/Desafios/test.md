::: mermaid
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

    subgraph movie["Filmes"]
        get_df[["generate_unified_df()"]]
    end

    subgraph series["Series"]
    end