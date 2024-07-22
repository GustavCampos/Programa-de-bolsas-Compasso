### Diagramas de Entidade Relacionamento pra banco de dados
::: mermaid
erDiagram
    CAR ||--o{ NAMED-DRIVER : allows
    CAR {
        string registrationNumber PK
        string make
        string model
        string[] parts
    }
    PERSON ||--o{ NAMED-DRIVER : is
    PERSON {
        string driversLicense PK "The license #"
        string(99) firstName "Only 99 characters are allowed"
        string lastName
        string phone UK
        int age
    }
    NAMED-DRIVER {
        string carRegistrationNumber PK, FK
        string driverLicence PK, FK
    }
    MANUFACTURER only one to zero or more CAR : makes
:::

### Podemos mostrar linhas de desenvolvimento
::: mermaid
gitGraph:
    commit "Ashish"
    branch newbranch
    checkout newbranch
    commit id:"1111"
    commit tag:"test"
    checkout main
    commit type: HIGHLIGHT
    commit
    merge newbranch
    commit
    branch b2
    commit
:::

### Diagramas de arquitetura
::: mermaid
graph LR
    subgraph Client
        C[User]
    end

    subgraph Infrastructure
        LB[Load Balancer]
        WS[Web Server]
        AS[Application Server]
        DB[(Database)]
    end

    C --> LB
    LB --> WS
    WS --> AS
    AS --> DB
:::