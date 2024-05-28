### É possível reutilizar containers?

> A resposta curta é: **Sim!**

Vamos dar uma olhada nas ações realizadas durante o desafio, comandos utilizados podem ser conferidos em [comandos.sh](../comandos.sh).

![exemplo_persistencia.png](../../Evidências/exemplo_persistencia.png)

Como mostrado acima, um dos dois containeres criados durante o processo praticamente não fica em execução. Entretanto, ele ainda fica salvo localmente e podemos iniciá-lo novamente caso surja a necessidade.

A menos que um container tenha sido criado com a tag de autoremove ```--rm``` ou tenha sido diretamente excluído, o container fica salvo no ambiente docker e pode ser reiniciado com o comando ```start```, bastando saber apenas seu nome ou id.