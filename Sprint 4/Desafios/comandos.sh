# TODO: Alguns comandos precisam ser executados no diretorio que 
# TODO: este arquivo se encontra!
cd '.\Sprint 4\Desafios\'
# ou
cd Sprint\ 4/Desafios/
# ou até mesmo
cd "Sprint 4/Desafios/"

# Utilizando o docker compose para criar as imagens e containers necessários
docker compose up -d

# Rodando container carguru
docker start -i carguru-container

# Rodando container mascarar-dados
docker exec mascarar-dados-container python script.py args
# Você pode utilizar quantos argumentos quiser após o script.py
# Exemplo:
docker exec mascarar-dados-container python script.py usuario43 senha_super_secreta email cor_favorita
# Também é possivel utilizar um string completa:
docker exec mascarar-dados-container python script.py "Jorge Alberto" "bananas de pijamas"