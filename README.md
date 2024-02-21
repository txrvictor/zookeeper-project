# Distributed Systems
---
**EN**:
Final Project for the Distributed Systems course offered by Universidade Federal do ABC (UFABC)

**PT**:
Projeto final de Sistemas Distribuídos.

Aplicação *ZooKeeper* simples para a consolidação de conceitos vistos em laboratório.

  > Barriers;

  > Queues;

  > Locks;

  > Leader Election.



### Breve Explicação

A aplicação proposta foi feita de forma simplificada e visa a criação de *znodes* com IDs únicos que durem por um tempo limitado. A ideia pode ser utilizada tanto criar uma chave/*token* única que expira em certa data, ou mesmo para criar URLs de serviços que devem possuir um tempo de vida limitado. Para tal, foi criado um projeto em *Java* que pode ser executado via linha de comando ou execução do arquivo *.bat* encontrado no repositório.
O *znode* criado que simula uma chave ou *token* contém uma data de expiração como *metadado*, sendo sua expiração comparada com a data atual. A execução do cliente continua até que o prazo expire e então encerra a aplicação. É possível vários clientes executarem requisições ao servidor para criar a chave única simultaneamente.


### Algoritmo Simplificado

  - Eleger um *Leader* se não houver;
  - Executar uma operação de *lock*;
  - Criar *znode* com chave única;
  - Adicionar barreira até expirar a data;
  - Remover barreira e *znode* expirado;
  - Encerrar aplicação.


### Execução

Executar um servidor *ZooKeeper* devidamente configurado, podendo haver um *ensemble* de servidores para testar a funcionalidade de *Leader Election*. Após garantir que o servidor estiver disponível, pode-se executar o arquivo **compile_and_run.bat** ou executar o seguinte código no terminal de comando (dentro da pasta do projeto):

```sh
$ cd <path_da_pasta_projeto>
$ compile_and_run.bat
```
