# Restaurante Digital - Mensageria com RabbitMQ

Este é um sistema de mensageria para gerenciar pedidos de um restaurante digital de forma desacoplada, utilizando **RabbitMQ** e **Node.js**.

O sistema conta com um produtor (app do cliente) que envia os pedidos para uma *Fanout Exchange*, a qual distribui o pedido simultaneamente para duas filas:
1. `fila_cozinha`: Consumida pelo app da Cozinha para preparar o prato.
2. `fila_pagamento`: Consumida pelo app do Financeiro para realizar a cobrança.

Também há uma **Interface Web** moderna para simular os pedidos, ligar/desligar os consumidores e visualizar os logs de processamento em tempo real.

---

## Pre-requisitos

Certifique-se de ter instalado em sua máquina:
- **Docker** e **Docker Compose**
- **Node.js** (v18 ou superior) e **npm**

---

## Como Executar o Projeto

1. **Suba o RabbitMQ no Docker:**
   ```bash
   docker compose up -d
   ```
   *O painel do RabbitMQ estará disponível em: http://localhost:15672 (login: `guest` / senha: `guest`)*

2. **Instale as dependências do Node.js:**
   ```bash
   npm install
   ```

3. **Inicie o servidor do painel:**
   ```bash
   npm start
   ```

4. **Acesse a interface no navegador:**
   Abra o endereço: [http://localhost:3000](http://localhost:3000)

---

## Como Executar o Teste de Desacoplamento

Para provar que o sistema é totalmente desacoplado:

1. **Desligue os Consumidores**: No painel (http://localhost:3000), clique nos botões **Desligar Consumidor** da Cozinha e do Financeiro. O status de ambos mudará para *Desconectado / Inativo*.
2. **Envie Mensagens**: Clique no botão **Enviar 5 Pedidos Aleatórios**.
3. **Verifique o Acúmulo**:
   - Note que o contador **Na Fila** de ambos os consumidores subirá para `5`.
   - Acesse o painel do RabbitMQ (http://localhost:15672/#/queues) e veja as filas `fila_cozinha` e `fila_pagamento` com as 5 mensagens acumuladas (estado *Ready*).
4. **Processe em Sequência**: Clique em **Ligar Consumidor** para a Cozinha e o Financeiro. Você verá os logs das mensagens sendo processados um por um em sequência no terminal da tela, e o contador de filas voltará para `0`.
