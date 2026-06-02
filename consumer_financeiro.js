const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = 'pedidos_exchange';
const QUEUE_NAME = 'fila_pagamento';

function log(message) {
  const timestamp = new Date().toLocaleTimeString();
  const formatted = `[Financeiro - ${timestamp}] ${message}`;
  console.log(formatted);
  if (process.send) {
    process.send({ type: 'log', message: formatted });
  }
}

async function start() {
  let connection;
  try {
    log('Iniciando consumidor do Financeiro...');
    connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    
    // Ensure exchange and queue exist, and are bound
    await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');
    
    // Only accept one message at a time
    await channel.prefetch(1);
    
    log('Conectado ao RabbitMQ. Aguardando cobranças...');
    
    channel.consume(QUEUE_NAME, async (msg) => {
      if (msg !== null) {
        try {
          const order = JSON.parse(msg.content.toString());
          log(`Recebido pedido #${order.pedido_id} (Mesa ${order.mesa}) para cobrança de: "${order.item}". Processando pagamento...`);
          
          // Simulate payment validation (random between 1.5 and 2.5 seconds)
          const payTime = 1500 + Math.random() * 1000;
          await new Promise(resolve => setTimeout(resolve, payTime));
          
          log(`Pagamento do pedido #${order.pedido_id} processado com sucesso! Valor debitado.`);
          channel.ack(msg);
        } catch (error) {
          log(`Erro ao processar mensagem: ${error.message}`);
          // Nack message, putting it back in the queue
          channel.nack(msg);
        }
      }
    }, { noAck: false });
    
    // Handle graceful shutdown
    process.on('SIGTERM', async () => {
      log('Encerrando consumidor do Financeiro...');
      await channel.close();
      await connection.close();
      process.exit(0);
    });

  } catch (error) {
    log(`Erro crítico no Consumidor do Financeiro: ${error.message}`);
    process.exit(1);
  }
}

start();
