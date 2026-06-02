const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = 'pedidos_exchange';
const QUEUE_NAME = 'fila_cozinha';

function log(message) {
  const timestamp = new Date().toLocaleTimeString();
  const formatted = `[Cozinha - ${timestamp}] ${message}`;
  console.log(formatted);
  if (process.send) {
    process.send({ type: 'log', message: formatted });
  }
}

async function start() {
  let connection;
  try {
    log('Iniciando consumidor da Cozinha...');
    connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    
    // Ensure exchange and queue exist, and are bound
    await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');
    
    // Only accept one message at a time
    await channel.prefetch(1);
    
    log('Conectado ao RabbitMQ. Aguardando novos pedidos...');
    
    channel.consume(QUEUE_NAME, async (msg) => {
      if (msg !== null) {
        try {
          const order = JSON.parse(msg.content.toString());
          log(`Recebido pedido #${order.pedido_id} (Mesa ${order.mesa}): "${order.item}". Iniciando preparo...`);
          
          // Simulate preparation (random between 2 and 4 seconds)
          const prepTime = 2000 + Math.random() * 2000;
          await new Promise(resolve => setTimeout(resolve, prepTime));
          
          log(`Concluído preparo do pedido #${order.pedido_id}: "${order.item}"! Pronto para entrega.`);
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
      log('Encerrando consumidor da Cozinha...');
      await channel.close();
      await connection.close();
      process.exit(0);
    });

  } catch (error) {
    log(`Erro crítico no Consumidor da Cozinha: ${error.message}`);
    process.exit(1);
  }
}

start();
