const amqp = require('amqplib');

class RabbitHandler {
  constructor(rabbitmqUrl) {
    this.rabbitmqUrl = rabbitmqUrl;
    this.connection = null;
    this.channel = null;
  }

  async connect() {
    this.connection = await amqp.connect(this.rabbitmqUrl);
    this.channel = await this.connection.createChannel();
  }

  async close() {
    if (this.channel) {
      await this.channel.close();
      this.channel = null;
    }
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
  }

  async publishRequest(queueName, payload, responseChannel) {
    if (!this.channel) {
      throw new Error("RabbitMQ channel is not open.");
    }

    await this.channel.assertQueue(queueName, { durable: true });
    
    const messageBody = Buffer.from(
      JSON.stringify({
        response_channel: responseChannel,
        data: payload
      })
    );

    this.channel.sendToQueue(queueName, messageBody, { persistent: true });
  }

  async startConsumer(queueName, onMessageCallback) {
    if (!this.channel) {
      throw new Error("RabbitMQ channel is not open.");
    }

    await this.channel.prefetch(1);
    await this.channel.assertQueue(queueName, { durable: true });

    await this.channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        await onMessageCallback(msg, this.channel);
      }
    });
  }
}

module.exports = RabbitHandler;