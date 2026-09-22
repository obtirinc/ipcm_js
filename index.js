const RabbitHandler = require('./mqHandler');
const RedisHandler = require('./cachePubSub');

class RemoteServiceError extends Error {
  constructor(message) {
    super(message);
    this.name = 'RemoteServiceError';
  }
}

class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TimeoutError';
  }
}

class IPCMessenger {
  constructor(rabbitmqUrl, redisUrl) {
    this.mq = new RabbitHandler(rabbitmqUrl);
    this.redis = new RedisHandler(redisUrl);
  }

  async connect() {
    await Promise.all([this.mq.connect(), this.redis.connect()]);
  }

  async close() {
    await Promise.all([this.mq.close(), this.redis.close()]);
  }

  async sendRequest(queueName, payload, timeout = 10) {
    const channelId = this.redis.generateChannelId();

    await this.mq.publishRequest(queueName, payload, channelId);

    try {
      const response = await this.redis.waitForResponse(channelId, timeout);

      if (response.status === 'error') {
        throw new RemoteServiceError(response.message);
      }
      return response.data;
    } catch (err) {
      if (err.message && err.message.includes('within')) {
        throw new TimeoutError(`Request to queue '${queueName}' timed out.`);
      }
      throw err;
    }
  }

  async startListening(queueName, processCallback) {
    const onMessage = async (msg, channel) => {
      let payload;
      let respChannel = null;

      try {
        const body = JSON.parse(msg.content.toString());
        respChannel = body.response_channel;

        const result = await processCallback(body.data);
        payload = { status: 'success', data: result };
      } catch (e) {
        payload = { status: 'error', message: `${e.name || 'Error'}: ${e.message}` };
      }

      if (respChannel) {
        await this.redis.publishResponse(respChannel, payload);
      }

      // Acknowledge the message once processing and publishing complete
      channel.ack(msg);
    };

    await this.mq.startConsumer(queueName, onMessage);
  }
}

module.exports = {
  IPCMessenger,
  RemoteServiceError,
  TimeoutError
};