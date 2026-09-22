const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

class RedisHandler {
  constructor(redisUrl) {
    this.redisUrl = redisUrl;
    this.client = null;
  }

  async connect() {
    this.client = new Redis(this.redisUrl, { lazyConnect: true });
    await this.client.connect();
  }

  async close() {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }

  generateChannelId() {
    return `response_channel:${uuidv4()}`;
  }

  async waitForResponse(channelId, timeoutSeconds) {
    if (!this.client) {
      throw new Error("Redis client is not connected. Call connect() first.");
    }

    // In Redis, a client in subscriber mode cannot execute normal commands.
    // Duplicate the existing connection pool for subscribing.
    const subscriber = this.client.duplicate();
    await subscriber.connect();

    return new Promise((resolve, reject) => {
      let timer = null;

      const cleanup = async () => {
        if (timer) clearTimeout(timer);
        try {
          await subscriber.unsubscribe(channelId);
          await subscriber.quit();
        } catch (_) {
          // Ignore cleanup errors
        }
      };

      timer = setTimeout(async () => {
        await cleanup();
        reject(new Error(`No response received on ${channelId} within ${timeoutSeconds}s`));
      }, timeoutSeconds * 1000);

      subscriber.subscribe(channelId, (err) => {
        if (err) {
          cleanup();
          return reject(err);
        }
      });

      subscriber.on('message', async (channel, message) => {
        if (channel === channelId) {
          await cleanup();
          try {
            resolve(JSON.parse(message));
          } catch (parseError) {
            reject(parseError);
          }
        }
      });
    });
  }

  async publishResponse(channelId, data) {
    if (!this.client) {
      throw new Error("Redis client is not connected.");
    }
    await this.client.publish(channelId, JSON.stringify(data));
  }
}

module.exports = RedisHandler;