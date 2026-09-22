# **IPCMessenger**

[![Node.js Version](https://img.shields.io/badge/node.js-%3E%3D16.0.0-green.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A lightweight, high-performance asynchronous Inter-Process Communication (IPC) library for Node.js microservices.

Rather than replacing message brokers, **IPCMessenger structures and standardizes how you use them**. It combines the distinct strengths of **RabbitMQ** and **Redis Pub/Sub** into a single, unified interface that delivers low-latency, point-to-point **Request-Response** messaging without the massive boilerplate overhead.

---

## 💡 **Why IPCMessenger?**

### **The Problem with Raw Message Brokers**
Implementing a robust **Request-Response (RPC)** pattern between decoupled microservices using native brokers requires substantial infrastructure boilerplate:
* **RabbitMQ Complexity:** To receive a response asynchronously, the requesting service must declare a temporary, exclusive callback queue, generate a unique correlation ID, attach headers, and maintain consumer listeners. If a process crashes, orphaned queues can leak resources on the broker.
* **Redis Pub/Sub Limitations:** While Redis Pub/Sub is extremely fast for message delivery, it lacks native queue persistence, worker load balancing, and competing consumer management out of the box.

### **The IPCMessenger Solution: A Best-of-Both-Worlds Architecture**
IPCMessenger abstracts these broker mechanics away by combining them into a standardized hybrid pattern:

1. **RabbitMQ for Workload Distribution & Durability:** Used for distributing incoming requests across competing consumer queues. If your responder microservices scale up or temporarily go offline, RabbitMQ reliably queues and load-balances work without dropping messages.
2. **Redis Pub/Sub for Low-Latency Responses:** Used exclusively for routing responses directly back to the specific requesting process in-memory. This delivers high-speed, point-to-point delivery without the overhead of creating, monitoring, and destroying temporary RabbitMQ queues.

---

## 🌟 **Key Advantages**

* 🛡️ **Reliable, Durable Messaging:** Leverages RabbitMQ's persistent queueing for incoming tasks. Requests remain safely queued during worker restarts, deployment rollouts, or traffic spikes, preventing data loss.
* ⚡ **High Performance & Low Latency:** Bypasses RabbitMQ's disk/queue lifecycle overhead on the response path by utilizing Redis's ultra-fast in-memory Pub/Sub mechanism.
* 🔓 **Complete Microservice Decoupling:** Requesters and Responders operate independently. Requesters don't need to know where responders live, how many worker instances exist, or how they are implemented.
* 🧹 **Zero Queue Leaks & Reduced Broker Load:** Eliminates the classic RabbitMQ RPC anti-pattern of creating and tearing down exclusive temporary reply queues for every request, preventing broker memory bloating.
* 🚀 **Accelerated Developer Velocity:** Replaces 50+ lines of low-level `amqplib` connection, exchange, channel, correlation ID, and header handling with a clean async/await interface.
* 🤖 **AI & LLM Friendly:** Because multi-step broker boilerplate is abstracted into high-level declarative calls, AI coding agents can reliably generate complete, bug-free microservices on the first attempt.
* ⚠️ **Transparent Error Propagation:** Exceptions raised inside remote worker callbacks are automatically captured, serialized, and re-raised locally as a `RemoteServiceError` on the requester side for native `try/catch` error handling.
* 📈 **Effortless Horizontal Scaling:** Scale responder instances up or down seamlessly. RabbitMQ automatically handles competing-consumer load balancing across all active workers.
* ⚙️ **Native Promise & Event Loop Integration:** Built from the ground up on modern Node.js Promises, `async/await`, `amqplib`, and `ioredis`, allowing thousands of concurrent requests to be dispatched without blocking the event loop.

---

## 📋 **Prerequisites & Dependencies**

### **Prerequisites**
* **Node.js**: v16.0.0 or higher
* **RabbitMQ**: An accessible RabbitMQ broker instance (e.g., `amqp://guest:guest@localhost:5672/`)
* **Redis**: An accessible Redis server instance (e.g., `redis://localhost:6379/0`)

### **Dependencies**
* **`amqplib`**: Standard AMQP 0-9-1 channel-based library for Node.js.
* **`ioredis`**: Robust, full-featured Redis client for Node.js.
* **`uuid`**: RFC4122 UUID generator for unique response channels.

---

## 🛠️ **Installation**

### **Option 1: Direct Installation via npm**
Install the package directly into your project:

```bash
npm install amqplib ioredis uuid
```

### **Option 2: Installation via Git**
Install directly from GitHub:

```bash
npm install git+https://github.com/obtirinc/ipc_messanger.git
```

---

## 📂 **Project Structure**

When integrating `IPCMessenger` into a multi-service Node.js project, organize your codebase cleanly as follows:

```text
your_project/
│
├── ipc_messenger/
│   ├── index.js          # High-level Orchestrator & RemoteServiceError
│   ├── mqHandler.js      # RabbitMQ Logic (via amqplib)
│   └── cachePubSub.js    # Redis Logic (via ioredis)
│
├── serviceA.js           # Requester Service (e.g., API Gateway / Client)
└── serviceB.js           # Responder Service (e.g., Worker / Processor)
```

---

## 💻 **Implementation Guide**

### **1. The Requester (`serviceA.js`)**

This service dispatches a request to a RabbitMQ task queue and asynchronously awaits a reply over Redis.

```javascript
const { IPCMessenger, RemoteServiceError, TimeoutError } = require('./ipc_messenger');

async function main() {
  // Initialize connection settings
  const ipc = new IPCMessenger(
    'amqp://guest:guest@localhost/',
    'redis://localhost:6379/0'
  );

  await ipc.connect();

  try {
    console.log("Sending request to 'order_processing'...");
    const payload = { order_id: 123, action: 'validate' };

    // Yields control asynchronously without blocking the event loop
    const response = await ipc.sendRequest('order_processing', payload, 10);
    console.log('Server Response:', response);

  } catch (err) {
    if (err instanceof RemoteServiceError) {
      console.error(`The remote service failed with error: ${err.message}`);
    } else if (err instanceof TimeoutError) {
      console.error('The request timed out.');
    } else {
      console.error('Unexpected error:', err);
    }
  } finally {
    await ipc.close();
  }
}

main();
```

### **2. The Responder (`serviceB.js`)**

This service listens on a RabbitMQ queue as a competing consumer, processes the workload, and routes the result back via Redis.

```javascript
const { IPCMessenger } = require('./ipc_messenger');

async function processTask(data) {
  /** Your business logic goes here. */
  console.log('Processing workload:', data);

  // Simulate asynchronous work
  await new Promise((resolve) => setTimeout(resolve, 1000));

  if (data.order_id === 0) {
    throw new Error('Invalid order ID!');
  }

  return { status: 'verified', timestamp: new Date().toISOString() };
}

async function main() {
  const ipc = new IPCMessenger(
    'amqp://guest:guest@localhost/',
    'redis://localhost:6379/0'
  );

  await ipc.connect();

  console.log("Worker listening on queue 'order_processing'...");
  // Maintains a persistent consumer listener
  await ipc.startListening('order_processing', processTask);
}

main();
```

---

## 🔄 **Technical Flow (Under the Hood)**

```text
[ Requester Service ]                                    [ Responder Worker ]
         │                                                        │
         ├── 1. Generate unique channel UUID ─────────────────────┤
         │     (e.g., response_channel:xyz)                       │
         │                                                        │
         ├── 2. Subscribe to Redis channel: response_channel:xyz  │
         │                                                        │
         ├── 3. Publish payload + channel UUID to RabbitMQ ──────►│
         │                                                        ├── 4. Consume from queue
         │                                                        ├── 5. Execute callback
         │                                                        │
         │◄── 6. Publish result/error to Redis ───────────────────┤
         │       (channel: response_channel:xyz)                  │
         │                                                        │
         ├── 7. Unsubscribe from Redis & return payload ──────────┘
```

---

## ⚙️ **API Reference**

### **`new IPCMessenger(rabbitmqUrl, redisUrl)`**
* **`rabbitmqUrl`** *(string)*: AMQP connection string (e.g., `amqp://user:pass@host:5672/`).
* **`redisUrl`** *(string)*: Redis connection string (e.g., `redis://host:6379/0`).

### **`await connect()`**
Establishes underlying asynchronous connection pools to both RabbitMQ and Redis.

### **`await sendRequest(queueName, payload, timeout = 10)`**
* **`queueName`** *(string)*: Target RabbitMQ queue.
* **`payload`** *(object)*: JSON-serializable object containing command/request data.
* **`timeout`** *(number)*: Maximum time in seconds to wait for a reply before throwing `TimeoutError`.
* **Returns**: `Promise<object>` containing the response from the remote responder.

### **`await startListening(queueName, processCallback)`**
* **`queueName`** *(string)*: Target RabbitMQ queue to consume from.
* **`processCallback`** *(function)*: An `async` function accepting `payload` (object) as an argument and returning a result (object).

### **`await close()`**
Gracefully shuts down active consumers and closes broker connection pools.

---

## ⚠️ **Error Handling & Propagation**

`IPCMessenger` includes a built-in remote exception handling mechanism. If an uncaught exception occurs within the `processCallback` on the Responder side:

1. The Responder catches the exception automatically.
2. It serializes the exception class name and error message into a structured error payload.
3. The Responder sends this error back across the Redis response channel.
4. The Requester receives the payload and throws a **`RemoteServiceError`**, allowing the caller to handle remote failures gracefully.

```javascript
try {
  const result = await ipc.sendRequest('order_processing', payload);
} catch (err) {
  if (err instanceof RemoteServiceError) {
    console.error(`Remote service exception caught: ${err.message}`);
  }
}
```

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
