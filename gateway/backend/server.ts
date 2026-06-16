import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'user.proto');

const MOCK_USERS: Record<string, { id: string; name: string; email: string }> = {
  '1': { id: '1', name: 'Alice Johnson', email: 'alice@example.com' },
  '2': { id: '2', name: 'Bob Smith', email: 'bob@example.com' },
  '3': { id: '3', name: 'Charlie Brown', email: 'charlie@example.com' },
};

function createServer(): grpc.Server {
  const server = new grpc.Server();

  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
  const gatewayPkg = (protoDescriptor as any).gateway;
  if (!gatewayPkg || !gatewayPkg.UserService) {
    throw new Error('gateway.UserService not found in proto definition');
  }

  server.addService(gatewayPkg.UserService.service, {
    GetUser: (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      const userId = call.request.id;
      console.log(`[Backend] GetUser called with id=${userId}`);

      const user = MOCK_USERS[userId];
      if (!user) {
        callback({
          code: grpc.status.NOT_FOUND,
          message: `User ${userId} not found`,
        } as grpc.ServiceError);
        return;
      }

      callback(null, user);
    },

    CreateUser: (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      const { name, email } = call.request;
      console.log(`[Backend] CreateUser called with name=${name}, email=${email}`);

      const newId = String(Object.keys(MOCK_USERS).length + 1);
      const newUser = { id: newId, name, email };
      MOCK_USERS[newId] = newUser;

      callback(null, newUser);
    },

    ListUsers: (call: grpc.ServerWritableStream<any, any>) => {
      const pageSize = call.request.page_size || 10;
      console.log(`[Backend] ListUsers called with page_size=${pageSize}`);

      const users = Object.values(MOCK_USERS);
      let index = 0;

      const sendNext = () => {
        if (index >= users.length) {
          call.end();
          return;
        }

        call.write(users[index]);
        index++;

        setTimeout(sendNext, 500);
      };

      sendNext();
    },

    StreamEvents: (call: grpc.ServerWritableStream<any, any>) => {
      const topic = call.request.topic || 'default';
      console.log(`[Backend] StreamEvents started for topic=${topic}`);

      let eventCount = 0;
      const maxEvents = 10;

      const interval = setInterval(() => {
        if (call.destroyed || eventCount >= maxEvents) {
          clearInterval(interval);
          if (!call.destroyed) {
            call.end();
          }
          console.log(`[Backend] StreamEvents ended for topic=${topic}, sent ${eventCount} events`);
          return;
        }

        eventCount++;
        const event = {
          type: `${topic}.event_${eventCount}`,
          data: JSON.stringify({ message: `Event #${eventCount} on topic '${topic}'`, value: Math.random() }),
          timestamp: Date.now(),
        };

        call.write(event);
      }, 1000);

      call.on('cancelled', () => {
        clearInterval(interval);
        console.log(`[Backend] StreamEvents cancelled for topic=${topic}`);
      });
    },
  });

  return server;
}

async function main() {
  const port = 50051;
  const server = createServer();

  await new Promise<void>((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${port}`,
      grpc.ServerCredentials.createInsecure(),
      (err, boundPort) => {
        if (err) {
          reject(err);
          return;
        }
        server.start();
        console.log(`[Backend] gRPC mock server started on port ${boundPort}`);
        console.log(`[Backend] Services: gateway.UserService`);
        console.log(`[Backend] Methods:  GetUser, CreateUser, ListUsers (stream), StreamEvents (stream)`);
        resolve();
      }
    );
  });

  const shutdown = async () => {
    console.log('\n[Backend] Shutting down...');
    await new Promise<void>((resolve) => {
      server.tryShutdown(() => resolve());
    });
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[Backend] Failed to start:', err);
  process.exit(1);
});
