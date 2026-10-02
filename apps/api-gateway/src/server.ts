import { ApolloServer } from '@apollo/server';
import { startStandaloneServer } from '@apollo/server/standalone';
import { typeDefs } from './graphql/schema/typeDefs.js';
import { resolvers, GraphQLContext } from './graphql/resolvers/index.js';
import { OrderServiceClient, IOrderServiceClient } from './clients/order-service.client.js';
import { env } from './config/env.js';

export interface CreateServerOptions {
  orderServiceClient?: IOrderServiceClient;
}

export function createApolloServer(): ApolloServer<GraphQLContext> {
  return new ApolloServer<GraphQLContext>({
    typeDefs,
    resolvers,
    formatError: (formattedError) => {
      // Ensure stack traces are never exposed
      if (formattedError.extensions) {
        delete formattedError.extensions.stacktrace;
      }

      // Allow validation and user input errors through with sanitized messages
      if (
        formattedError.extensions?.code === 'BAD_USER_INPUT' ||
        formattedError.extensions?.code === 'GRAPHQL_VALIDATION_FAILED' ||
        formattedError.extensions?.code === 'BAD_REQUEST'
      ) {
        return {
          message: formattedError.message,
          extensions: formattedError.extensions,
        };
      }

      // Sanitize internal errors so database internals, filesystem paths, etc. are never leaked
      return {
        message: 'An internal error occurred while processing the request',
        extensions: {
          code: 'INTERNAL_SERVER_ERROR',
        },
      };
    },
  });
}

export interface RunningGatewayServer {
  server: ApolloServer<GraphQLContext>;
  url: string;
  stop: () => Promise<void>;
}

export async function startGatewayServer(
  port?: number,
  options?: CreateServerOptions,
): Promise<RunningGatewayServer> {
  const listenPort = port ?? env.API_GATEWAY_PORT;
  const orderServiceClient = options?.orderServiceClient ?? new OrderServiceClient();

  const server = createApolloServer();

  const { url } = await startStandaloneServer(server, {
    listen: { port: listenPort },
    context: async (): Promise<GraphQLContext> => ({
      orderServiceClient,
    }),
  });

  const graphqlUrl = `${url.replace(/\/$/, '')}/graphql`;
  console.log(`[ApiGateway] GraphQL API Gateway running at: ${graphqlUrl}`);

  return {
    server,
    url: graphqlUrl,
    stop: async () => {
      await server.stop();
    },
  };
}
