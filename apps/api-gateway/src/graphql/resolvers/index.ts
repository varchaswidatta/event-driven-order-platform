import { DateTimeScalar, DecimalScalar } from '../scalars/index.js';
import { orderResolvers, GraphQLContext } from './order.resolver.js';

export { GraphQLContext };

export const resolvers = {
  DateTime: DateTimeScalar,
  Decimal: DecimalScalar,
  Query: {
    ...orderResolvers.Query,
  },
  Mutation: {
    ...orderResolvers.Mutation,
  },
};
