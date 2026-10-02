export const typeDefs = `#graphql
  scalar DateTime
  scalar Decimal

  enum OrderStatus {
    PENDING
    INVENTORY_PROCESSING
    CONFIRMED
    INVENTORY_FAILED
  }

  type OrderItem {
    productId: ID!
    quantity: Int!
    unitPrice: Decimal!
  }

  type Order {
    id: ID!
    customerId: ID!
    status: OrderStatus!
    totalAmount: Decimal!
    currency: String!
    items: [OrderItem!]!
    createdAt: DateTime!
    updatedAt: DateTime!
  }

  input OrderItemInput {
    productId: ID!
    quantity: Int!
    unitPrice: Decimal!
  }

  input CreateOrderInput {
    customerId: ID!
    items: [OrderItemInput!]!
  }

  type Query {
    order(id: ID!): Order
    orders(customerId: ID): [Order!]!
  }

  type Mutation {
    createOrder(input: CreateOrderInput!): Order!
  }
`;
