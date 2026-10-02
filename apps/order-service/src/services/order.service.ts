import { CreateOrderInput, Order } from '../domain/order.js';
import { ORDER_STATUS } from '../domain/order-status.js';
import { Money } from '../domain/money.js';
import { createOrderSchema } from '../validation/order.schema.js';
import { InvalidOrderInputError, OrderNotFoundError } from '../errors/order.errors.js';
import { IOrderRepository } from '../repositories/order.repository.js';

export class OrderService {
  constructor(private readonly orderRepository: IOrderRepository) {}

  /**
   * Validates order input, computes total amount using decimal-safe math,
   * initializes order status to PENDING, and persists via the repository.
   */
  async createOrder(input: CreateOrderInput): Promise<Order> {
    // 1. Validate input against schema rules
    const validationResult = createOrderSchema.safeParse(input);
    if (!validationResult.success) {
      const issues = validationResult.error.issues.map(
        (issue) => `${issue.path.join('.')}: ${issue.message}`,
      );
      throw new InvalidOrderInputError('Invalid order input data', issues);
    }

    const validated = validationResult.data;

    // 2. Compute total amount using exact BigInt integer cents
    const totalAmount = Money.calculateTotal(validated.items);

    // 3. Persist via repository with PENDING status
    const createdOrder = await this.orderRepository.createOrder({
      customerId: validated.customerId,
      status: ORDER_STATUS.PENDING,
      totalAmount,
      currency: validated.currency,
      items: validated.items.map((item) => ({
        productId: item.productId,
        quantity: item.quantity,
        unitPrice: item.unitPrice,
      })),
    });

    return createdOrder;
  }

  /**
   * Retrieves an order by ID or throws OrderNotFoundError.
   */
  async getOrderById(id: string): Promise<Order> {
    const order = await this.orderRepository.findOrderById(id);
    if (!order) {
      throw new OrderNotFoundError(id);
    }
    return order;
  }

  /**
   * Retrieves all orders for a given customer ID with their items.
   */
  async getOrdersByCustomerId(customerId: string): Promise<Order[]> {
    return this.orderRepository.findOrdersByCustomerId(customerId);
  }

  /**
   * Retrieves orders, optionally filtered by customer ID.
   */
  async getOrders(customerId?: string): Promise<Order[]> {
    if (customerId) {
      return this.orderRepository.findOrdersByCustomerId(customerId);
    }
    if (this.orderRepository.findAllOrders) {
      return this.orderRepository.findAllOrders();
    }
    return [];
  }
}
