import { env } from '../config/env.js';
import { CreateOrderInput, OrderDto } from '../types/order.types.js';

export class OrderServiceValidationError extends Error {
  constructor(
    message: string,
    public readonly issues?: string[],
  ) {
    super(message);
    this.name = 'OrderServiceValidationError';
  }
}

export class OrderServiceHttpError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'OrderServiceHttpError';
  }
}

export interface IOrderServiceClient {
  createOrder(input: CreateOrderInput): Promise<OrderDto>;
  getOrderById(id: string): Promise<OrderDto | null>;
  listOrders(customerId?: string): Promise<OrderDto[]>;
}

export class OrderServiceClient implements IOrderServiceClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;

  constructor(options?: { baseUrl?: string; timeoutMs?: number }) {
    this.baseUrl = (options?.baseUrl ?? env.ORDER_SERVICE_URL).replace(/\/$/, '');
    this.timeoutMs = options?.timeoutMs ?? 5000;
  }

  async createOrder(input: CreateOrderInput): Promise<OrderDto> {
    const url = `${this.baseUrl}/orders`;
    let response: Response;

    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        body: JSON.stringify(input),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Network failure';
      throw new OrderServiceHttpError(503, `Order Service unreachable: ${message}`);
    }

    if (response.status === 201 || response.status === 200) {
      return (await response.json()) as OrderDto;
    }

    if (response.status === 400) {
      const errorBody = (await response.json().catch(() => ({}))) as {
        message?: string;
        issues?: string[];
      };
      throw new OrderServiceValidationError(
        errorBody.message ?? 'Invalid order input data',
        errorBody.issues,
      );
    }

    const errorBody = (await response.json().catch(() => ({}))) as {
      message?: string;
    };
    throw new OrderServiceHttpError(
      response.status,
      errorBody.message ?? `Order Service returned HTTP ${response.status}`,
    );
  }

  async getOrderById(id: string): Promise<OrderDto | null> {
    const url = `${this.baseUrl}/orders/${encodeURIComponent(id)}`;
    let response: Response;

    try {
      response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Network failure';
      throw new OrderServiceHttpError(503, `Order Service unreachable: ${message}`);
    }

    if (response.status === 200) {
      return (await response.json()) as OrderDto;
    }

    if (response.status === 404) {
      return null;
    }

    const errorBody = (await response.json().catch(() => ({}))) as {
      message?: string;
    };
    throw new OrderServiceHttpError(
      response.status,
      errorBody.message ?? `Order Service returned HTTP ${response.status}`,
    );
  }

  async listOrders(customerId?: string): Promise<OrderDto[]> {
    const query = customerId ? `?customerId=${encodeURIComponent(customerId)}` : '';
    const url = `${this.baseUrl}/orders${query}`;
    let response: Response;

    try {
      response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Network failure';
      throw new OrderServiceHttpError(503, `Order Service unreachable: ${message}`);
    }

    if (response.status === 200) {
      return (await response.json()) as OrderDto[];
    }

    const errorBody = (await response.json().catch(() => ({}))) as {
      message?: string;
    };
    throw new OrderServiceHttpError(
      response.status,
      errorBody.message ?? `Order Service returned HTTP ${response.status}`,
    );
  }
}
