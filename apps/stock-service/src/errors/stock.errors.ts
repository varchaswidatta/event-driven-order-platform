export abstract class StockDomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class DatabaseOperationError extends StockDomainError {
  constructor(message: string) {
    super(message);
  }
}

export class InsufficientStockError extends StockDomainError {
  constructor(
    public readonly productId: string,
    public readonly requested: number,
    public readonly available: number,
  ) {
    super(
      `Insufficient stock for product "${productId}": requested ${requested}, available ${available}`,
    );
  }
}

export class ProductNotFoundError extends StockDomainError {
  constructor(public readonly productId: string) {
    super(`Product not found: "${productId}"`);
  }
}

export class InvalidStockRequestError extends StockDomainError {
  constructor(
    message: string,
    public readonly issues: string[] = [],
  ) {
    const formatted = issues.length > 0 ? `${message}: ${issues.join('; ')}` : message;
    super(formatted);
  }
}
