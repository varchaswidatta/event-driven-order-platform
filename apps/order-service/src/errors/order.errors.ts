export abstract class OrderDomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidOrderInputError extends OrderDomainError {
  public readonly issues: string[];

  constructor(message: string, issues: string[] = []) {
    const formattedMessage = issues.length > 0 ? `${message}: ${issues.join('; ')}` : message;
    super(formattedMessage);
    this.issues = issues;
  }
}

export class OrderNotFoundError extends OrderDomainError {
  public readonly orderId: string;

  constructor(orderId: string) {
    super(`Order with ID "${orderId}" not found`);
    this.orderId = orderId;
  }
}

export class DatabaseOperationError extends OrderDomainError {
  constructor(message: string) {
    // Sanitized database error that omits internal connection strings and credentials
    super(message);
  }
}
