export abstract class InventoryDomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class DatabaseOperationError extends InventoryDomainError {
  constructor(message: string) {
    // Sanitized database error that omits internal connection strings and credentials
    super(message);
  }
}

export class DuplicateReservationError extends InventoryDomainError {
  public readonly orderId: string;

  constructor(orderId: string) {
    super(`Reservation for order "${orderId}" already exists`);
    this.orderId = orderId;
  }
}

export class InvalidEventError extends InventoryDomainError {
  public readonly issues: string[];

  constructor(message: string, issues: string[] = []) {
    const formattedMessage = issues.length > 0 ? `${message}: ${issues.join('; ')}` : message;
    super(formattedMessage);
    this.issues = issues;
  }
}

export class UnsupportedEventTypeError extends InventoryDomainError {
  public readonly eventType: string;

  constructor(eventType: string) {
    super(`Unsupported event type: "${eventType}"`);
    this.eventType = eventType;
  }
}

export class UnsupportedEventVersionError extends InventoryDomainError {
  public readonly eventType: string;
  public readonly eventVersion: number;

  constructor(eventType: string, eventVersion: number) {
    super(`Unsupported event version ${eventVersion} for event type "${eventType}"`);
    this.eventType = eventType;
    this.eventVersion = eventVersion;
  }
}
