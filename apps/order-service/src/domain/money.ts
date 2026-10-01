/**
 * Decimal-safe monetary arithmetic utility for PostgreSQL NUMERIC(12,2).
 *
 * Floating-point numbers (IEEE-754) suffer from precision errors (e.g. 0.1 + 0.2 !== 0.3).
 * To guarantee absolute arithmetic accuracy and prevent precision drift in financial calculations:
 * 1. String amounts are parsed into integer minor units (cents) using BigInt.
 * 2. All multiplications and additions are performed using exact BigInt integer math.
 * 3. The final result is formatted back to an exact two-decimal place string representation.
 */
export class Money {
  /**
   * Converts a decimal string representation (e.g., "19.99", "5", "0.50") into integer cents (BigInt).
   */
  static parseToCents(amountStr: string): bigint {
    const trimmed = amountStr.trim();
    if (!/^\d+(\.\d{1,2})?$/.test(trimmed)) {
      throw new Error(
        `Invalid monetary amount format: "${amountStr}". Expected non-negative numeric string with up to 2 decimal places.`,
      );
    }

    const parts = trimmed.split('.');
    const wholePart = parts[0] ?? '0';
    const fractionalPart = parts[1] ?? '';
    const paddedFraction = fractionalPart.padEnd(2, '0');
    return BigInt(wholePart) * 100n + BigInt(paddedFraction);
  }

  /**
   * Formats integer cents (BigInt) into a standard 2-decimal string (e.g., 1999n -> "19.99").
   */
  static formatFromCents(cents: bigint): string {
    if (cents < 0n) {
      throw new Error('Monetary amounts cannot be negative');
    }
    const whole = cents / 100n;
    const fraction = cents % 100n;
    return `${whole.toString()}.${fraction.toString().padStart(2, '0')}`;
  }

  /**
   * Calculates total amount: sum(quantity * unitPrice) using exact integer arithmetic.
   */
  static calculateTotal(items: Array<{ quantity: number; unitPrice: string }>): string {
    let totalCents = 0n;
    for (const item of items) {
      if (!Number.isInteger(item.quantity) || item.quantity <= 0) {
        throw new Error(`Invalid quantity: ${item.quantity}. Must be a positive integer.`);
      }
      const unitCents = Money.parseToCents(item.unitPrice);
      totalCents += BigInt(item.quantity) * unitCents;
    }
    return Money.formatFromCents(totalCents);
  }
}
