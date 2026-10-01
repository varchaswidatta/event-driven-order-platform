import { describe, it, expect } from 'vitest';
import { Money } from '../../src/domain/money.js';

describe('Money utility (decimal-safe arithmetic)', () => {
  it('parses valid decimal strings to integer cents', () => {
    expect(Money.parseToCents('0.00')).toBe(0n);
    expect(Money.parseToCents('10')).toBe(1000n);
    expect(Money.parseToCents('10.5')).toBe(1050n);
    expect(Money.parseToCents('19.99')).toBe(1999n);
    expect(Money.parseToCents('100.05')).toBe(10005n);
  });

  it('formats integer cents into two-decimal strings', () => {
    expect(Money.formatFromCents(0n)).toBe('0.00');
    expect(Money.formatFromCents(5n)).toBe('0.05');
    expect(Money.formatFromCents(50n)).toBe('0.50');
    expect(Money.formatFromCents(1000n)).toBe('10.00');
    expect(Money.formatFromCents(1999n)).toBe('19.99');
  });

  it('calculates total for multiple items without floating-point precision loss', () => {
    // 3 items at 19.99 = 59.97
    // In IEEE-754: 19.99 * 3 = 59.970000000000006
    const items = [
      { quantity: 3, unitPrice: '19.99' },
      { quantity: 2, unitPrice: '10.50' }, // 21.00
      { quantity: 1, unitPrice: '0.03' }, // 0.03
    ];
    // Total should be exactly 81.00
    const total = Money.calculateTotal(items);
    expect(total).toBe('81.00');
  });

  it('handles single item with 0.10 and 0.20 correctly without IEEE-754 error', () => {
    // Classic 0.10 + 0.20 test
    const items = [
      { quantity: 1, unitPrice: '0.10' },
      { quantity: 1, unitPrice: '0.20' },
    ];
    expect(Money.calculateTotal(items)).toBe('0.30');
  });

  it('throws on invalid decimal string format or negative amounts', () => {
    expect(() => Money.parseToCents('invalid')).toThrow();
    expect(() => Money.parseToCents('-5.00')).toThrow();
    expect(() => Money.parseToCents('10.999')).toThrow(); // More than 2 decimals
  });

  it('throws on invalid quantity', () => {
    expect(() => Money.calculateTotal([{ quantity: 0, unitPrice: '10.00' }])).toThrow();
    expect(() => Money.calculateTotal([{ quantity: -1, unitPrice: '10.00' }])).toThrow();
    expect(() => Money.calculateTotal([{ quantity: 1.5, unitPrice: '10.00' }])).toThrow();
  });
});
