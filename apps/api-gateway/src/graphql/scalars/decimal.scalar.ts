import { GraphQLScalarType, Kind, GraphQLError } from 'graphql';

const DECIMAL_REGEX = /^\d+(\.\d{1,2})?$/;

function validateAndFormatDecimal(val: string): string {
  if (!DECIMAL_REGEX.test(val)) {
    throw new GraphQLError(
      `Invalid Decimal value: "${val}". Must be a non-negative decimal string with up to 2 decimal places (e.g., "100.00").`,
    );
  }
  // Normalize to 2 decimal places if needed (e.g. "100" -> "100.00", "100.5" -> "100.50")
  const parts = val.split('.');
  const whole = parts[0] ?? '0';
  const frac = parts[1] ?? '';
  if (frac.length === 0) {
    return `${whole}.00`;
  }
  if (frac.length === 1) {
    return `${whole}.${frac}0`;
  }
  return `${whole}.${frac}`;
}

export const DecimalScalar = new GraphQLScalarType({
  name: 'Decimal',
  description:
    'Fixed-point decimal value represented as a string with up to 2 decimal places to prevent floating-point loss.',
  serialize(value: unknown): string {
    if (typeof value === 'string') {
      return validateAndFormatDecimal(value);
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || value < 0) {
        throw new GraphQLError(`Decimal cannot represent invalid number: ${value}`);
      }
      return validateAndFormatDecimal(value.toFixed(2));
    }
    throw new GraphQLError(
      `Decimal cannot represent non-string or non-number value: ${JSON.stringify(value)}`,
    );
  },
  parseValue(value: unknown): string {
    if (typeof value === 'string') {
      return validateAndFormatDecimal(value);
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || value < 0) {
        throw new GraphQLError(`Decimal cannot represent invalid number: ${value}`);
      }
      return validateAndFormatDecimal(value.toFixed(2));
    }
    throw new GraphQLError(`Decimal cannot represent value: ${JSON.stringify(value)}`);
  },
  parseLiteral(ast): string {
    if (ast.kind === Kind.STRING) {
      return validateAndFormatDecimal(ast.value);
    }
    if (ast.kind === Kind.INT) {
      return validateAndFormatDecimal(`${ast.value}.00`);
    }
    throw new GraphQLError(
      `Decimal literal must be a string (e.g., "100.00") or integer, received kind: ${ast.kind}`,
    );
  },
});
