import { GraphQLScalarType, Kind, GraphQLError } from 'graphql';

export const DateTimeScalar = new GraphQLScalarType({
  name: 'DateTime',
  description:
    'A date-time string at UTC, such as 2026-10-01T18:30:00.000Z, compliant with the ISO 8601 standard.',
  serialize(value: unknown): string {
    if (value instanceof Date) {
      if (isNaN(value.getTime())) {
        throw new GraphQLError('DateTime cannot represent an invalid Date instance');
      }
      return value.toISOString();
    }
    if (typeof value === 'string' || typeof value === 'number') {
      const date = new Date(value);
      if (isNaN(date.getTime())) {
        throw new GraphQLError(`DateTime cannot represent an invalid date value: ${value}`);
      }
      return date.toISOString();
    }
    throw new GraphQLError(
      `DateTime cannot represent non-string or non-date value: ${JSON.stringify(value)}`,
    );
  },
  parseValue(value: unknown): string {
    if (typeof value === 'string') {
      const date = new Date(value);
      if (isNaN(date.getTime())) {
        throw new GraphQLError(`Invalid ISO 8601 DateTime string: ${value}`);
      }
      return date.toISOString();
    }
    if (value instanceof Date) {
      if (isNaN(value.getTime())) {
        throw new GraphQLError('DateTime cannot represent an invalid Date instance');
      }
      return value.toISOString();
    }
    throw new GraphQLError(`DateTime cannot represent non-string value: ${JSON.stringify(value)}`);
  },
  parseLiteral(ast): string {
    if (ast.kind !== Kind.STRING) {
      throw new GraphQLError(`DateTime must be a string literal, received: ${ast.kind}`);
    }
    const date = new Date(ast.value);
    if (isNaN(date.getTime())) {
      throw new GraphQLError(`Invalid ISO 8601 DateTime literal: ${ast.value}`);
    }
    return date.toISOString();
  },
});
