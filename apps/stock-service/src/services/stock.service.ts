import { IStockRepository } from '../repositories/stock.repository.js';
import { ReserveStockInput, ReserveStockResult } from '../domain/stock.js';
import { reserveStockRequestSchema } from '../validation/stock.schema.js';
import { InvalidStockRequestError } from '../errors/stock.errors.js';

export interface IStockService {
  reserveStock(rawInput: unknown): Promise<ReserveStockResult>;
}

export class StockService implements IStockService {
  constructor(private readonly repository: IStockRepository) {}

  /**
   * Validates the incoming reserveStock payload and delegates atomic reservation to repository.
   *
   * @throws InvalidStockRequestError if input validation fails (e.g. invalid UUID, non-positive quantity, empty items)
   */
  async reserveStock(rawInput: unknown): Promise<ReserveStockResult> {
    const parseResult = reserveStockRequestSchema.safeParse(rawInput);
    if (!parseResult.success) {
      const issues = parseResult.error.issues.map(
        (issue) => `${issue.path.join('.')}: ${issue.message}`,
      );
      throw new InvalidStockRequestError('Invalid stock reservation request', issues);
    }

    const input: ReserveStockInput = parseResult.data;
    return this.repository.reserveStock(input);
  }
}
