import path from 'node:path';
import fs from 'node:fs';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

export function getStockProtoPath(): string {
  const candidates = [
    path.resolve(process.cwd(), 'proto/stock.proto'),
    path.resolve(process.cwd(), '../../proto/stock.proto'),
    path.resolve(__dirname, '../../../proto/stock.proto'),
    path.resolve(__dirname, '../../../../proto/stock.proto'),
  ];

  const found = candidates.find((p) => fs.existsSync(p));
  if (!found) {
    throw new Error(
      `stock.proto not found. Checked candidate locations:\n${candidates.join('\n')}`,
    );
  }
  return found;
}

export function loadStockProtoDefinition(): grpc.GrpcObject {
  const protoPath = getStockProtoPath();
  const packageDefinition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  return grpc.loadPackageDefinition(packageDefinition);
}
