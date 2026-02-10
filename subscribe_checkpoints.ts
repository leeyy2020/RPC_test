// subscribe_checkpoints.ts
import 'dotenv/config';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { bcs, splitGenericParameters, toHex, type BcsType } from '@mysten/bcs';
import { SuiGrpcClient } from '@mysten/sui/grpc';
import type { SuiJsonRpcClient, SuiMoveNormalizedStruct, SuiMoveNormalizedType } from '@mysten/sui/jsonRpc';
import { SuiJsonRpcClient as JsonRpcClient } from '@mysten/sui/jsonRpc';

const baseUrl = process.env.FULLNODE_URL ?? 'http://127.0.0.1:9000';
const targetObjectId = process.env.TARGET_OBJECT_ID?.toLowerCase();
const checkpointDir = '/root/rpc_test/checkpoints_json';
const jsonRpcUrl = process.env.JSONRPC_URL ?? baseUrl;
const decodeConcurrency = Math.max(
  1,
  Number.parseInt(process.env.DECODE_CONCURRENCY ?? '8', 10) || 8,
);

// 你这个节点看起来是 mainnet 全节点（本地暴露端口），network 建议填 mainnet
const client = new SuiGrpcClient({
  network: 'mainnet',
  baseUrl,
});
const jsonRpcClient = new JsonRpcClient({ url: jsonRpcUrl });

type StructTag = {
  address: string;
  module: string;
  name: string;
  typeArgs: SuiMoveNormalizedType[];
};

type SqrtChange = {
  txDigest: string | null;
  checkpoint: string;
  objectId: string;
  beforeSqrtPrice: string | null;
  afterSqrtPrice: string | null;
  sqrtDelta: string | null;
  eventType: string | null;
  matchedBy: Array<'event_pool_id' | 'event_pool'>;
};

const addressBcs = bcs.bytes(32).transform({
  input: (value: string | Uint8Array) => {
    if (typeof value === 'string') {
      const hex = value.startsWith('0x') ? value.slice(2) : value;
      if (hex.length !== 64) throw new Error(`Invalid address hex length: ${value}`);
      const bytes = new Uint8Array(32);
      for (let i = 0; i < 32; i += 1) {
        bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
      }
      return bytes;
    }
    return value;
  },
  output: (value) => `0x${toHex(new Uint8Array(value))}`,
});

class MoveEventBcsDecoder {
  private readonly client: SuiJsonRpcClient;
  private readonly structDefCache = new Map<string, Promise<SuiMoveNormalizedStruct>>();
  private readonly concreteTypeCache = new Map<string, BcsType<any>>();
  private readonly primitiveTypeCache = new Map<string, BcsType<any>>();

  constructor(client: SuiJsonRpcClient) {
    this.client = client;
    this.primitiveTypeCache.set('Bool', bcs.bool());
    this.primitiveTypeCache.set('U8', bcs.u8());
    this.primitiveTypeCache.set('U16', bcs.u16());
    this.primitiveTypeCache.set('U32', bcs.u32());
    this.primitiveTypeCache.set('U64', bcs.u64());
    this.primitiveTypeCache.set('U128', bcs.u128());
    this.primitiveTypeCache.set('U256', bcs.u256());
    this.primitiveTypeCache.set('Address', addressBcs);
    this.primitiveTypeCache.set('Signer', addressBcs);
  }

  async decodeContents(typeName: string, bytes: Uint8Array): Promise<Record<string, unknown> | null> {
    const tag = parseStructTag(typeName);
    const concrete = structTagToString(tag);
    const schema = await this.getConcreteTypeBcs(concrete, tag);
    const parsed = schema.parse(bytes);
    if (!parsed || typeof parsed !== 'object') return null;
    return parsed as Record<string, unknown>;
  }

  private async getStructDef(address: string, module: string, name: string): Promise<SuiMoveNormalizedStruct> {
    const key = `${address.toLowerCase()}::${module}::${name}`;
    const cached = this.structDefCache.get(key);
    if (cached) return cached;
    const promise = this.client.getNormalizedMoveStruct({
      package: address,
      module,
      struct: name,
    });
    this.structDefCache.set(key, promise);
    return promise;
  }

  private async getConcreteTypeBcs(concreteTypeKey: string, tag: StructTag): Promise<BcsType<any>> {
    const cached = this.concreteTypeCache.get(concreteTypeKey);
    if (cached) return cached;

    const structDef = await this.getStructDef(tag.address, tag.module, tag.name);
    const fieldEntries: Record<string, BcsType<any>> = {};
    for (const field of structDef.fields) {
      fieldEntries[field.name] = await this.toBcsType(field.type, tag.typeArgs);
    }
    const schema = bcs.struct(concreteTypeKey, fieldEntries);
    this.concreteTypeCache.set(concreteTypeKey, schema);
    return schema;
  }

  private async toBcsType(
    moveType: SuiMoveNormalizedType,
    genericArgs: SuiMoveNormalizedType[],
  ): Promise<BcsType<any>> {
    if (typeof moveType === 'string') {
      const primitive = this.primitiveTypeCache.get(moveType);
      if (!primitive) throw new Error(`Unsupported primitive move type: ${moveType}`);
      return primitive;
    }

    if ('Vector' in moveType) return bcs.vector(await this.toBcsType(moveType.Vector, genericArgs));
    if ('TypeParameter' in moveType) {
      const concrete = genericArgs[moveType.TypeParameter];
      if (!concrete) throw new Error(`Missing type parameter at index ${moveType.TypeParameter}`);
      return this.toBcsType(concrete, genericArgs);
    }
    if ('Reference' in moveType) return this.toBcsType(moveType.Reference, genericArgs);
    if ('MutableReference' in moveType) return this.toBcsType(moveType.MutableReference, genericArgs);
    if ('Struct' in moveType) {
      const resolvedArgs = await Promise.all(
        moveType.Struct.typeArguments.map(async (arg) => this.resolveTypeParameters(arg, genericArgs)),
      );
      const nestedTag: StructTag = {
        address: moveType.Struct.address,
        module: moveType.Struct.module,
        name: moveType.Struct.name,
        typeArgs: resolvedArgs,
      };
      return this.getConcreteTypeBcs(structTagToString(nestedTag), nestedTag);
    }

    throw new Error(`Unsupported move type node: ${JSON.stringify(moveType)}`);
  }

  private async resolveTypeParameters(
    moveType: SuiMoveNormalizedType,
    genericArgs: SuiMoveNormalizedType[],
  ): Promise<SuiMoveNormalizedType> {
    if (typeof moveType === 'string') return moveType;
    if ('TypeParameter' in moveType) {
      const resolved = genericArgs[moveType.TypeParameter];
      if (!resolved) throw new Error(`Missing type argument at index ${moveType.TypeParameter}`);
      return this.resolveTypeParameters(resolved, genericArgs);
    }
    if ('Vector' in moveType) return { Vector: await this.resolveTypeParameters(moveType.Vector, genericArgs) };
    if ('Reference' in moveType) {
      return { Reference: await this.resolveTypeParameters(moveType.Reference, genericArgs) };
    }
    if ('MutableReference' in moveType) {
      return { MutableReference: await this.resolveTypeParameters(moveType.MutableReference, genericArgs) };
    }
    if ('Struct' in moveType) {
      return {
        Struct: {
          address: moveType.Struct.address,
          module: moveType.Struct.module,
          name: moveType.Struct.name,
          typeArguments: await Promise.all(
            moveType.Struct.typeArguments.map((arg) => this.resolveTypeParameters(arg, genericArgs)),
          ),
        },
      };
    }
    return moveType;
  }
}

function formatCheckpointTimestamp(
  ts?: { seconds?: bigint; nanos?: number } | null,
): string {
  if (!ts?.seconds) return 'n/a';
  const ms = Number(ts.seconds) * 1000 + Math.floor((ts.nanos ?? 0) / 1_000_000);
  return new Date(ms).toISOString();
}

function formatLocalNow(): string {
  return new Date().toISOString();
}

function replacer(_key: string, value: unknown) {
  if (typeof value === 'bigint') return value.toString();
  return value;
}

function formatMs(ms: number): string {
  return ms.toFixed(2);
}

async function runWithConcurrency<T>(tasks: Array<() => Promise<T>>, concurrency: number): Promise<T[]> {
  if (tasks.length === 0) return [];
  const workers = Math.min(concurrency, tasks.length);
  const results = new Array<T>(tasks.length);
  let cursor = 0;

  const runWorker = async () => {
    while (true) {
      const current = cursor;
      cursor += 1;
      if (current >= tasks.length) return;
      results[current] = await tasks[current]();
    }
  };

  await Promise.all(Array.from({ length: workers }, () => runWorker()));
  return results;
}

function parseStructTag(typeName: string): StructTag {
  const firstAngle = typeName.indexOf('<');
  const head = firstAngle === -1 ? typeName : typeName.slice(0, firstAngle);
  const parts = head.split('::');
  if (parts.length !== 3) throw new Error(`Invalid struct tag: ${typeName}`);
  const [address, module, name] = parts;
  const typeArgs = firstAngle === -1 ? [] : parseTypeArguments(typeName);
  return { address: address.toLowerCase(), module, name, typeArgs };
}

function parseTypeArguments(typeName: string): SuiMoveNormalizedType[] {
  const start = typeName.indexOf('<');
  const end = typeName.lastIndexOf('>');
  if (start === -1 || end === -1 || end <= start) return [];
  const inner = typeName.slice(start + 1, end);
  if (!inner.trim()) return [];
  return splitGenericParameters(inner).map((arg) => parseMoveTypeString(arg.trim()));
}

function parseMoveTypeString(typeName: string): SuiMoveNormalizedType {
  const lower = typeName.toLowerCase();
  if (lower === 'bool') return 'Bool';
  if (lower === 'u8') return 'U8';
  if (lower === 'u16') return 'U16';
  if (lower === 'u32') return 'U32';
  if (lower === 'u64') return 'U64';
  if (lower === 'u128') return 'U128';
  if (lower === 'u256') return 'U256';
  if (lower === 'address') return 'Address';
  if (lower === 'signer') return 'Signer';
  if (lower.startsWith('vector<') && typeName.endsWith('>')) {
    return { Vector: parseMoveTypeString(typeName.slice(7, -1).trim()) };
  }
  const tag = parseStructTag(typeName);
  return {
    Struct: {
      address: tag.address,
      module: tag.module,
      name: tag.name,
      typeArguments: tag.typeArgs,
    },
  };
}

function moveTypeToString(type: SuiMoveNormalizedType): string {
  if (typeof type === 'string') return type.toLowerCase();
  if ('Vector' in type) return `vector<${moveTypeToString(type.Vector)}>`;
  if ('TypeParameter' in type) return `$T${type.TypeParameter}`;
  if ('Reference' in type) return `&${moveTypeToString(type.Reference)}`;
  if ('MutableReference' in type) return `&mut ${moveTypeToString(type.MutableReference)}`;
  if ('Struct' in type) {
    const head = `${type.Struct.address.toLowerCase()}::${type.Struct.module}::${type.Struct.name}`;
    if (type.Struct.typeArguments.length === 0) return head;
    return `${head}<${type.Struct.typeArguments.map(moveTypeToString).join(',')}>`;
  }
  return JSON.stringify(type);
}

function structTagToString(tag: StructTag): string {
  const head = `${tag.address.toLowerCase()}::${tag.module}::${tag.name}`;
  if (tag.typeArgs.length === 0) return head;
  return `${head}<${tag.typeArgs.map(moveTypeToString).join(',')}>`;
}

function getStringField(fields: Record<string, any> | undefined, key: string): string | null {
  const value = fields?.[key];
  if (value == null) return null;
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'bigint') return value.toString();
  if (typeof value === 'object') {
    if (typeof value.bytes === 'string') return value.bytes;
    if (typeof value.hex === 'string') return value.hex;
  }
  const kind = value?.kind;
  if (!kind || kind.oneofKind !== 'stringValue') return null;
  return kind.stringValue ?? null;
}

function getEventFields(event: any): Record<string, any> | undefined {
  const jsonFields =
    event?.json?.kind?.oneofKind === 'structValue'
      ? event.json.kind.structValue?.fields
      : undefined;
  if (jsonFields) return jsonFields;
  if (event?.decodedFields && typeof event.decodedFields === 'object') {
    return event.decodedFields as Record<string, any>;
  }
  return undefined;
}

function computeDelta(before: string | null, after: string | null): string | null {
  if (!before || !after) return null;
  try {
    return (BigInt(after) - BigInt(before)).toString();
  } catch {
    return null;
  }
}

function extractSqrtChangesFromTransactions(
  transactions: any[],
  targetObjectIdRaw: string,
  checkpoint: string,
): SqrtChange[] {
  const target = targetObjectIdRaw.toLowerCase();
  const sqrtChanges: SqrtChange[] = [];
  for (const tx of transactions) {
    const txDigest = tx?.digest ?? null;
    const events = tx?.events?.events ?? [];
    for (const event of events) {
      const fields = getEventFields(event);
      const eventPoolId = getStringField(fields, 'pool_id')?.toLowerCase() ?? null;
      const eventPool = getStringField(fields, 'pool')?.toLowerCase() ?? null;
      const beforeSqrt = getStringField(fields, 'before_sqrt_price');
      const afterSqrt = getStringField(fields, 'after_sqrt_price');
      if (!beforeSqrt && !afterSqrt) continue;
      const matchedBy: Array<'event_pool_id' | 'event_pool'> = [];
      if (eventPoolId === target) matchedBy.push('event_pool_id');
      if (eventPool === target) matchedBy.push('event_pool');
      if (matchedBy.length === 0) continue;
      sqrtChanges.push({
        txDigest,
        checkpoint,
        objectId: target,
        beforeSqrtPrice: beforeSqrt,
        afterSqrtPrice: afterSqrt,
        sqrtDelta: computeDelta(beforeSqrt, afterSqrt),
        eventType: event?.eventType ?? event?.contents?.name ?? null,
        matchedBy,
      });
    }
  }
  return sqrtChanges;
}

const decoder = new MoveEventBcsDecoder(jsonRpcClient);

async function main() {
  if (!targetObjectId) {
    throw new Error('TARGET_OBJECT_ID is required in .env for pool filtering.');
  }

  mkdirSync(checkpointDir, { recursive: true });

  const debugRaw = process.env.DEBUG_RAW === '1';

  const call = client.subscriptionService.subscribeCheckpoints({
    readMask: {
      paths: [
        'sequence_number',
        'digest',
        'summary.timestamp',
        'transactions.digest',
        'transactions.events.events.contents',
        'transactions.events.events.json',
      ],
    },
  });

  for await (const msg of call.responses) {
    if (debugRaw) {
      console.log(
        '[raw]',
        JSON.stringify(msg, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
      );
    }

    const checkpoint = msg.checkpoint;
    const txs = checkpoint?.transactions ?? [];
    const decodeStartIso = formatLocalNow();
    const decodeStartMs = performance.now();
    const decodeTasks: Array<() => Promise<'decoded' | 'error' | 'skipped'>> = [];
    for (const tx of txs as any[]) {
      const events = tx?.events?.events ?? [];
      for (const event of events) {
        decodeTasks.push(async () => {
          if (event?.json) return 'skipped';
          const typeName = event?.contents?.name;
          const raw = event?.contents?.value;
          if (typeof typeName !== 'string' || !raw || typeof raw !== 'object') return 'skipped';
          const bytes = Uint8Array.from(
            Object.values(raw as Record<string, unknown>).filter((v): v is number => Number.isInteger(v)),
          );
          if (bytes.length === 0) return 'skipped';
          try {
            event.decodedFields = await decoder.decodeContents(typeName, bytes);
            event.eventType = event.eventType ?? typeName;
            return 'decoded';
          } catch (err) {
            event.decodeError = err instanceof Error ? err.message : String(err);
            return 'error';
          }
        });
      }
    }
    const decodeResults = await runWithConcurrency(decodeTasks, decodeConcurrency);
    const decodedEventCount = decodeResults.filter((s) => s === 'decoded').length;
    const decodeErrorCount = decodeResults.filter((s) => s === 'error').length;
    const decodeEndMs = performance.now();
    const decodeEndIso = formatLocalNow();

    const checkpointSeq = checkpoint?.sequenceNumber?.toString();
    const checkpointTime = formatCheckpointTimestamp(checkpoint?.summary?.timestamp);
    console.log(
      `[decode] checkpoint=${checkpointSeq ?? 'n/a'} checkpoint_time=${checkpointTime} start=${decodeStartIso} end=${decodeEndIso} duration_ms=${formatMs(decodeEndMs - decodeStartMs)} decoded=${decodedEventCount} errors=${decodeErrorCount}`,
    );
    if (checkpointSeq) {
      const outPath = join(checkpointDir, `checkpoint_${checkpointSeq}.json`);
      const payload = {
        fetchedAt: formatLocalNow(),
        checkpointSequenceNumber: checkpointSeq,
        checkpointDigest: checkpoint?.digest ?? null,
        checkpointTimestamp: formatCheckpointTimestamp(checkpoint?.summary?.timestamp),
        decodeStartAt: decodeStartIso,
        decodeEndAt: decodeEndIso,
        decodeDurationMs: Number(formatMs(decodeEndMs - decodeStartMs)),
        decodedEventCount,
        decodeErrorCount,
        transactions: txs,
      };
      writeFileSync(outPath, `${JSON.stringify(payload, replacer, 2)}\n`, 'utf8');
    }

    const sqrtChanges = extractSqrtChangesFromTransactions(
      txs as any[],
      targetObjectId,
      checkpointSeq ?? 'n/a',
    );
    for (const item of sqrtChanges) {
      console.log(
        `[sqrt] local_time=${formatLocalNow()} checkpoint=${checkpointSeq ?? 'n/a'} checkpoint_time=${formatCheckpointTimestamp(checkpoint?.summary?.timestamp)} tx=${item.txDigest ?? 'n/a'} event_type=${item.eventType ?? 'n/a'} pool=${targetObjectId} before=${item.beforeSqrtPrice ?? 'n/a'} after=${item.afterSqrtPrice ?? 'n/a'} delta=${item.sqrtDelta ?? 'n/a'} matched_by=${item.matchedBy.join(',')}`,
      );
    }
  }
}

main().catch(console.error);
