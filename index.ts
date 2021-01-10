import fs from "fs";
import readline from "readline";
import R from "ramda";
import { promisify } from "util";
import { file } from 'tmp-promise';
import { Readable, pipeline } from "stream";
import { Buffer } from "buffer";

export const ITEM_KEY = Symbol();

export type ItemKey = string | number;

/**
 * JOIN
 */

export function join<S extends JoinSpec>(spec: S): JoinFn<S> {
  const sourceSpecs: Record<keyof S, SourceSpec<any, InferJoinKey<S>>> = R.pickBy(v => v !== ITEM_KEY, spec);

  const indexers = Object.entries(sourceSpecs).map(
    ([sourceKey, sourceSpec]) => [sourceKey, index(sourceSpec)] as const
  );

  function emptyItem(key: InferJoinKey<S>): any {
    return R.map(value => value === ITEM_KEY ? key : [], spec);
  }

  return async input => {
    /**
     * indexer(...) will not throw synchronously, so it's safe to be located before the try-catch statement
     */
    const indexPromises = indexers.map(([sourceKey, indexer]) => (
      indexer(input[sourceKey as keyof JoinInput<S>]).then(index => [sourceKey, index] as const)
    ));

    async function cleanUp() {
      await Promise.all(
        indexPromises.map(p => p.then(([, index]) => index.cleanUp()).catch())
      );
    }

    try {
      const indices = await Promise.all(indexPromises)

      const allKeys = new Set(
        R.chain(index => [...index[1].keys()], indices)
      );
      
      const joinIndex: JoinResult<S> = {
        cleanUp,

        async get(key: InferJoinKey<S>) {
          const result = emptyItem(key);

          if (!allKeys.has(key)) {
            return result;
          }

          await Promise.all(
            indices.map(
              ([sourceKey, sourceIndex]) => sourceIndex.get(key).then(sourceItems => {
                result[sourceKey] = sourceItems;
              })
            )
          );

          return result;
        },

        has(key: InferJoinKey<S>) {
          return allKeys.has(key);
        },

        keys() {
          return allKeys;
        },

        async *[Symbol.asyncIterator](): AsyncIterator<any> {
          for (const key of allKeys) {
            yield joinIndex.get(key);
          }
        },
      };

      return joinIndex;
    } catch (e) {
      await cleanUp();
      throw e;
    }
  };
}

export type JoinFn<S extends JoinSpec = JoinSpec> = (input: JoinInput<S>) => Promise<JoinResult<S>>;

export interface JoinSpec<K extends ItemKey = ItemKey> {
  readonly [field: string]: SourceSpec<any, K> | typeof ITEM_KEY
}

export type JoinInput<S extends JoinSpec = JoinSpec> = {
  readonly [K in JoinSourceKeys<S>]: S[K] extends SourceSpec<infer T> ? SourceInput<T> : never
};

export type JoinResult<S extends JoinSpec = JoinSpec> = Index<JoinOutputItem<S>, InferJoinKey<S>>;

export type JoinOutputItem<S extends JoinSpec = JoinSpec> = {
  readonly [K in keyof S]:
    S[K] extends SourceSpec<infer T> ? T[]
    : S[K] extends typeof ITEM_KEY ? InferJoinKey<S>
    : never
};

/**
 * INDEX
 */

export function index<T, K extends ItemKey>(spec: SourceSpec<T, K>): IndexFn<T, K> {
  // todo - validate spec here

  return async input => {
    const index: Map<K, ItemFilePosition[]> = new Map();

    function itemListener(item: ItemInfo<T>) {
      const key = spec(item.data);
      if (index.has(key)) {
        index.get(key)!.push(item.position);
      } else {
        index.set(key, [item.position]);
      }
    }

    let filePath: fs.PathLike;
    if (isAsyncIterable(input)) {
      filePath = await writeToTmp(input, itemListener);
    } else {
      filePath = input;
      await readFromFile(input, itemListener);
    }

    async function cleanUp() {
      if (isAsyncIterable(input)) {
        await promisify(fs.unlink)(filePath);
      }
    }

    try {
      const fd = await promisify(fs.open)(filePath, 'r');
      async function readItem(position: ItemFilePosition): Promise<T> {
        const buffer = Buffer.alloc(position.length);
        await read(fd, buffer, 0, position.length, position.offset);
        const itemString = buffer.toString('utf8');
        return JSON.parse(itemString);
      }

      return {
        cleanUp,

        async get(key: K) {
          const itemPositions = index.get(key) ?? [];
          const items = itemPositions.map(readItem);
          return Promise.all(items);
        },

        has(key: K) {
          return index.has(key);
        },

        keys() {
          return index.keys();
        },

        async *[Symbol.asyncIterator](): AsyncIterator<T> {
          for await (const line of readJsonLinesFromFile(filePath)) {
            yield JSON.parse(line);
          }
        },
      };
    } catch (e) {
      cleanUp();
      throw e;
    }
  };
}

export type IndexFn<T = any, K extends ItemKey = ItemKey> = (input: SourceInput<T>) => Promise<IndexResult<T, K>>;

export type IndexResult<T = any, K extends ItemKey = ItemKey> = Index<T, K>;

export interface Index<T = any, K extends ItemKey = ItemKey> {
  cleanUp(): Promise<void>
  keys(): Iterable<K>
  get(key: K): Promise<Array<T>>
  has(key: K): boolean
  [Symbol.asyncIterator](): AsyncIterator<T>
}

/**
 * INTERNAL
 */

type SourceSpec<T = any, K extends ItemKey = ItemKey> = (item: T) => K;

type SourceInput<T = any> = AsyncIterable<T> | fs.PathLike;

type InferJoinKey<S extends JoinSpec = JoinSpec> = S extends JoinSpec<infer K> ? K : never;

type JoinSourceKeys<S extends JoinSpec = JoinSpec> = {
  readonly [K in keyof S]: S[K] extends SourceSpec ? K : never
}[keyof S];

type ItemInfoListener<T> = (item: ItemInfo<T>) => void;

interface ItemInfo<T> {
  data: T
  position: ItemFilePosition
}

interface ItemFilePosition {
  offset: number
  length: number
}

async function writeToTmp<T>(items: AsyncIterable<T>, listener: ItemInfoListener<T>): Promise<string> {
  const {fd, path} = await file({
    keep: false,
    postfix: '.jsonl',
  });

  try {
    const writable = fs.createWriteStream('', { fd });

    const bytes = Readable.from(
      (async function* () {
        let offset = 0;
        for await (const item of items) {
          const jsonString = JSON.stringify(item);
          const buffer = Buffer.from(`${jsonString}\n`, 'utf8');
          listener({
            data: item,
            position: {
              offset,
              length: buffer.byteLength,
            },
          });
          yield buffer;
          offset += buffer.byteLength;
        }
      })()
    );

    await promisify(pipeline)(bytes, writable);

    return path;
  } catch (e) {
    await promisify(fs.unlink)(path);
    throw e;
  }
}

async function readFromFile<T>(path: fs.PathLike, listener: ItemInfoListener<T>): Promise<void> {
  let offset = 0;
  for await (const line of readJsonLinesFromFile(path)) {
    const buffer = Buffer.from(line, 'utf8');
    listener({
      data: JSON.parse(line),
      position: {
        offset,
        length: buffer.byteLength + 1, // +1 to account for newline
      },
    });
    offset += buffer.byteLength + 1; // +1 to account for newline
  }
}

function readJsonLinesFromFile(path: fs.PathLike): AsyncIterable<string> {
  const fileStream = fs.createReadStream(path, {encoding: 'utf8'});

  return readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
}

function isAsyncIterable<T>(value: any): value is AsyncIterable<T> {
  return (value as any)[Symbol.asyncIterator] ? true : false;
}

const read = promisify(fs.read);
