import { Backend, BackendSearchParams, BackendSearchResults, Hash, HashedSet, HashReference, Literal, LiteralUtils, StoredOpHeader } from '@hyper-hyper-space/core';

import sqlite3 from 'sqlite3'
import { Database, open } from 'sqlite'
import { Logger, LogLevel } from '@hyper-hyper-space/core/dist/util/logging';
import { MultiMap } from '@hyper-hyper-space/core/dist/util/multimap';

class SQLiteBackend implements Backend {

    static readonly CLASS_KEY = 'class';
    static readonly REFERENCES_KEY = 'references';
    static readonly REFERENCING_CLASS_KEY = 'referencing-class';


    static log = new Logger(SQLiteBackend.name, LogLevel.INFO);
    static backendName = 'sqlite';

    static registered: MultiMap<string, SQLiteBackend>;

    static register(backend: SQLiteBackend) {
        SQLiteBackend.registered.add(backend.filename, backend);
    }

    static deregister(backend: SQLiteBackend) {
        SQLiteBackend.registered.delete(backend.filename, backend);
    }

    static getRegisteredInstances(filename: string): Set<SQLiteBackend> {
        return SQLiteBackend.registered.get(filename);
    }

    static async fireCallbacks(dbName: string, literal: Literal) {
        for (const backend of SQLiteBackend.getRegisteredInstances(dbName)) {
            if (backend.objectStoreCallback !== undefined) {
                await backend.objectStoreCallback(literal);
            }
        }
    }


    filename: string;
    dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>>;

    objectStoreCallback?: (literal: Literal) => Promise<void>

    constructor(filename: string) {

        this.filename = filename;

        this.dbPromise = open({
            filename: filename,
            driver: sqlite3.Database
          }).then(async (db:  Database<sqlite3.Database, sqlite3.Statement>) => {

                await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                                  'objects(' + 
                                    'sequence INTEGER PRIMARY KEY AUTOINCREMENT,' + 
                                    'hash TEXT,' +
                                    'literal TEXT NOT NULL,' +
                                    'timestamp INTEGER NOT NULL,' + 
                                    'op_height INTEGER,' +
                                    'prev_op_count INTEGER,' + 
                                    'header_hash TEXT' +
                                  ')');

                await db.exec('CREATE UNIQUE INDEX IF NOT EXISTS ' +
                                'object_hash ON objects(hash)')

                await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                                'object_lookup(' +
                                    'object_hash' +  
                                    'key TEXT,' +
                                    'hash TEXT' + 
                                ')');

                await db.exec('CREATE INDEX IF NOT EXISTS ' + 
                                'object_lookup_key ON object_lookup(key)');

                await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                                'terminal_ops(' + 
                                    'mutable_object_hash TEXT PRIMARY KEY,' + 
                                    'terminal_ops TEXT' + 
                                    'last_op TEXT' + 
                                ')');

                await db.exec('CREATE TABLE IF NOT EXISTS ' +
                                'op_headers(' + 
                                    'header_hash TEXT PRIMARY KEY,' + 
                                    'op_hash TEXT, ' + 
                                    'literal TEXT' +
                                ')');

                await db.exec('CREATE INDEX IF NOT EXISTS ' +
                              'op_header_op_hash ON op_headers(op_hash)');

                return db;
          });

          SQLiteBackend.register(this);
        
    }

    getBackendName(): string {
        return SQLiteBackend.backendName;
    }
    
    getName(): string {
        return this.filename;
    }

    async store(literal: Literal, opHeader?: StoredOpHeader): Promise<void> {
        let db = await this.dbPromise;

        let tries = 0
        let transaction = false;

        try {
            do {
                try {
                    await db.run('begin immediate');
                    transaction = true;
                } catch (e) {
                    tries = tries + 1;
                    console.log(e);
                    if (tries > 10) {
                        throw new Error('Cannot obtain write lock for database ' + this.filename);
                    } else {
                        await new Promise(r => setTimeout(r, 1000));
                    }
                }
        
            } while (!transaction)

            const hash = literal.hash;

            let result = await db.get<{hash: Hash}>('SELECT hash FROM objects WHERE hash=?', [hash]);

            if (result === undefined) {

                await db.run('INSERT INTO objects(hash, literal, timestamp, op_height, prev_op_count, header_hash) VALUES (?, ?, ?, ?, ?, ?)',
                [hash, JSON.stringify(literal), new Date().getTime().toString(), ])

                const idxEntries = new Array<string>();

                SQLiteBackend.addIdxEntry(idxEntries, SQLiteBackend.CLASS_KEY, literal.value._class);

                for (const dep of literal.dependencies) {
                    let reference = dep.path + '#' + dep.hash;
                    SQLiteBackend.addIdxEntry(idxEntries, SQLiteBackend.REFERENCES_KEY, reference);
                    let referencingClass = dep.className + '.' + dep.path + '#' + dep.hash;
                    SQLiteBackend.addIdxEntry(idxEntries, SQLiteBackend.REFERENCING_CLASS_KEY, referencingClass);
                }

                // caveat: if two different objects with the same hash (or more likely something in the stored process changes)
                //         are saved, the first one may leave some ghost idxEntries. We're not going to address that here.

                for (const key of idxEntries) {
                    await db.run('INSERT INTO object_lookup(key, hash) VALUES (?, ?)', [key, hash]);
                }

                const isOp = literal.value['_flags'].indexOf('op') >= 0;

                if (isOp) {
                    if (opHeader === undefined) {
                        throw new Error('Missing causal history received by backend while trying to store op ' + literal.hash);
                    }

                    await db.run('INSERT INTO op_headers(header_hash, op_hash, literal) VALUES (?, ?, ?) ' +
                                'ON CONFLICT(header_hash) DO UPDATE SET op_hash=excluded.op_hash, literal=excluded.literal',
                                [opHeader.literal.headerHash, hash, JSON.stringify(opHeader.literal)]);

                    const mutableHash = LiteralUtils.getFields(literal)['targetObject']['_hash'];

                    const prevOpHashes = HashedSet.elementsFromLiteral(LiteralUtils.getFields(literal)['prevOps']).map(HashReference.hashFromLiteral);

                    let terminalOpsRow = await db.get<[string, string]>('SELECT terminal_ops, last_op FROM terminal_ops WHERE mutable_object_hash=?', [mutableHash]);

                    let terminalOps: Array<Hash>;

                    if (terminalOpsRow === undefined) {
                        terminalOps = [hash];
                    } else {
                        terminalOps = JSON.parse(terminalOpsRow[0]);

                        for (const hash of prevOpHashes) {
                            let idx = terminalOps.indexOf(hash);
                            if (idx >= 0) {
                                terminalOps.splice(idx, 1);
                            }
                        }
        
                        if (terminalOps.indexOf(hash) < 0) { // this should always be true
                            terminalOps.push(hash);
                        }
                    }

                    await db.run('INSERT INTO terminal_ops(mutable_object_hash, terminal_ops, last_op) VALUES (?, ?, ?) ' +
                                'ON CONFICT(mutable_object_hash) DO UPDATE SET terminal_ops=excluded.terminal_ops, last_op=excluded.last_op',
                                [mutableHash, JSON.stringify(terminalOps), hash]);

                }

            }

            await db.run('commit');
    
        } catch (e) {
            await db.run('rollback');
        }
        

        
    }

    async load(hash: string): Promise<Literal | undefined> {
        
        let db = await this.dbPromise;

        let result = await db.get<{literal: string}>('SELECT literal FROM objects WHERE hash=?', [hash]);

        if (result === undefined) {
            return undefined;
        } else {
            return JSON.parse(result.literal);
        }
    }

    async loadOpHeader(opHash: string): Promise<StoredOpHeader | undefined> {
        
        let db = await this.dbPromise;
        
        let result = await db.get<{literal: string}>('SELECT literal FROM op_headers WHERE op_hash=?', [opHash]);

        if (result === undefined) {
            return undefined;
        } else {
            return {literal: JSON.parse(result.literal)};
        }

    }

    async loadOpHeaderByHeaderHash(headerHash: string): Promise<StoredOpHeader | undefined> {
        let db = await this.dbPromise;
        
        let result = await db.get<{literal: string}>('SELECT literal FROM op_headers WHERE header_hash=?', [headerHash]);

        if (result === undefined) {
            return undefined;
        } else {
            return {literal: JSON.parse(result.literal)};
        }
    }

    async loadTerminalOpsForMutable(hash: string): Promise<{ lastOp: string; terminalOps: string[]; } | undefined> {
        let db = await this.dbPromise;

        let result = await db.get<{terminal_ops: string}>('SELECT terminal_ops FROM terminal_ops WHERE mutable_object_hash=?', [hash]);

        if (result === undefined) {
            return undefined;
        } else {
            return JSON.parse(result.terminal_ops);
        }
    }

    searchByClass(className: string, params?: BackendSearchParams): Promise<BackendSearchResults> {
        return this.searchByIndex(SQLiteBackend.CLASS_KEY, className, params);
    }

    searchByReference(referringPath: string, referencedHash: string, params?: BackendSearchParams): Promise<BackendSearchResults> {
        return this.searchByIndex(SQLiteBackend.REFERENCES_KEY, 
                                  referringPath + '#' + referencedHash, params);
    }

    searchByReferencingClass(referringClassName: string, referringPath: string, referencedHash: string, params?: BackendSearchParams): Promise<BackendSearchResults> {
        return this.searchByIndex(SQLiteBackend.REFERENCING_CLASS_KEY, 
                                  referringClassName + '.' + referringPath + '#' + referencedHash, params);
    }

    private async searchByIndex(key: string, value: string, params?: BackendSearchParams) : Promise<BackendSearchResults> {

        const db = await this.dbPromise;

        const order = (params === undefined || params.order === undefined) ? 'asc' : params.order.toLowerCase();

        const queryParams:(number|string)[] = [key + '_' + value];

        let startClause = ''

        if (params?.start !== undefined) {
            if (order === 'asc') {
                startClause = 'AND serial > ? ';
            } else {
                startClause = 'AND serial < ? ';
            }

            queryParams.push(Number(params?.start));
        }

        let limit = '';

        if (params?.limit !== undefined) {
            limit = ' LIMIT ?';

            queryParams.push(params?.limit);
        }

        const results = await db.all<[{literal: string, serial: number}]>('SELECT literal, serial FROM objects INNER JOIN object_lookup ON objects.hash = object_lookup.hash WHERE key=? ' + startClause + 'ORDER BY serial ' + order + limit, queryParams);

        let searchResults = {} as BackendSearchResults;

        searchResults.items = [] as Array<Literal>;
        searchResults.start = undefined;
        searchResults.end   = undefined;


        for (const result of results) {
            searchResults.items.push(JSON.parse(result.literal));

            if (searchResults.start === undefined) {
                searchResults.start = result.serial + '';
            }
            searchResults.end = result.serial + '';
        }

        return searchResults;

    }

    close(): void {
        this.dbPromise.then((db:Database) => { db.close(); SQLiteBackend.deregister(this);} );
    }

    setStoredObjectCallback(objectStoreCallback: (literal: Literal) => Promise<void>): void {
        this.objectStoreCallback = objectStoreCallback;
    }

    async ready(): Promise<void> {
        await this.dbPromise;
    }

    private static addIdxEntry(idx:Array< string>, key: string, value: string): void {
        idx.push(key + '_' + value);
    }

}

export { SQLiteBackend };

