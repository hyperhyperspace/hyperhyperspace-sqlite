import { Backend, BackendSearchParams, BackendSearchResults, Hash, HashedSet, HashReference, Literal, LiteralUtils, RNGImpl, StoredOpHeader } from '@hyper-hyper-space/core';

import sqlite3 from 'sqlite3'
import { Database, open } from 'sqlite'
import { Logger, LogLevel } from '@hyper-hyper-space/core/dist/util/logging';
import { MultiMap } from '@hyper-hyper-space/core/dist/util/multimap';
import { Lock } from '@hyper-hyper-space/core/dist/util/concurrency';

type filename = string;

type dbMonitor = {
    alreadyCallbackedObjects: Set<Hash>;
    alreadyCallbackedMaxSeq?: number;
    interval: any;
    intervalLock: Lock;
    dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>>;
};

class SQLiteBackend implements Backend {

    static readonly CLASS_KEY = 'class';
    static readonly REFERENCES_KEY = 'references';
    static readonly REFERENCING_CLASS_KEY = 'referencing-class';


    static log = new Logger(SQLiteBackend.name, LogLevel.INFO);
    static backendName = 'sqlite';

    static activeBackends: MultiMap<filename, SQLiteBackend> = new MultiMap();
    static dbMonitors: Map<filename, dbMonitor> = new Map();

/*    static alreadyCallbackedObjects: MultiMap<filename, Hash> = new MultiMap();
    static maxSequenceAlreadyCallbacked: Map<filename, number> = new Map();
    static intervals: Map<filename, any> = new Map();
    static databases: Map<filename, Promise<Database<sqlite3.Database, sqlite3.Statement>>> = new Map();*/

    static open(filename: filename, fast: boolean): Promise<Database<sqlite3.Database, sqlite3.Statement>> {

        let dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>>;

        dbPromise = open({
            filename: filename,
            driver: sqlite3.Database
        }).then(async (db:  Database<sqlite3.Database, sqlite3.Statement>) => {

            let done = false;

            let count = 0;

            do {

                count = count + 1;

                try {

                    await db.run('PRAGMA journal_mode = WAL');

                    if (fast) {
                        await db.run('PRAGMA read_uncommitted = 1');
                        await db.run('PRAGMA synchronous = OFF');    
                    }
                    
                    await db.run('begin immediate');
                    
                    await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                    'objects(' + 
                      'sequence INTEGER PRIMARY KEY AUTOINCREMENT,' + 
                      'hash TEXT NOT NULL,' +
                      'literal TEXT NOT NULL,' +
                      'timestamp INTEGER NOT NULL' + 
                    ')');

                    await db.exec('CREATE UNIQUE INDEX IF NOT EXISTS ' +
                                    'object_hash ON objects(hash)')

                    await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                                    'object_lookup(' +
                                        'key TEXT,' +
                                        'hash TEXT' + 
                                    ')');

                    await db.exec('CREATE INDEX IF NOT EXISTS ' + 
                                    'object_lookup_key ON object_lookup(key)');

                    await db.exec('CREATE TABLE IF NOT EXISTS ' + 
                                    'terminal_ops(' + 
                                        'mutable_object_hash TEXT PRIMARY KEY,' + 
                                        'terminal_ops TEXT,' + 
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

                    await db.exec('commit');
                
                    done = true;
                
                } catch (e: any) {

                    try {
                        db.exec('rollback');
                    } catch (e) {

                    }
                    
                    if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                        await new Promise(r => setTimeout(r, 25));
                    } else {
                        console.log('init work')
                        console.log(e);
                        
                        throw e;
                    }
                }

                if (count % 100 === 0) {
                    console.log('WARN: waited for 2.5 seconds (init tx) on SQLite database ' + filename);
                }
        
            } while (!done);

            return db;
        });

        return dbPromise;
        
    }

    static registerDbMonitorFor(backend: SQLiteBackend, fast: boolean): Promise<void> {

        const filename = backend.filename as filename;

        let dbMonitor = SQLiteBackend.dbMonitors.get(filename);

        if (dbMonitor === undefined) {
        
            const dbPromise = SQLiteBackend.open(filename, fast).then(async (db: Database<sqlite3.Database, sqlite3.Statement>) => {
                    
                        let done = false;

                        let count = 0;

                        do {

                            count = count + 1;

                            try {
            
                                const dbMonitor = SQLiteBackend.dbMonitors.get(filename);

                                if (dbMonitor !== undefined) {
                                    let result = await db.get<{max_seq: number}>('SELECT max(sequence) as max_seq FROM objects');
    
                                    if (result !== undefined) {
                                        dbMonitor.alreadyCallbackedMaxSeq = result.max_seq;
                                    }
        
                                }

                                done = true;

                            } catch (e: any) {
                                if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                                    await new Promise(r => setTimeout(r, 25));
                                } else {
                                    console.log('max sequence lookup')
                                    console.log(e);
                                    
                                    throw e;
                                }
                            }

                            if (count % 100 === 0) {
                                console.log('WARN: waited for 2.5 seconds (max sequence lookup) on SQLite database ' + backend.filename);
                            }
                        
                        } while (!done);

                        return db;
                    });
                    
                        
            const interval = setInterval(async () => {

                        const dbMonitor = SQLiteBackend.dbMonitors.get(filename);

                        if (dbMonitor !== undefined) {

                            if (dbMonitor.intervalLock.acquire()) {

                                try {

                                    const db = await dbMonitor.dbPromise;

                                    let query = 'SELECT hash, literal, sequence FROM objects';
                                    let params = [];
            
                                    const max = dbMonitor.alreadyCallbackedMaxSeq;
            
                                    if (max !== undefined) {
                                        query = query + ' WHERE sequence > ?'
                                        params.push(max);
                                    }
            
                                    query = query + ' ORDER BY sequence ASC';

                                    const alreadyCallbackedObjects = dbMonitor.alreadyCallbackedObjects;

                                    try {

                                        const results = await db.all<[{hash: Hash, literal: string, sequence: number}]>(query, params);
                    
                                        for (const result of results) {
                                            const literal = JSON.parse(result.literal);
                                            if (!alreadyCallbackedObjects.has(result.hash)) {
                                                await SQLiteBackend.fireCallbacks(filename, literal);
                                            } else {
                                                alreadyCallbackedObjects.delete(result.hash);
                                            }

                                            dbMonitor.alreadyCallbackedMaxSeq = result.sequence;
                                        }
                                        
                                        
                                    } catch (e: any) {
                                        if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                                            // ok, we'll do it next time
                                        } else {
                                            console.log('callbacks interval')
                                            console.log(e);
                    
                                            throw e;
                                        }
                                    }
                                } finally {
                                    dbMonitor.intervalLock.release()
                                }
                            }

                        }

                    

            }, 500);
                   
            dbMonitor = { 
                dbPromise: dbPromise,
                alreadyCallbackedMaxSeq: undefined,
                alreadyCallbackedObjects: new Set<Hash>(),
                interval: interval,
                intervalLock: new Lock()
            };

            SQLiteBackend.dbMonitors.set(filename, dbMonitor); 
        }

        SQLiteBackend.activeBackends.add(filename, backend);

        return dbMonitor.dbPromise.then(() => {});
    }

    static deregisterDbMonitorFor(backend: SQLiteBackend) {

        const filename = backend.filename as filename;

        SQLiteBackend.activeBackends.delete(filename, backend);

        if (SQLiteBackend.activeBackends.get(filename).size === 0) {

            const dbMonitor = SQLiteBackend.dbMonitors.get(filename);

            SQLiteBackend.dbMonitors.delete(filename);

            if (dbMonitor !== undefined) {
                clearInterval(dbMonitor.interval);
                dbMonitor.dbPromise.then((db: Database) => db.close());
            }            
        }
    }

    static getRegisteredInstances(filename: filename): Set<SQLiteBackend> {
        return SQLiteBackend.activeBackends.get(filename);
    }

    static async fireCallbacks(dbName: string, literal: Literal) {

        for (const backend of SQLiteBackend.getRegisteredInstances(dbName)) {
            if (backend.objectStoreCallback !== undefined) {
                await backend.objectStoreCallback(literal);
            }
        }
    }


    filename?: string;
    memoryDbId?: string;
    dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>>;
    writeLock: Lock;

    fast: boolean;

    objectStoreCallback?: (literal: Literal) => Promise<void>

    constructor(filename: string, fast=false) {

        if (filename === ':memory:') {
            this.memoryDbId = new RNGImpl().randomHexString(128);
            this.dbPromise = SQLiteBackend.open(filename, fast);
        } else {
            this.filename = filename;
            this.dbPromise = SQLiteBackend.registerDbMonitorFor(this, fast).then(() => SQLiteBackend.open(filename, fast));
        }

        this.writeLock = new Lock();
        this.fast = fast;
    }

    getBackendName(): string {
        return SQLiteBackend.backendName;
    }
    
    getName(): string {
        return (this.filename || this.memoryDbId) as string;
    }

    async store(literal: Literal, opHeader?: StoredOpHeader): Promise<void> {

        let db = await this.dbPromise;

        let done = false;

        let removeImmediateIfRollback = false;

        let count = 0;

        do {

            count = count + 1;

            if (this.writeLock.acquire()) {
                try {

                    await db.run('begin immediate');

                    const hash = literal.hash;

                    let result = await db.get<{hash: Hash}>('SELECT hash FROM objects WHERE hash=?', [hash]);
        
                    if (result === undefined) {
                        
                        if (this.filename !== undefined) {
                            SQLiteBackend.dbMonitors.get(this.filename)?.alreadyCallbackedObjects.add(hash);
                            removeImmediateIfRollback = true;
                        }

                        await db.run('INSERT INTO objects(hash, literal, timestamp) VALUES (?, ?, ?)',
                        [hash, JSON.stringify(literal), new Date().getTime().toString()]);
        
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
        
                            let terminalOpsResult = await db.get<{terminal_ops: string}>('SELECT terminal_ops FROM terminal_ops WHERE mutable_object_hash=?', [mutableHash]);
        
                            let terminalOps: Array<Hash>;
        
                            if (terminalOpsResult === undefined) {
                                terminalOps = [hash];
                            } else {
                                terminalOps = JSON.parse(terminalOpsResult.terminal_ops);
        
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
                                        'ON CONFLICT(mutable_object_hash) DO UPDATE SET terminal_ops=excluded.terminal_ops, last_op=excluded.last_op',
                                        [mutableHash, JSON.stringify(terminalOps), hash]);
        
                        }
        
                    }
        
                    await db.run('commit');

                    done = true;
                } catch (e: any) {

                    if (removeImmediateIfRollback && this.filename !== undefined) {
                        SQLiteBackend.dbMonitors.get(this.filename)?.alreadyCallbackedObjects.delete(literal.hash);
                    }

                    try {
                        await db.run('rollback');
                    } catch (e) {

                    }
        
                    if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                        await new Promise(r => setTimeout(r, 25));                   
                    } else {
                        console.log('SQLite store error!')
                        console.log(e);
                        throw e;
                    }
                } finally {
                    this.writeLock.release();
                }
            } else {
                await new Promise(r => setTimeout(r, 50));
            }
            if (count % 100 === 0) {
                console.log('WARN: waited for 5 seconds (store) on SQLite database ' + this.filename);
            }
    
        } while (!done)

        if (this.filename !== undefined) {
            await SQLiteBackend.fireCallbacks(this.filename, literal);
        } else if (this.objectStoreCallback !== undefined) {
            await this.objectStoreCallback(literal);
        }
        
    }

    async load(hash: string): Promise<Literal | undefined> {

        let result = await this.dbGetWithWaiting<{literal: string}>('SELECT literal FROM objects WHERE hash=?', [hash]);

        if (result === undefined) {
            return undefined;
        } else {
            return JSON.parse(result.literal);
        }
    }

    async loadOpHeader(opHash: string): Promise<StoredOpHeader | undefined> {
        
        let result = await this.dbGetWithWaiting<{literal: string}>('SELECT literal FROM op_headers WHERE op_hash=?', [opHash]);

        if (result === undefined) {
            return undefined;
        } else {
            return {literal: JSON.parse(result.literal)};
        }

    }

    async loadOpHeaderByHeaderHash(headerHash: string): Promise<StoredOpHeader | undefined> {
        
        let result = await this.dbGetWithWaiting<{literal: string}>('SELECT literal FROM op_headers WHERE header_hash=?', [headerHash]);

        if (result === undefined) {
            return undefined;
        } else {
            return {literal: JSON.parse(result.literal)};
        }
    }

    async loadTerminalOpsForMutable(hash: string): Promise<{ lastOp: string; terminalOps: string[]; } | undefined> {

        let result = await this.dbGetWithWaiting<{terminal_ops: string, last_op: string}>('SELECT terminal_ops FROM terminal_ops WHERE mutable_object_hash=?', [hash]);

        if (result === undefined) {
            return undefined;
        } else {
            return {terminalOps: JSON.parse(result.terminal_ops), lastOp: result.last_op};
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

        const order = (params === undefined || params.order === undefined) ? 'asc' : params.order.toLowerCase();

        const queryParams:(number|string)[] = [key + '_' + value];

        let startClause = ''

        if (params?.start !== undefined) {
            if (order === 'asc') {
                startClause = 'AND sequence > ? ';
            } else {
                startClause = 'AND sequence < ? ';
            }

            queryParams.push(Number(params?.start));
        }

        let limit = '';

        if (params?.limit !== undefined) {
            limit = ' LIMIT ?';

            queryParams.push(params?.limit);
        }

        const results = await this.dbAllWithWaiting<[{literal: string, sequence: number}]>('SELECT literal, sequence FROM objects INNER JOIN object_lookup ON objects.hash = object_lookup.hash WHERE key=? ' + startClause + 'ORDER BY sequence ' + order + limit, queryParams);

        let searchResults = {} as BackendSearchResults;

        searchResults.items = [] as Array<Literal>;
        searchResults.start = undefined;
        searchResults.end   = undefined;

        for (const result of results) {
            searchResults.items.push(JSON.parse(result.literal));

            if (searchResults.start === undefined) {
                searchResults.start = result.sequence + '';
            }
            searchResults.end = result.sequence + '';
        }

        return searchResults;

    }

    close(): void {
        if (this.filename !== undefined) {
            SQLiteBackend.deregisterDbMonitorFor(this);
        }
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

    private async dbGetWithWaiting<T>(q: string, params: any) : Promise<T|undefined> {

        const db = await this.dbPromise;

        let result: T|undefined = undefined;
        let run = false;

        let count=0;

        while (!run) {
            count = count + 1;
            try {
                result = await db.get<T>(q, params);
                run = true;
            } catch (e: any) {
                if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                    await new Promise(r => setTimeout(r, 25));
                } else {
                    console.log('get')
                    console.log(e);

                    throw e;
                }
            }

            if (count % 200 === 0) {
                console.log('WARN: waited for 5 seconds (get) on SQLite database ' + this.filename);
            }
        }
        
        return result;
    }

    private async dbAllWithWaiting<T>(q: string, params: any) : Promise<T> {

        const db = await this.dbPromise;

        let result: T|undefined = undefined;

        let count=0;

        while (result === undefined) {

            count = count + 1;

            try {
                result = await db.all<T>(q, params);
            } catch (e: any) {
                if (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_LOCKED') {
                    await new Promise(r => setTimeout(r, 25));   
                } else {
                    console.log('all')
                    console.log(e);

                    throw e;
                }
            }

            if (count % 200 === 0) {
                console.log('WARN: waited for 5 seconds on (read all) SQLite database ' + this.filename);
            }
        }
        
        return result;
    }

}

export { SQLiteBackend };

