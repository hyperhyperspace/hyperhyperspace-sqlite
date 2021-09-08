export { SQLiteBackend } from './SQLiteBackend';
/*
import { HashedLiteral, MutableSet, Store } from '@hyper-hyper-space/core';
import { SQLiteBackend } from './SQLiteBackend';

const main = async () => {

    let back = new SQLiteBackend('testdb');
    let store = new Store(back);

    let back2 = new SQLiteBackend('testdb');
    let store2 = new Store(back2);

    let mut = new MutableSet<HashedLiteral>();

    await store.save(mut);


    let mut2 = await store2.load(mut.hash()) as MutableSet<HashedLiteral>;

    mut.add(new HashedLiteral('hola'));
    mut.add(new HashedLiteral('que'));
    mut.add(new HashedLiteral('tal'));

    await mut2.loadAndWatchForChanges();

    await store.save(mut);

    for (const opHash of mut._allAppliedOps) {
        const opHeader = await store.loadOpHeader(opHash);

        if (opHash !== opHeader?.opHash) {
            console.log('header hash mismatch');
        }
    }

    console.log(mut.size() === mut2.size());
    while (mut.size() !== mut2.size()) {
        await new Promise(r => setTimeout(r, 200));
        console.log(mut.size() === mut2.size());
    }
    
    store.close();
    store2.close();
}

main();
*/