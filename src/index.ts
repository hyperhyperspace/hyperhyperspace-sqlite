export { SQLiteBackend } from './SQLiteBackend';

import { HashedLiteral, MutableSet, Store } from '@hyper-hyper-space/core';
import { SQLiteBackend } from './SQLiteBackend';

const main = async () => {

    let back = new SQLiteBackend(':memory:');
    let store = new Store(back);

    let mut = new MutableSet<HashedLiteral>();

    await store.save(mut);


    let mut2 = await store.load(mut.hash()) as MutableSet<HashedLiteral>;

    mut.add(new HashedLiteral('hola'));
    mut.add(new HashedLiteral('que'));
    mut.add(new HashedLiteral('tal'));

    await mut2.loadAndWatchForChanges();

    await store.save(mut);

    console.log(mut.size() === mut2.size());
    while (mut.size() !== mut2.size()) {
        await new Promise(r => setTimeout(r, 200));
        console.log(mut.size() === mut2.size());
    }
    


}

main();