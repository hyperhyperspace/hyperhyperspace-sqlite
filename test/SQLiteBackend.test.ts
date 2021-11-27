import { HashedLiteral, HashedObject, HeaderBasedSyncAgent, IdbBackend, Identity, MutableSet, PeerGroupAgent, Resources, RNGImpl, RSAKeyPair, StateGossipAgent, Store } from '@hyper-hyper-space/core';
import { SQLiteBackend } from '../src/SQLiteBackend';
import { PermissionTest } from './PermissionTest';
import { PermissionedFeatureSet } from './PermissionedFeatureSet';

import '@hyper-hyper-space/node-env';
import { TestPeerGroupPods } from './TestPeerGroupPods';
import { TestIdentity } from './TestIdentity';

describe('[SQL] SQLite backend', () => {
    test( '[SQL01] Basic undo w/ SQLite backend', async (done) => {

        let store = new Store(new SQLiteBackend(':memory:'));
        
        await testBasicUndoCycle(store);

        store.close();

        done();
    }, 30000);


    test( '[SQL02] Multi object undo cascade w/ SQLite backend', async (done) => {

        let store = new Store(new SQLiteBackend(':memory:'));
        
        await testMultiObjectUndoCascade (store);

        store.close();

        done();
    }, 30000);


    test('[SQL03] Causal history agent-based set sync in small peer group (wrtc), using SQLite', async (done) => {

        await syncInSmallPeerGroup(done, 'wrtc', undefined, true);

    }, 300000);
});
async function testBasicUndoCycle(store: Store) {

    const rootKeyPair = await RSAKeyPair.generate(1024);
    const rootId = Identity.fromKeyPair({role:'root'}, rootKeyPair);

    const adminKeyPair = await RSAKeyPair.generate(1024);
    const adminId = Identity.fromKeyPair({role:'admin'}, adminKeyPair);

    const userKeyPair = await RSAKeyPair.generate(1024);
    const userId = Identity.fromKeyPair({role:'user'}, userKeyPair);


    const temporaryUserKeyPair = await RSAKeyPair.generate(1024);
    const temporaryUserId = Identity.fromKeyPair({role:'user'}, temporaryUserKeyPair);
    await store.save(rootKeyPair);
    await store.save(rootId);

    await store.save(adminKeyPair);
    await store.save(adminId);

    await store.save(userKeyPair);
    await store.save(userId);

    const permissions = new PermissionTest();
    permissions.setAuthor(rootId);

    await permissions.addAdmin(adminId);
    await permissions.addUser(userId, adminId);
    await store.save(permissions);

    const permissionsClone = await store.load(permissions.hash()) as PermissionTest;
    await permissionsClone.loadAllChanges();

    await permissions.addUser(temporaryUserId, adminId);
    await store.save(permissions);

    permissions.watchForChanges(true);

    expect(permissions.isUser(userId));
    expect(permissions.isUser(temporaryUserId));

    expect(permissionsClone.isUser(userId));
    expect(!permissionsClone.isUser(temporaryUserId));

    await permissionsClone.removeAdmin(adminId);

    await store.save(permissionsClone);

    let i = 0;

    while (permissions.isUser(temporaryUserId) && i < 20) {
        await new Promise(r => setTimeout(r, 100));
    }

    expect(!permissions.isUser(temporaryUserId));
    expect(permissions.isUser(userId));

}


async function testMultiObjectUndoCascade(store: Store) {

    const rootKeyPair = await RSAKeyPair.generate(1024);
    const rootId = Identity.fromKeyPair({role:'root'}, rootKeyPair);

    const adminKeyPair = await RSAKeyPair.generate(1024);
    const adminId = Identity.fromKeyPair({role:'admin'}, adminKeyPair);

    await store.save(rootKeyPair);
    await store.save(rootId);

    await store.save(adminKeyPair);
    await store.save(adminId);

    const permissions = new PermissionTest();
    permissions.setAuthor(rootId);

    await permissions.addAdmin(adminId);
    await store.save(permissions);

    const permissionsClone = await store.load(permissions.hash()) as PermissionTest;
    await permissionsClone.loadAllChanges();

    permissions.watchForChanges(true);

    const features = new PermissionedFeatureSet(permissions);

    await store.save(features);

    features.watchForChanges(true);

    const useFeatureOpFail = features.useFeatureIfEnabled('anon-read', 'sample-usage-key');

    expect(useFeatureOpFail === undefined);

    features.enableFeature('anon-write', adminId)

    const useFeatureOp = features.useFeatureIfEnabled('anon-write', 'sample-usage-key');

    expect(useFeatureOp !== undefined);

    const featuresClone = await store.load(features.hash()) as PermissionedFeatureSet;
    featuresClone.users = permissionsClone;

    expect(featuresClone.useFeatureIfEnabled('anon-write', 'another-usage-key') === undefined);

    await store.save(features);
    await featuresClone.loadAllChanges();

    expect(featuresClone.useFeatureIfEnabled('anon-write', 'yet-another-usage-key') !== undefined);

    featuresClone.enableFeature('anon-read', adminId);

    await store.save(featuresClone);

    expect(featuresClone.isEnabled('anon-read') === true);

    permissions.removeAdmin(adminId);
    await store.save(permissions);

    await featuresClone.loadAllChanges();

    /*let i = 0;

    while (featuresClone.isEnabled('anon-read') && i < 100) {
        await new Promise(r => setTimeout(r, 100));
    }*/

    expect(featuresClone.isEnabled('anon-read') === false);
    expect(featuresClone.isEnabled('anon-write' ));

}

async function syncInSmallPeerGroup(done: () => void, network: 'wrtc'|'ws'|'mix' = 'wrtc', basePort?: number, useSQLite=false, bigElement=false) {

    const size = 3;
        
    let peerNetworkId = new RNGImpl().randomHexString(64);

    let pods = await TestPeerGroupPods.generate(peerNetworkId, size, size, size-1, network, 'no-discovery', basePort);

    let stores : Array<Store> = [];
    
    for (let i=0; i<size; i++) {

        useSQLite

        const peerNetwork = pods[i].getAgent(PeerGroupAgent.agentIdForPeerGroup(peerNetworkId)) as PeerGroupAgent;
        const store = new Store(useSQLite ? new SQLiteBackend(':memory:') : new IdbBackend('store-for-peer-' + peerNetwork.getLocalPeer().endpoint));
        stores.push(store);
        let gossip = new StateGossipAgent(peerNetworkId, peerNetwork);
        
        pods[i].registerAgent(gossip);
    }

    let id = await TestIdentity.getFirstTestIdentity();
    let kp = await TestIdentity.getFistTestKeyPair();
    
    let s = new MutableSet<HashedObject>();
    
    s.setAuthor(id);
    
    await stores[0].save(kp);
    await stores[0].save(s);

    for (let i=0; i<size; i++) {
        const peerGroupAgent = pods[i].getAgent(PeerGroupAgent.agentIdForPeerGroup(peerNetworkId)) as PeerGroupAgent;
        
        //let agent = new TerminalOpsSyncAgent(peerGroupAgent, s.hash(), stores[i], MutableSet.opClasses);
        let agent = new HeaderBasedSyncAgent(peerGroupAgent, s.hash(),  await Resources.create({store: stores[i]}), MutableSet.opClasses);
        let gossip = pods[i].getAgent(StateGossipAgent.agentIdForGossip(peerNetworkId)) as StateGossipAgent;
        gossip.trackAgentState(agent.getAgentId());
        //agent;
        pods[i].registerAgent(agent);
    }

    await s.add(id);
    
    await s.delete(id);
    
    await s.add(id);
    
    await s.delete(id);
    
    await s.add(id);

    if (bigElement) {
        let big = '';
        for (let i=0; i<1024 * 32; i++) {
            big = big + 'x';
        }
        s.add(new HashedLiteral(big));
    }
    //stores[size-1].save(sclone);

    //sclone.bindToStore();
    //sclone.loadAllOpsFromStore();

    //await stores[0].load(s.hash());

    await stores[0].save(s);

    //let ctx = s.toContext();

    //console.log(ctx.literals);

    //TestTopology.waitForPeers(swarms, size - 1);

    let meshReady = false;

    let count = 0;

    while (!meshReady && count < 1000) {
        await new Promise(r => setTimeout(r, 100));
        const meshAgent = pods[size-1].getAgent(PeerGroupAgent.agentIdForPeerGroup(peerNetworkId)) as PeerGroupAgent
        meshReady = meshAgent.getPeers().length === (size-1);
        //console.log(count + '. peers: ' + meshAgent.getPeers().length);
        count = count + 1;
    }


    let replicated = false;

    if (meshReady) {
        count = 0;

        while (!replicated && count < 1500) {

            await new Promise(r => setTimeout(r, 100));

            const sr = await stores[size-1].load(s.hash()) as MutableSet<Identity> | undefined;

            if (sr !== undefined) {
                
                
                await sr.loadAllChanges();
                replicated = sr.size() === (1 + (bigElement? 1 : 0));
                //for (const elmt of sr.values()) {
                    //console.log('FOUND ELMT:');
                    //console.log(elmt);
                //}
            }

            count = count + 1;
        }
    }



    //const meshAgent = pods[0].getAgent(PeerMeshAgent.agentIdForMesh(peerNetworkId)) as PeerMeshAgent
    //expect(meshAgent.getPeers().length).toEqual(size-1);

    for (const pod of pods) {
        pod.shutdown();
    }

    expect(meshReady).toBeTruthy();
    expect(replicated).toBeTruthy();

    for (let i=0; i<size; i++) {
        //stores[i].close();
    }

    done();
}