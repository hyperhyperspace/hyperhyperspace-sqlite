import { TestPeerSource } from './TestPeerSource';

import { AgentPod, MeshProxy } from '@hyper-hyper-space/core';

import { PeerInfo, PeerGroupAgent } from '@hyper-hyper-space/core';
import { ObjectDiscoveryPeerSource } from '@hyper-hyper-space/core';

import { PeerSource } from '@hyper-hyper-space/core';

import { RNGImpl } from '@hyper-hyper-space/core';
import { Identity, RSAKeyPair } from '@hyper-hyper-space/core';
import { LinkupManager } from '@hyper-hyper-space/core';
import { HashedLiteral } from '@hyper-hyper-space/core';
import { Mesh } from '@hyper-hyper-space/core';


class TestPeerGroupPods {
    
    static async generate(peerGroupId: string, activePeers: number, totalPeers: number, peerConnCount: number, network: 'wrtc'|'ws'|'mix' = 'wrtc', discovery:'linkup-discovery'|'no-discovery', basePort?: number): Promise<Array<AgentPod>> {

        let peers = new Array<PeerInfo>();

        for (let i=0; i<totalPeers; i++) {
            let id = Identity.fromKeyPair({'id':'peer' + i}, await RSAKeyPair.generate(1024));
            
            let host = LinkupManager.defaultLinkupServer;

            if (network === 'ws' || (network === 'mix' && i < totalPeers / 2)) {
                host = 'ws://localhost:' + (basePort as number + i);
            }

            let peer: PeerInfo = {
                endpoint: host  + '/' + new RNGImpl().randomHexString(128),
                identity: id,
                identityHash: id.hash()
            };

            peers.push(peer);
        }

        let peerSource = new TestPeerSource(peers);
        let pods = new Array<AgentPod>();

        for (let i=0; i<activePeers; i++) {

            let meshClient: Mesh | MeshProxy;
            let mesh: Mesh;
            


            meshClient = new Mesh();
            mesh = meshClient;

            let pod: AgentPod = mesh.pod;

            let peerSourceToUse: PeerSource = peerSource;

            let params: any = { maxPeers: peerConnCount, minPeers: peerConnCount, tickInterval: 1.5, peerConnectionAttemptInterval: 15, peerConnectionTimeout: 14 };

            if (discovery === 'linkup-discovery') {

                params.tickInterval = 1; // speed up peer group management to make up for peer discovery

                let object = new HashedLiteral(peerGroupId);


                meshClient.startObjectBroadcast(object, [LinkupManager.defaultLinkupServer], [peers[i].endpoint]);

                peerSourceToUse = new ObjectDiscoveryPeerSource(mesh, object, [LinkupManager.defaultLinkupServer], peers[i].endpoint, (ep: string) => peerSource.getPeerForEndpoint(ep));
            }

            let peerGroupAgent = new PeerGroupAgent(peerGroupId, peers[i], peerSourceToUse, params);
            pod.registerAgent(peerGroupAgent);
            pods.push(pod);
        }

        return pods;

    }

}

export { TestPeerGroupPods };