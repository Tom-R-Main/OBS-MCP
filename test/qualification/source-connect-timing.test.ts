/* SPDX-License-Identifier: GPL-2.0-only */
// Actual Source OBS client; fake timers advance only its application timers.
// WebSocket data/handshake fences are real loopback events, not elapsed sleeps.
import {once} from 'node:events';
import {writeFileSync} from 'node:fs';
import {join} from 'node:path';
import WebSocket,{WebSocketServer} from 'ws';
import {expect,it,vi} from 'vitest';
import {OBSWebSocketClient} from '../../src/client.js';
it('Source shares connect ownership and starts the RPC timeout only after readySocket resolves',async()=>{
 vi.useFakeTimers({toFake:['setTimeout','clearTimeout']});
 const server=new WebSocketServer({host:'127.0.0.1',port:0});await once(server,'listening');const address=server.address();if(typeof address==='string'||!address)throw Error('fixture address');
 const frames:Record<string,any>[]=[];let accepts=0;let held:Record<string,any>|undefined;let signalFixture=()=>{};const fixtureSeen=new Promise<void>(resolve=>{signalFixture=resolve;});
 const accepted=once(server,'connection');server.on('connection',peer=>{accepts++;peer.on('message',raw=>{const frame=JSON.parse(raw.toString())as Record<string,any>;frames.push(frame);if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));else if(frame.op===6){const{name}= {name:frame.d.requestType};const answer=(data:Record<string,unknown>)=>peer.send(JSON.stringify({op:7,d:{requestId:frame.d.requestId,requestType:name,requestStatus:{result:true,code:100},responseData:data}}));if(name==='GetVersion')answer({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,availableRequests:['GetVersion','Fixture'],supportedImageFormats:['png'],platform:'macos',platformDescription:'fixture'});else if(name==='Fixture'&&!held){held=frame;signalFixture();}else if(name==='Fixture')answer({fresh:true});}});});
 const client=new OBSWebSocketClient(`ws://127.0.0.1:${address.port}`,null);const first=client.connect();const second=client.connect();expect(first).toBe(second);const connectResult=first.catch(error=>error);let peer:WebSocket|undefined;
 let settled=false;const result=client.sendRequest('Fixture',{},40).then(value=>{settled=true;return{value};},reason=>{settled=true;return{error:String((reason as Error).message)};});
 try{
  [peer]=await accepted as [WebSocket];expect(accepts).toBe(1);expect(client.isConnected()).toBe(false);
  const pending=()=>((client as unknown as {pendingRequests:Map<string,unknown>}).pendingRequests.size);
  expect(pending()).toBe(0);await vi.advanceTimersByTimeAsync(100);expect(settled).toBe(false);expect(pending()).toBe(0);
  peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));await first;await fixtureSeen;expect(pending()).toBe(1);expect(client.isConnected()).toBe(true);
  await vi.advanceTimersByTimeAsync(41);const timed=await result;expect(timed).toEqual({error:'Request Fixture timed out after 40ms'});expect(pending()).toBe(0);expect(client.isConnected()).toBe(true);expect(accepts).toBe(1);
  peer.send(JSON.stringify({op:7,d:{requestId:held!.d.requestId,requestType:'Fixture',requestStatus:{result:true,code:100},responseData:{late:true}}}));
  expect(await client.sendRequest('Fixture',{},40)).toEqual({fresh:true});expect(pending()).toBe(0);
  const receipt={case:'Source connect wait does not consume RPC timeout',virtualHandshakeWaitMs:100,rpcTimeoutMs:40,virtualAfterDispatchMs:41,sharedPromise:true,physicalConnections:accepts,result:timed,frames};
  if(process.env.SOURCE_CONNECT_EVIDENCE_DIR)writeFileSync(join(process.env.SOURCE_CONNECT_EVIDENCE_DIR,'source-connect-timing.json'),JSON.stringify(receipt,null,2)+'\n');
 }finally{vi.useRealTimers();await client.disconnect();await connectResult;for(const peer of server.clients)peer.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
it('Source connection deadline also covers held capability discovery without restarting after Identify',async()=>{
 vi.useFakeTimers({toFake:['setTimeout','clearTimeout']});
 const server=new WebSocketServer({host:'127.0.0.1',port:0});await once(server,'listening');const address=server.address();if(typeof address==='string'||!address)throw Error('fixture address');
 const accepted=once(server,'connection');let versionSeen=()=>{};const requested=new Promise<void>(resolve=>{versionSeen=resolve;});let count=0;
 server.on('connection',peer=>{count++;peer.on('message',raw=>{const frame=JSON.parse(raw.toString())as Record<string,any>;if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));if(frame.op===6&&frame.d.requestType==='GetVersion')versionSeen();});});
 const client=new OBSWebSocketClient(`ws://127.0.0.1:${address.port}`,null);const connecting=client.connect().then(()=>({ok:true}),reason=>({error:String((reason as Error).message)}));
 try{const[peer]=await accepted as [WebSocket];await vi.advanceTimersByTimeAsync(9000);peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));await requested;await vi.advanceTimersByTimeAsync(1001);const result=await connecting;expect(result).toEqual({error:'WebSocket connection timeout - OBS may not be running or WebSocket may be disabled'});expect(count).toBe(1);if(process.env.SOURCE_CONNECT_EVIDENCE_DIR)writeFileSync(join(process.env.SOURCE_CONNECT_EVIDENCE_DIR,'source-capability-deadline.json'),JSON.stringify({heldBeforeHelloMs:9000,heldCapabilityMs:1001,totalConnectDeadlineMs:10000,result,physicalConnections:count},null,2)+'\n');}
 finally{vi.useRealTimers();await client.disconnect();for(const peer of server.clients)peer.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
