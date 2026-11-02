// SPDX-License-Identifier: GPL-2.0-only
// Evidence only: actual compiled Source callbacks + actual Source client on loopback.
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {WebSocketServer} from 'ws';
process.env.OBS_MCP_LOG_LEVEL='silent';
const watchdog=setTimeout(()=>{console.error('Source callback fixture watchdog expired');process.exit(124);},15000);watchdog.unref();
const root=process.cwd();
const {OBSWebSocketClient}=await import(pathToFileURL(join(root,'build/client.js')).href);
const record=await import(pathToFileURL(join(root,'build/tools/record.js')).href);
const outputs=await import(pathToFileURL(join(root,'build/tools/outputs.js')).href);
const {ensureStructuredContent}=await import(pathToFileURL(join(root,'build/tools/results.js')).href);
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function bounded(promise,label){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label)),4000)})]);}finally{clearTimeout(timer);}}
const body=(name,raw,expected)=>({name,raw,expected});
const err='Error stopping recording: Cannot convert object to primitive value';
const cases=[
 body('response-absent',undefined,'undefined'),body('response-null','null','undefined'),body('response-empty-object','{}','undefined'),
 body('response-false','false','undefined'),body('response-zero','0','undefined'),body('response-empty-string','""','undefined'),
 body('response-array','[]','undefined'),body('response-string','"value"','undefined'),
 body('response-own-toString-only','{"toString":null}','undefined'),body('response-own-proto','{"__proto__":{"outputPath":"not-inherited"}}','undefined'),
 body('path-null','{"outputPath":null}','null'),body('path-false','{"outputPath":false}','false'),body('path-true','{"outputPath":true}','true'),
 body('path-empty','{"outputPath":""}',''),body('path-string','{"outputPath":"clip.mkv"}','clip.mkv'),body('path-unicode','{"outputPath":"λ/🎬.mkv"}','λ/🎬.mkv'),
 body('path-negative-zero','{"outputPath":-0.0}','0'),body('path-u64-max','{"outputPath":18446744073709551615}','18446744073709552000'),
 body('path-js-tie','{"outputPath":144713588892437.625}','144713588892437.62'),body('path-min-subnormal','{"outputPath":5e-324}','5e-324'),
 body('path-empty-array','{"outputPath":[]}',''),body('path-null-array','{"outputPath":[null]}',''),
 body('path-nested-array','{"outputPath":["a",null,["b",null,["c"]],{}]}','a,,b,,c,[object Object]'),
 body('path-array-booleans-numbers','{"outputPath":[false,true,0,-0.0,18446744073709551615]}','false,true,0,0,18446744073709552000'),
 body('path-ordinary-object','{"outputPath":{"value":2}}','[object Object]'),body('path-own-valueOf','{"outputPath":{"valueOf":null}}','[object Object]'),
 body('path-proto-data','{"outputPath":{"__proto__":{"toString":null}}}','[object Object]'),
 body('path-symbol-name-data','{"outputPath":{"Symbol.toPrimitive":"ignored"}}','[object Object]'),
 body('path-toJSON-data','{"outputPath":{"toJSON":false}}','[object Object]'),
 ...['null','false','0','"text"','[]','{}'].map((value,index)=>({name:`path-own-toString-${index}`,raw:`{"outputPath":{"toString":${value}}}`,throws:true})),
 {name:'path-own-toString-valueOf',raw:'{"outputPath":{"toString":null,"valueOf":3}}',throws:true},
 {name:'path-own-toString-nested-array',raw:'{"outputPath":["before",[{"toString":false}],"after"]}',throws:true},
 {name:'path-nested-object-harmless-key',raw:'{"outputPath":{"child":{"toString":null}}}',expected:'[object Object]'},
 {name:'record-rejected',reject:{code:500,comment:'controlled peer detail'}},
];
const namedCases=[
 ...[undefined,'null','false','[]','{"toString":null}','{"outputPath":{"toString":null}}'].map((raw,index)=>({name:`named-ignored-response-${index}`,raw,outputName:index===0?'':index===1?"a'b\nλ":'Named'})),
 {name:'named-rejected',outputName:'Named',reject:{code:500,comment:'controlled peer detail'}},
];
const server=new WebSocketServer({host:'127.0.0.1',port:0});await once(server,'listening');
const url=`ws://127.0.0.1:${server.address().port}`;let connectionCount=0,activeCase;
const history=[],results=[];
server.on('connection',socket=>{connectionCount++;socket.on('error',()=>{});socket.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0',obsStudioVersion:'32.2.2'}}));socket.on('message',bytes=>{
 const frame=JSON.parse(bytes.toString());if(frame.op===1){socket.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));return;}if(frame.op===3)return;
 assert.equal(frame.op,6);history.push({case:activeCase?.name??'bootstrap',frame});
 if(frame.d.requestType==='GetVersion'){socket.send(JSON.stringify({op:7,d:{requestId:frame.d.requestId,requestType:'GetVersion',requestStatus:{result:true,code:100},responseData:{obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,availableRequests:['GetVersion','StopRecord','StopOutput'],supportedImageFormats:['png'],platform:'macos'}}}));return;}
 assert(activeCase,'effect has no active fixture row');assert.equal(frame.d.requestType,activeCase.tool==='obs-stop-record'?'StopRecord':'StopOutput');
 const status=activeCase.reject?{result:false,code:activeCase.reject.code,comment:activeCase.reject.comment}:{result:true,code:100};
 const head=JSON.stringify({requestId:frame.d.requestId,requestType:frame.d.requestType,requestStatus:status});
 // Keep lexical JSON untouched until the actual Source client parses it.
 const raw='{"op":7,"d":'+head.slice(0,-1)+(activeCase.raw===undefined?'':',"responseData":'+activeCase.raw)+'}}';
 activeCase.ack={requestType:frame.d.requestType,status,raw};socket.send(raw);
 });});
const client=new OBSWebSocketClient(url,null);
const actualSendRequest=client.sendRequest.bind(client);
client.sendRequest=async(...args)=>{try{return await actualSendRequest(...args);}catch(error){if(activeCase)activeCase.transportErrorMessage=error instanceof Error?error.message:String(error);throw error;}};
const callbacks=new Map();
const capture={registerTool(name,definition,callback){callbacks.set(name,{definition,callback});return {};}};
record.initialize(capture,client);outputs.initialize(capture,client);
let failure;
try{
 await bounded(client.connect(),'fixture client connect stalled');await bounded(client.sendRequest('GetVersion'),'explicit capability barrier stalled');
 for(const row of [...cases.map(c=>({...c,tool:'obs-stop-record'})),...namedCases.map(c=>({...c,tool:'obs-stop-output'}))]){
  activeCase=row;const before=history.length;const args=row.tool==='obs-stop-output'?{outputName:row.outputName}:{};
  const rawCallbackResult=await bounded(callbacks.get(row.tool).callback(args),'actual callback did not settle');const structuredResult=ensureStructuredContent(rawCallbackResult);const trace=history.slice(before);
  assert.equal(trace.length,1);assert.equal(trace[0].frame.op,6);assert(row.ack);
  if(row.tool==='obs-stop-record')assert(!Object.hasOwn(trace[0].frame.d,'requestData'));
  else assert.deepEqual(trace[0].frame.d.requestData,{outputName:row.outputName});
  let expected;
  if(row.reject)expected={content:[{type:'text',text:row.tool==='obs-stop-record'?'Error stopping recording: OBS request StopRecord failed with code 500: controlled peer detail':'Stop Output failed: OBS request StopOutput failed with code 500: controlled peer detail'}],isError:true};
  else if(row.throws)expected={content:[{type:'text',text:err}],isError:true};
  else {const message=row.tool==='obs-stop-record'?`Recording stopped, saved to: ${row.expected}`:`Output '${row.outputName}' stopped`;expected={content:[{type:'text',text:message}],structuredContent:{message}};}
  assert.deepEqual(structuredResult,expected,row.name);
  results.push({name:row.name,tool:row.tool,responseDataRaw:row.raw??'<absent>',args,ack:row.ack,trace,rawCallbackResult,structuredResult,knownTransportAccepted:!row.reject,callbackFormattingError:row.throws===true,transportErrorMessage:row.transportErrorMessage});
 }
} catch(error){failure=String(error.stack??error);throw error;}
finally{
 activeCase=undefined;await bounded(client.disconnect(),'client disconnect did not join');for(let i=0;i<80&&server.clients.size;i++)await pause(5);assert.equal(server.clients.size,0);await bounded(new Promise((resolve,reject)=>server.close(e=>e?reject(e):resolve())),'listener close did not join');
 const report={scope:'Actual compiled Source registration callbacks plus real current Source WebSocket client and exact loopback response lexemes. ensureStructuredContent applied. No MCP transport/input-output schema/global tool policy or native callback public parity execution.',cases:results.length,recordCases:cases.length,namedCases:namedCases.length,knownAcceptedFormattingErrors:results.filter(r=>r.knownTransportAccepted&&r.callbackFormattingError).length,connections:connectionCount,cleanup:{clientDisconnected:true,openSockets:server.clients.size,listenerClosed:true},failure,results};
 clearTimeout(watchdog);console.log(JSON.stringify(report));
}
