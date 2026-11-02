// SPDX-License-Identifier: GPL-2.0-only
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {Client} from '@modelcontextprotocol/client';
import {StdioClientTransport,getDefaultEnvironment} from '@modelcontextprotocol/client/stdio';
const parse = JSON.parse;
const base={sceneName:'Demo',sceneItemId:2};
const cases=[
 ['defaults',{},{}],
 ['position-x-deletes-supplied-y',{sceneItemTransform:{positionY:22,keep:true},positionX:1},{sceneItemTransform:{z:3,a:1}}],
 ['position-y-deletes-supplied-x',{sceneItemTransform:{positionX:11,positionY:22,keep:true},positionY:2},{sceneItemTransform:{positionY:2}}],
 ['scale-x-deletes-supplied-y',{sceneItemTransform:{scaleY:4,keep:true},scaleX:3},{}],
 ['scale-y-deletes-supplied-x',{sceneItemTransform:{scaleX:3,keep:true},scaleY:4},{}],
 ['crop-independent',{sceneItemTransform:{cropTop:2,cropBottom:3,cropLeft:4,cropRight:5},cropTop:0},{}],
 ['flat-overrides',{sceneItemTransform:{rotation:3,positionX:3},positionX:0,positionY:-1,rotation:0,scaleX:-2,scaleY:0,cropTop:-0,cropBottom:2,cropLeft:3,cropRight:4},{}],
 ['proto-record',{sceneItemTransform:parse('{"__proto__":{"outer":"strip"},"$serde_json::private::RawValue":"123","nested":{"__proto__":"keep","$serde_json::private::RawValue":{"keep":true}}}')},{sceneItemTransform:parse('{"__proto__":{"readback":"keep"},"z":3,"a":1}')}],
 ['numeric-request',{sceneItemTransform:parse('{"negativeZero":-0.0,"integer":1.0,"unsafe":9007199254740993,"u64":18446744073709551615,"tie":144713588892437.625}'),positionX:144713588892437.625},{sceneItemTransform:parse('{"huge":18446744073709551615,"tie":144713588892437.625}')}],
 ['empty-scene-name',{sceneName:'',sceneUuid:'uuid'},{}],
 ['missing-scene-name',{sceneName:undefined,sceneUuid:'uuid'},{}],
 ['both-scene-selectors-missing',{sceneName:undefined,canvasUuid:''},{}],
 ['selected-null',{}, {sceneItemTransform:null}],
 ['selected-false',{}, {sceneItemTransform:false}],
 ['selected-zero',{}, {sceneItemTransform:0}],
 ['selected-empty-string',{}, {sceneItemTransform:''}],
 ['selected-array',{}, {sceneItemTransform:[false,null,{z:3,a:1}]}],
 ['response-null',{},null],
 ['response-false',{},false],
 ['response-array',{},[]],
 ['width-zero',{}, {sceneItemTransform:{sourceWidth:0,sourceHeight:23}}],
 ['height-negative-zero',{}, {sceneItemTransform:{sourceWidth:23,sourceHeight:-0}}],
 ['width-string-zero',{}, {sceneItemTransform:{sourceWidth:'0',sourceHeight:false}}],
 ['readback-rejected',{}, {},'GetSceneItemTransform'],
 ['mutation-rejected',{}, {},'SetSceneItemTransform'],
 ['invalid-record',{sceneItemTransform:[]},{}],
 ['invalid-id-unsafe',{sceneItemId:9007199254740992},{}],
 ['invalid-id-negative',{sceneItemId:-1},{}],
 ['invalid-flat-null',{positionX:null},{}],
 ['invalid-selector-null',{sceneName:null},{}],
 ['safe-id-endpoint',{sceneItemId:9007199254740991},{}],
];
const results=[];
const root=process.cwd();
for(const legacy of[false,true]) {
 const valid=cases.filter(row=>!row[0].startsWith('invalid-'));
 const code=`import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{EventEmitter}from'node:events';import{initialize}from'./build/tools/index.js';import{OBSRequestError}from'./build/client.js';const cases=${JSON.stringify(valid)};let active;const obs=new EventEmitter();Object.assign(obs,{isConnected:()=>true,getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false}),supportsRequest:()=>true,sendRequest:async(requestType,data)=>{if(requestType==='SetSceneItemTransform')active=cases.shift();process.stderr.write('TRACE '+JSON.stringify({case:active[0],requestType,...(data===undefined?{}:{requestData:JSON.parse(JSON.stringify(data))})})+'\\n');if(active[3]===requestType)throw new OBSRequestError(requestType,500,'PRIVATE transform fixture failure');return requestType==='GetSceneItemTransform'?active[2]:{};}});serveStdio(()=>{const server=new McpServer({name:'transform-callback-source',version:'1'});initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});return server;});`;
 const directory=mkdtempSync(join(root,'native/target/transform-source-oracle-'));
 const client=new Client({name:'transform-callback-oracle',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
 const transport=new StdioClientTransport({command:process.execPath,args:['--input-type=module','-e',code],cwd:root,env:{...getDefaultEnvironment(),OBS_MCP_STATE_DIR:directory,OBS_MCP_CONFIRM_LIVE:'false'},stderr:'pipe'});
 let diagnostics='';transport.stderr?.on('data',chunk=>diagnostics+=String(chunk));
 try {await client.connect(transport);for(const row of cases){const args={...base,...row[1]};const result=await client.callTool({name:'obs-set-scene-item-transform',arguments:args});delete result._meta;results.push({legacy,case:row[0],args:parse(JSON.stringify(args)),result});}}
 finally{await client.close();await transport.close();rmSync(directory,{recursive:true,force:true});}
 const traces=diagnostics.split('\n').filter(line=>line.startsWith('TRACE ')).map(line=>parse(line.slice(6)));
 for(const row of results.filter(row=>row.legacy===legacy))row.trace=traces.filter(trace=>trace.case===row.case).map(({case:_,...trace})=>trace);
}
for(const legacy of[false,true]) {
 const find=name=>results.find(row=>row.legacy===legacy&&row.case===name);
 assert.deepEqual(find('position-x-deletes-supplied-y').trace[0].requestData.sceneItemTransform,{keep:true,positionX:1});
 assert.deepEqual(find('position-y-deletes-supplied-x').trace[0].requestData.sceneItemTransform,{positionY:2,keep:true});
 assert.deepEqual(find('scale-x-deletes-supplied-y').trace[0].requestData.sceneItemTransform,{keep:true,scaleX:3});
 assert.deepEqual(find('scale-y-deletes-supplied-x').trace[0].requestData.sceneItemTransform,{keep:true,scaleY:4});
 assert.deepEqual(find('crop-independent').trace[0].requestData.sceneItemTransform,{cropTop:0,cropBottom:3,cropLeft:4,cropRight:5});
 assert.equal(Object.hasOwn(find('proto-record').trace[0].requestData.sceneItemTransform,'__proto__'),false);
 assert.equal(Object.hasOwn(find('proto-record').trace[0].requestData.sceneItemTransform.nested,'__proto__'),true);
 assert.equal(find('empty-scene-name').result.structuredContent.message,'Successfully updated transform for item with ID 2 in ');
 for(const[name,value]of[['selected-false',false],['selected-zero',0],['selected-empty-string',''],['selected-array',[false,null,{z:3,a:1}]]])assert.deepEqual(find(name).result.structuredContent.sceneItemTransform,value);
 for(const name of['defaults','selected-null','response-null','response-false','response-array'])assert.deepEqual(find(name).result.structuredContent.sceneItemTransform,{});
 assert.equal(find('width-zero').result.structuredContent.warnings.length,1);assert.equal(find('height-negative-zero').result.structuredContent.warnings.length,1);assert.equal(find('width-string-zero').result.structuredContent.warnings,undefined);
 assert.notEqual(find('readback-rejected').result.isError,true);assert.match(find('readback-rejected').result.structuredContent.warnings[0],/Could not read the applied transform back/);
 assert.equal(find('mutation-rejected').result.isError,true);assert.equal(find('mutation-rejected').trace.length,1);
 for(const row of results.filter(row=>row.legacy===legacy&&row.case.startsWith('invalid-'))){assert.equal(row.result.isError,true);assert.equal(row.trace.length,0);}
 assert.match(find('numeric-request').result.content[0].text, /Applied transform: {"huge":18446744073709552000,"tie":144713588892437.62}/);
}
console.log(JSON.stringify({cases:cases.length,eras:2,callbacks:results.length,results}));
