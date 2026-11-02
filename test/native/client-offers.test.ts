/* SPDX-License-Identifier: GPL-2.0-only */
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";

const root=fileURLToPath(new URL("../../",import.meta.url));
const binary=process.env.NATIVE_TEST_BINARY??join(root,"native/target/debug/obs-mcp-native");
type Info=ConstructorParameters<typeof Client>[0];
type Caps=NonNullable<ConstructorParameters<typeof Client>[1]>["capabilities"];
type Frame=Record<string,unknown>;
const capabilities={roots:{listChanged:true},sampling:{context:{},tools:{}},elicitation:{form:{applyDefaults:true},url:{}},experimental:{"example.invalid/unused":{}},extensions:{"example.invalid/unused":{}}} satisfies Caps;
const info=():Info=>({name:"offer-caller",version:"1",title:"A title is not authority",description:"Unused capability advertisement",websiteUrl:"https://example.invalid/client",icons:[{src:"https://example.invalid/icon.png",mimeType:"image/png",sizes:["16x16","any"],theme:"dark"}]});
const sourceCode=`import{EventEmitter}from'node:events';import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{initialize}from'./build/tools/index.js';class Fake extends EventEmitter{isConnected(){return false}getConnectionStatus(){return{url:'ws://unused.invalid',identified:false,hasPassword:false}}supportsRequest(){return null}async sendRequest(){process.stderr.write('UNEXPECTED_OBS_EFFECT\\n');throw Error('Unexpected OBS request')}}serveStdio(()=>{const server=new McpServer({name:'obs-mcp',version:'1.1.1'});initialize(server,new Fake(),{resourcesAndPrompts:false,sessionNotifications:false,confirmLive:false});return server});`;
type Session={client:Client;transport:StdioClientTransport;sent:Frame[];received:Frame[];diagnostics:string;legacy:boolean;native:boolean;implementation:Info;fake:FakeOBSServer};
const sessions:Session[]=[];const evidence:unknown[]=[];let next=0;
function saveEvidence(){if(process.env.NATIVE_CAPS_EVIDENCE)writeFileSync(process.env.NATIVE_CAPS_EVIDENCE,JSON.stringify(evidence,null,2)+"\n");}
async function start(native:boolean,legacy:boolean,rich=true,connect=true):Promise<Session>{
 const fake=await FakeOBSServer.start();
 const directory=join(root,"native/target",`client-offers-${process.pid}-${next++}`);mkdirSync(directory,{mode:0o700});
 const implementation=rich?info():{name:"offer-caller",version:"1"};
 const client=new Client(implementation,{capabilities:rich?capabilities:{},versionNegotiation:{mode:legacy?"legacy":{pin:"2026-07-28"}}});
 const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:["--input-type=module","-e",sourceCode],cwd:root,stderr:"pipe",env:{...getDefaultEnvironment(),OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?"1":"0",OBS_MCP_TOOLSETS:"",OBS_MCP_TOOLS:"obs-claim-control,obs-control-status,obs-release-control",OBS_MCP_READ_ONLY:"false",OBS_MCP_READ_OBS_CONFIG:"false",OBS_MCP_CONFIRM_LIVE:"false",OBS_MCP_DYNAMIC_TOOLSETS:"false",OBS_WEBSOCKET_URL:fake.url,OBS_MCP_COMPAT_NODE:join(directory,"nonexistent-node"),OBS_MCP_COMPAT_ENTRY:join(directory,"nonexistent-sdk.js")}});
 const session:Session={client,transport,sent:[],received:[],diagnostics:"",legacy,native,implementation,fake};sessions.push(session);
 transport.stderr?.on("data",bytes=>{session.diagnostics=(session.diagnostics+String(bytes)).slice(-8192);});
 let receive:typeof transport.onmessage;Object.defineProperty(transport,"onmessage",{configurable:true,get:()=>receive,set:(handler:typeof transport.onmessage)=>{receive=handler?message=>{session.received.push(structuredClone(message) as Frame);handler(message);}:undefined;}});
 const send=transport.send.bind(transport);transport.send=async frame=>{session.sent.push(structuredClone(frame) as Frame);await send(frame);};
 try{if(connect)await client.connect(transport);else await transport.start();return session;}catch(error){evidence.push({phase:"connect",native,legacy,error:String(error),sent:session.sent,received:session.received,diagnostics:session.diagnostics});saveEvidence();throw error;}
}
afterEach(async()=>{for(const session of sessions.splice(0)){await session.client.close().catch(()=>undefined);await session.transport.close();expect(session.fake.connectionCount).toBe(0);await session.fake.close();expect(session.diagnostics).not.toMatch(/^UNEXPECTED_OBS_EFFECT$/m);}saveEvidence();});
async function raw(session:Session,id:number,method:string,params:Frame){
 let response:Frame|undefined;const prior=session.transport.onmessage;
 session.transport.onmessage=message=>{const frame=message as unknown as Frame;if(frame.id===id){response=frame;session.received.push(frame);}else prior?.(message);};
 try{await session.transport.send({jsonrpc:"2.0",id,method,params} as Parameters<StdioClientTransport["send"]>[0]);await vi.waitFor(()=>expect(response).toBeDefined(),{timeout:3000});return response!;}finally{session.transport.onmessage=prior;}
}
function meta(clientInfo:unknown=info(),caps:unknown=capabilities){return{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":clientInfo,"io.modelcontextprotocol/clientCapabilities":caps};}
for(const legacy of[false,true])it(`admits unused client offers and extended info like actual Source SDK (legacy=${legacy})`,async()=>{
 const source=await start(false,legacy);const expected=await source.client.listTools();expect(expected.tools.some(tool=>tool.name==="obs-claim-control")).toBe(true);
 const sourceClaim=await source.client.callTool({name:"obs-claim-control",arguments:{minutes:1,reason:"offer probe"}});expect(sourceClaim.structuredContent).toMatchObject({holder:"offer-caller",reason:"offer probe"});
 evidence.push({phase:"source",legacy,sent:source.sent,claim:sourceClaim});saveEvidence();
 const native=await start(true,legacy);expect((await native.client.listTools()).tools.map(tool=>tool.name)).toEqual(["obs-claim-control","obs-release-control","obs-control-status"]);
 const claim=await native.client.callTool({name:"obs-claim-control",arguments:{minutes:1,reason:"offer probe"}});expect(claim.structuredContent).toMatchObject({holder:"offer-caller",reason:"offer probe"});
 expect(native.client.getServerCapabilities()).toEqual({tools:{listChanged:false},resources:{listChanged:false},completions:{},prompts:{listChanged:false}});
 for(const session of[source,native]){
  session.implementation.title="Changed display title";const status=await session.client.callTool({name:"obs-control-status",arguments:{}});expect(status.structuredContent).toMatchObject({caller:"offer-caller",lease:{holder:"offer-caller"}});
  session.implementation.name="different-caller";const release=await session.client.callTool({name:"obs-release-control",arguments:{}});
  const after=await session.client.callTool({name:"obs-control-status",arguments:{}});
  if(legacy){expect(release.isError).not.toBe(true);expect(after.structuredContent).toMatchObject({caller:"offer-caller",lease:null});}
  else{expect(release.isError).toBe(true);expect(after.structuredContent).toMatchObject({caller:"different-caller",lease:{holder:"offer-caller"}});}
  const sent=session.sent as {method?:string;params?:Record<string,unknown>}[];const handshake=sent.find(frame=>legacy?frame.method==="initialize":frame.params?._meta!==undefined)!;
  expect(legacy?handshake.params?.capabilities:(handshake.params?._meta as Frame)["io.modelcontextprotocol/clientCapabilities"]).toEqual(capabilities);
  expect(session.sent.some(frame=>["roots/list","sampling/createMessage","elicitation/create"].includes(String(frame.method)))).toBe(false);
  expect(session.received.some(frame=>["roots/list","sampling/createMessage","elicitation/create"].includes(String(frame.method)))).toBe(false);
  expect(session.received.some(frame=>(frame.result as Frame|undefined)?.resultType==="input_required")).toBe(false);
 }
 evidence.push({phase:"native",legacy,sent:native.sent,claim,serverCapabilities:native.client.getServerCapabilities()});saveEvidence();
});

it("refuses malformed offers and unowned feature invocation without fallback, OBS effects or era replacement",async()=>{
 const native=await start(true,false,false);
 const invalid=[meta(info(),{roots:null}),meta(info(),{roots:{listChanged:"yes"}}),meta(info(),{sampling:{tools:[]}}),meta(info(),{elicitation:{form:{applyDefaults:1}}}),meta(info(),{extensions:{vendor:1}}),meta({...info(),icons:[{src:3}]}),meta({...info(),icons:[{src:"x",theme:"sepia"}]}),meta({...info(),title:null}),meta(info(),{tasks:{}})];
 for(const [index,value]of invalid.entries()){const result=await raw(native,8000+index,"tools/list",{_meta:value});expect(result.error).toMatchObject({code:-32601});}
 for(const [index,method]of["roots/list","sampling/createMessage","elicitation/create","tasks/list"].entries()){const result=await raw(native,9000+index,method,{_meta:meta()});expect(result.error).toMatchObject({code:-32601});}
 expect((await native.client.listTools()).tools).toHaveLength(3);expect(native.diagnostics).not.toContain("nonexistent-node");
 const switched=await raw(native,9100,"initialize",{protocolVersion:"2025-11-25",clientInfo:{name:"other",version:"1"},capabilities:{}});expect(switched.error).toMatchObject({code:-32601});expect((await native.client.listTools()).tools).toHaveLength(3);
});

// Direct transport.send never lifts Client implementation metadata. Source's
// serveStdio therefore sees the actual absent field, including the first frame.
it("accepts absent modern client info as Source UNIDENTIFIED without carrying a prior name",async()=>{
 const observations:Frame[]=[];
 for(const native of[false,true]){
  const session=await start(native,false,false,false);
  const absent={"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}};
  let id=10000;
  const ask=(method:string,params:Frame={})=>raw(session,id++,method,{...params,_meta:absent});
  const call=(name:string,args:Frame={})=>ask("tools/call",{name,arguments:args});
  const discover=await ask("server/discover");
  evidence.push({phase:"missing-info-discover",native,reply:discover,sent:session.sent});saveEvidence();
  expect(discover).toHaveProperty("result.supportedVersions",["2026-07-28"]);
  expect((await ask("tools/list"))).toHaveProperty("result.tools");
  const initial=await call("obs-control-status");
  expect(initial).toHaveProperty("result.structuredContent",{caller:"an unidentified client",lease:null});
  const deniedClaim=await call("obs-claim-control",{minutes:1,reason:"unknown caller"});
  expect(deniedClaim).toHaveProperty("result.isError",true);
  expect(deniedClaim).toHaveProperty("result.content",[{type:"text",text:"Not claimed: this client did not identify itself. Over HTTP, connect with a name in the URL, e.g. http://127.0.0.1:<port>/mcp?client=codex"}]);
  const emptyRelease=await call("obs-release-control");
  expect(emptyRelease).toHaveProperty("result.content",[{type:"text",text:"No client has control"}]);
  const namedMeta=meta({name:"offer-caller",version:"1"},{});
  const namedClaim=await raw(session,id++,"tools/call",{_meta:namedMeta,name:"obs-claim-control",arguments:{minutes:1,reason:"identity boundary"}});
  const lease=(namedClaim.result as Frame).structuredContent;
  expect(lease).toMatchObject({holder:"offer-caller",reason:"identity boundary"});
  const unknownStatus=await call("obs-control-status");
  expect(unknownStatus).toHaveProperty("result.structuredContent",{caller:"an unidentified client",lease});
  const unknownClaim=await call("obs-claim-control",{minutes:2,reason:"must not replace named lease"});
  expect(unknownClaim).toHaveProperty("result.isError",true);
  expect(unknownClaim).toHaveProperty("result.content",(deniedClaim.result as Frame).content);
  const unknownRelease=await call("obs-release-control");
  expect(unknownRelease).toHaveProperty("result.isError",true);
  expect(((unknownRelease.result as Frame).content as {text:string}[])[0]?.text).toMatch(/^Not released: offer-caller has control/);
  expect(await call("obs-control-status")).toHaveProperty("result.structuredContent",{caller:"an unidentified client",lease});
  const namedRelease=await raw(session,id++,"tools/call",{_meta:namedMeta,name:"obs-release-control",arguments:{}});
  expect(namedRelease).toHaveProperty("result.content",[{type:"text",text:"Released control of OBS"}]);
  expect(await call("obs-control-status")).toHaveProperty("result.structuredContent",{caller:"an unidentified client",lease:null});
  // Capabilities remain required, while present null/malformed identity and
  // unrelated envelope vocabulary remain separate, refused native semantics.
  if(native){
   for(const invalid of[
    {...absent,"io.modelcontextprotocol/clientInfo":null},
    {...absent,"io.modelcontextprotocol/clientInfo":{}},
    {...absent,"io.modelcontextprotocol/clientInfo":{name:3,version:"1"}},
    {"io.modelcontextprotocol/protocolVersion":"2026-07-28"},
    {...absent,"io.modelcontextprotocol/clientCapabilities":null},
    {...absent,"io.modelcontextprotocol/clientCapabilities":{tasks:{}}},
    {...absent,progressToken:"unqualified"},
   ]){expect(await raw(session,id++,"tools/list",{_meta:invalid})).toHaveProperty("error.code",-32601);}
   expect(await call("obs-control-status")).toHaveProperty("result.structuredContent",{caller:"an unidentified client",lease:null});
  }
  observations.push({native,initial,deniedClaim,emptyRelease,namedClaim,unknownStatus,unknownClaim,unknownRelease,namedRelease,sent:session.sent});
  evidence.push({phase:"missing-info-identity-one-owner",native,observation:observations.at(-1)});saveEvidence();
 }
 evidence.push({phase:"missing-info-identity",observations});saveEvidence();
});
