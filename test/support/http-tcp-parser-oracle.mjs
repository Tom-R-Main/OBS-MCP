// SPDX-License-Identifier: MIT
// Built-in Node parser used by Source's default http.createServer; fixture only.
import http from 'node:http';
import net from 'node:net';
import {once} from 'node:events';
import {writeFileSync} from 'node:fs';
const head=(lines,body='')=>lines.join('\r\n')+'\r\n\r\n'+body;
export const parserCases=[
 ['plain',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 2','Connection: close'],'{}')],
 ['te-cl',head(['POST /mcp HTTP/1.1','Host: localhost','Transfer-Encoding: chunked','Content-Length: 0','Connection: close'],'0\r\n\r\n')],
 ['cl-te',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 0','Transfer-Encoding: chunked','Connection: close'],'0\r\n\r\n')],
 ['same-cl',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 2','Content-Length: 2','Connection: close'],'{}')],
 ['different-cl',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 1','Content-Length: 2','Connection: close'],'{}')],
 ['comma-cl',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 2, 2','Connection: close'],'{}')],
 ['chunked',head(['POST /mcp HTTP/1.1','Host: localhost','Transfer-Encoding: chunked','Connection: close'],'2\r\n{}\r\n0\r\n\r\n')],
 ['repeated-te',head(['POST /mcp HTTP/1.1','Host: localhost','Transfer-Encoding: chunked','Transfer-Encoding: chunked','Connection: close'],'0\r\n\r\n')],
 ['unknown-te',head(['POST /mcp HTTP/1.1','Host: localhost','Transfer-Encoding: gzip','Connection: close'],'')],
 ['bare-lf','POST /mcp HTTP/1.1\nHost: localhost\nContent-Length: 2\nConnection: close\n\n{}'],
 ['obs-fold',head(['POST /mcp HTTP/1.1','Host: localhost','X-Test: a',' b','Content-Length: 2','Connection: close'],'{}')],
 ['space-before-colon',head(['POST /mcp HTTP/1.1','Host : localhost','Content-Length: 2','Connection: close'],'{}')],
 ['duplicate-host-good-first',head(['POST /mcp HTTP/1.1','Host: localhost','Host: other.invalid','Content-Length: 2','Connection: close'],'{}')],
 ['duplicate-host-bad-first',head(['POST /mcp HTTP/1.1','Host: other.invalid','Host: localhost','Content-Length: 2','Connection: close'],'{}')],
 ['duplicate-auth-a-first',head(['POST /mcp HTTP/1.1','Host: localhost','Authorization: fixture-a','Authorization: fixture-b','Content-Length: 2','Connection: close'],'{}')],
 ['duplicate-auth-b-first',head(['POST /mcp HTTP/1.1','Host: localhost','Authorization: fixture-b','Authorization: fixture-a','Content-Length: 2','Connection: close'],'{}')],
 ['expect-continue',head(['POST /mcp HTTP/1.1','Host: localhost','Expect: 100-continue','Content-Length: 2','Connection: close']), '{}'],
 ['expect-continue-early-reject',head(['POST /mcp HTTP/1.1','Host: localhost','Expect: 100-continue','X-Fixture-Reject: true','Content-Length: 2','Connection: close']), '{}'],
 ['expect-other',head(['POST /mcp HTTP/1.1','Host: localhost','Expect: fixture-other','Content-Length: 2','Connection: close']), '{}'],
 ['pipelined',head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 2'],'{}')+head(['POST /mcp HTTP/1.1','Host: localhost','Content-Length: 2','Connection: close'],'{}')],
];
export async function rawExchange(port, request, afterContinue){
 const socket=net.connect({host:'127.0.0.1',port});let raw=Buffer.alloc(0),sent=false;
 const closed=once(socket,'close');socket.on('error',()=>{});await once(socket,'connect');
 const timer=setTimeout(()=>socket.destroy(),2000);socket.on('data',chunk=>{raw=Buffer.concat([raw,chunk]);if(raw.length>128*1024)socket.destroy();if(afterContinue&&!sent&&raw.includes(Buffer.from('HTTP/1.1 100 Continue\r\n\r\n'))){sent=true;socket.write(afterContinue);}});
 socket.write(request);await closed;clearTimeout(timer);
 return{statuses:[...raw.toString('latin1').matchAll(/HTTP\/1\.[01] (\d{3})/g)].map(match=>Number(match[1])),rawBytes:raw.length,sentAfterContinue:sent};
}
export async function nodeParserOracle(){
 const records=[];let current=[];const peers=new Set();
 const server=http.createServer((req,res)=>{const projected=new Headers();for(const[name,value]of Object.entries(req.headers)){if(value!==undefined)for(const item of Array.isArray(value)?value:[value])projected.append(name,item);}
  current.push({hostFirstIsLocalhost:req.headers.host==='localhost',authFirstIsA:req.headers.authorization==='fixture-a',hostFields:req.rawHeaders.filter((_,index)=>index%2===0&&req.rawHeaders[index].toLowerCase()==='host').length,authFields:req.rawHeaders.filter((_,index)=>index%2===0&&req.rawHeaders[index].toLowerCase()==='authorization').length,projectedAuthIsA:projected.get('authorization')==='fixture-a'});
  if(req.headers['x-fixture-reject']==='true'){res.writeHead(403).end();return;}
  req.resume();req.on('end',()=>res.writeHead(200,{'content-type':'application/json'}).end('{}'));
 });server.on('connection',peer=>{peers.add(peer);peer.once('close',()=>peers.delete(peer));});server.listen(0,'127.0.0.1');await once(server,'listening');
 try{for(const[name,request,after]of parserCases){current=[];const wire=await rawExchange(server.address().port,request,after);records.push({name,...wire,callbacks:current});}}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
 return{node:process.version,source:'src/http.ts default http.createServer + toRequest header projection',cases:records,openPeers:peers.size};
}
if(process.argv[1]?.endsWith('http-tcp-parser-oracle.mjs')){const result=await nodeParserOracle();if(process.argv[2])writeFileSync(process.argv[2],JSON.stringify(result,null,2)+'\n');else console.log(JSON.stringify(result));}
