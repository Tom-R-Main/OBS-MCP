#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// Capture static text from the actual prompt registration. Runtime replacement
// scans only the template, so inserted arguments cannot become other markers.
import { Client } from '@modelcontextprotocol/client';
import { McpServer, InMemoryTransport } from '@modelcontextprotocol/server';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { initialize, demoRecipe } from '../build/tools/prompts.js';
import { initialize as initializeResources } from '../build/tools/resources.js';
const server=new McpServer({name:'obs-mcp',version:'1.1.1'});initialize(server);
let sceneNames=null;
initializeResources(server, {sendRequest(type){if(type!=='GetSceneList'||sceneNames===null)throw Error('Static discovery unexpectedly requested OBS');return {scenes:sceneNames.map(sceneName=>({sceneName}))};}}, {sessionNotifications:false});
const client=new Client({name:'native-prompt-export',version:'1'});
const [a,b]=InMemoryTransport.createLinkedPair();
const markers={collection:'__OBS_NATIVE_COLLECTION_70ba__',scene:'__OBS_NATIVE_SCENE_70ba__',input:'__OBS_NATIVE_INPUT_70ba__',window:'__OBS_NATIVE_WINDOW_70ba__',seconds:'__OBS_NATIVE_SECONDS_70ba__',steps:'__OBS_NATIVE_STEPS_70ba__'};
function text(result){
 if(Object.keys(result).some(k=>k!=='messages')||result.messages?.length!==1||result.messages[0].role!=='user'||result.messages[0].content.type!=='text')throw Error('Prompt source shape requires a reviewed native port');
 return result.messages[0].content.text;
}
try{
 await Promise.all([server.connect(a),client.connect(b)]);
 const listing=await client.listPrompts();
 const resourceTemplates=await client.listResourceTemplates();
 sceneNames=[];
 const resourceListing=await client.listResources();
 if(resourceListing.resources.map(r=>r.uri).join(',')!=='obs://status,obs://take/current,obs://scenes')throw Error('Static resource inventory requires reviewed native port');
 sceneNames=[markers.scene];
 const dynamicResources=await client.listResources();
 if(dynamicResources.resources.length!==resourceListing.resources.length+1||JSON.stringify(dynamicResources.resources.slice(0,-1))!==JSON.stringify(resourceListing.resources))throw Error('Dynamic resource ordering requires reviewed native port');
 const sceneItemResource=dynamicResources.resources.at(-1);
 if(sceneItemResource.uri!==`obs://scene/${encodeURIComponent(markers.scene)}/items`||sceneItemResource.name!==`${markers.scene} items`)throw Error('Scene resource projection requires reviewed native port');
 sceneNames=null;
 if(listing.prompts.map(p=>p.name).join(',')!=='software-demo,record-demo,pre-stream-check')throw Error('Prompt inventory requires reviewed native port');
 const software=text(await client.getPrompt({name:'software-demo',arguments:{sceneCollection:markers.collection,sceneName:markers.scene,inputName:markers.input,maxDurationSeconds:'739'}}));
 const record=text(await client.getPrompt({name:'record-demo',arguments:{window:markers.window,scene:markers.scene,seconds:markers.seconds}}));
 const steps=text(await client.getPrompt({name:'record-demo',arguments:{window:markers.window,scene:markers.scene,seconds:markers.seconds,steps:markers.steps}}));
 const preStream=await client.getPrompt({name:'pre-stream-check'});text(preStream);
 const replacements={software:{collection:JSON.stringify(markers.collection),scene:JSON.stringify(markers.scene),input:JSON.stringify(markers.input),duration:'maxDurationSeconds:739'},record:{window:markers.window,scene:markers.scene,seconds:markers.seconds,steps:'   - '+markers.steps+'\n'}};
 for(const [template,reps]of [[software,replacements.software],[record,{window:markers.window,scene:markers.scene,seconds:markers.seconds}],[steps,{window:markers.window,scene:markers.scene,steps:replacements.record.steps}]])for(const marker of Object.values(reps))if(!template.includes(marker))throw Error('Prompt interpolation marker absent');
 const asset={formatVersion:1,listing,resourceTemplates,resourceListing,sceneItemResource,recipe:demoRecipe,templates:{software,record,steps,preStream},replacements};
 const path=fileURLToPath(new URL('../native/assets/prompts.json',import.meta.url));
 const bytes=JSON.stringify(asset,null,2)+'\n';
 if(!existsSync(path)||!readFileSync(path).equals(Buffer.from(bytes)))writeFileSync(path,bytes);
 console.log(JSON.stringify({nativePrompts:listing.prompts.length}));
}finally{await client.close();await server.close();}
