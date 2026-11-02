// SPDX-License-Identifier: GPL-2.0-only
// Development packaging metadata. Never imports or starts the Source server.
import {existsSync,lstatSync,readFileSync,readdirSync} from 'node:fs';
import {join,relative} from 'node:path';
const json=path=>JSON.parse(readFileSync(path,'utf8'));
export function nativeManifest(root,platform=process.platform,arch=process.arch){
 if(!['darwin','linux'].includes(platform))throw new Error('Native MCPB packaging is qualified only for macOS/Linux build hosts');
 const manifest=json(join(root,'manifest.json')),catalog=json(join(root,'native/assets/tools.json'));
 const tools=catalog.tools;if(!Array.isArray(tools)||tools.length!==178||new Set(tools.map(tool=>tool.name)).size!==178)throw new Error('Checked native catalog must contain the reviewed 178-name profile union');
 const cargo=readFileSync(join(root,'native/Cargo.toml'),'utf8');const version=/^version\s*=\s*"([^"]+)"/m.exec(cargo)?.[1];if(!version||version!==json(join(root,'package.json')).version)throw new Error('Native and project versions must agree');
 manifest.version=version;manifest.name='obs-mcp-native';manifest.display_name='OBS MCP Native';manifest.description='Rust OBS MCP service with immutable production and advanced profiles';
 manifest.long_description=`Host-specific Rust service for ${platform}/${arch}. Node.js and the JavaScript MCP SDK are not installed or run by this extension. Choose an existing approved private state directory before startup. Discovery works while OBS is offline. The 178-name catalog is a union of immutable profiles, not simultaneous tool availability: Production exposes up to 164 with approved host/media adapters; Advanced exposes 163 and excludes guarded Take/media/Stage/recovery owners. Read-only and dynamic tool groups narrow discovery further. Native transport/platform qualification is separate from package contents; consult docs/native-distribution.md for current prerequisites and limits.`;
 manifest.compatibility={...manifest.compatibility,platforms:[platform]};delete manifest.compatibility.runtimes;
 manifest.server={type:'binary',entry_point:'native/obs-mcp-native',mcp_config:{command:'${__dirname}/native/obs-mcp-native',args:[],env:{...manifest.server.mcp_config.env,OBS_MCP_STATE_DIR:'${user_config.obs_mcp_state_dir}',OBS_MCP_NATIVE_PROFILE:'${user_config.obs_mcp_native_profile}',OBS_MCP_NATIVE_RECORDING_ROOT:'${user_config.obs_mcp_recording_root}',OBS_MCP_NATIVE_FFMPEG:'${user_config.obs_mcp_ffmpeg}',OBS_MCP_NATIVE_FFPROBE:'${user_config.obs_mcp_ffprobe}'}}};
 delete manifest.server.mcp_config.env.OBS_MCP_COMPAT_ENTRY;delete manifest.server.mcp_config.env.OBS_MCP_COMPAT_NODE;delete manifest.server.mcp_config.env.OBS_MCP_NATIVE_STATEFUL;
 manifest.user_config.obs_mcp_state_dir={type:'directory',title:'Approved private state directory',description:'Required existing absolute directory owned by you, with safe private permissions (0700 on Unix). Stores endpoint-bound operation journals and snapshots. Create and approve it explicitly; the extension does not initialize a new home directory or erase history.',required:true};
 manifest.user_config.obs_mcp_native_profile={type:'string',title:'Native capability profile',description:'production (default) or advanced. Advanced permits manual/raw/vendor/hotkey effects but never creates guarded Take/media/Stage/recovery owners. Possible advanced effects durably block later guarded reentry in the same endpoint/state namespace; restarting MCP does not clear this.',required:false,default:'production'};
 manifest.user_config.obs_mcp_recording_root={type:'directory',title:'Approved recording directory (optional)',description:'Existing approved private recording directory for Production Take/Clip/file workflows. Leave empty for inspection and other available workflows. Advanced ignores guarded-owner configuration. Availability depends on validated local adapters and media tools.',required:false};
 for(const [key,title]of[['ffmpeg','FFmpeg'],['ffprobe','ffprobe']])manifest.user_config[`obs_mcp_${key}`]={type:'file',title:`${title} executable (optional)`,description:`Existing trusted ${title} executable used for local media inspection/processing. If omitted, discovery uses PATH. No executable is downloaded or installed by this extension.`,required:false};
 manifest.tools=tools.map(({name,description})=>({name,...(description?{description}:{})}));manifest.tools_generated=true;
 return manifest;
}
export function nativeBundleFiles(root){
 const files=['LICENSE','NOTICE.md','README.md','icon.png','native/Cargo.toml','native/Cargo.lock','native/rust-toolchain.toml','docs/protocol.json','docs/native-distribution.md','docs/native-profiles.md','docs/native-dependencies.md','docs/native-architecture.md','docs/full-rust-cutover.md','docs/native-migration-ledger.json','docs/production-assistant.md','.agents/skills/obs-demo/SKILL.md'];
 for(const directory of['native/src','native/assets','native/tests','native/platform','native/crates','licenses','docs/recipes','assets/identity']){
  if(!existsSync(join(root,directory)))continue;
  const walk=path=>{for(const name of readdirSync(path).sort()){const file=join(path,name),info=lstatSync(file);if(info.isSymbolicLink())throw new Error('Native source bundle refuses symbolic links');if(info.isDirectory()){if(!['target','.cargo-home','.git','node_modules','dist','__pycache__','.venv'].includes(name))walk(file);}else if(info.isFile()){const entry=relative(root,file);if(entry.startsWith('native/tests/')&&/\.[cm]?js$/.test(entry))continue;files.push(entry);}else throw new Error('Native source bundle requires regular files');}};walk(join(root,directory));
 }
 for(const file of files){const info=lstatSync(join(root,file));if(!info.isFile()||info.isSymbolicLink())throw new Error('Native bundle input must be a regular file');if(/(?:^|\/)(?:node_modules|build|server)(?:\/|$)|\.(?:[cm]?js|[cm]?ts|node)$/.test(file))throw new Error('JavaScript runtime payload is forbidden in the native bundle');}
 return files.sort();
}
