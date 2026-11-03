# SPDX-License-Identifier: GPL-2.0-only
"""Owned loopback peer. Generates only disposable fixture keys; never uses user credentials."""
import base64, hashlib, json, os, pathlib, socket, ssl, struct, subprocess, sys, tempfile, threading, time
mode=sys.argv[1]
fixture_parent=pathlib.Path(__file__).resolve().parents[2]/'target'
fixture_parent.mkdir(exist_ok=True)
root=pathlib.Path(tempfile.mkdtemp(prefix='.tls-ca-fixture-',dir=fixture_parent))
os.chmod(root,0o700)
(root/'state').mkdir(mode=0o700)
def openssl(*args):
    p=subprocess.run(['openssl',*args],cwd=root,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    if p.returncode:raise RuntimeError('fixture certificate generation failed')
openssl('req','-x509','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout','ca.fixture.key','-out','ca.pem','-subj','/CN=OBS MCP disposable test CA','-days','2','-addext','basicConstraints=critical,CA:TRUE,pathlen:0','-addext','keyUsage=critical,keyCertSign,cRLSign')
openssl('x509','-in','ca.pem','-outform','DER','-out','ca.der')
openssl('req','-new','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout','leaf.fixture.key','-out','leaf.csr','-subj','/CN=OBS MCP disposable peer')
san={'wrongdns':'DNS:wrong.invalid','wrongip':'IP:127.0.0.2'}.get(mode,'DNS:localhost,IP:127.0.0.1')
(root/'leaf.ext').write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName='+san+'\n')
if mode=='expired':
    (root/'index').write_text('');(root/'serial').write_text('1000\n')
    (root/'ca.conf').write_text('[ca]\ndefault_ca=local\n[local]\nnew_certs_dir=.\ndatabase=index\nserial=serial\nprivate_key=ca.fixture.key\ncertificate=ca.pem\ndefault_md=sha256\npolicy=policy\n[policy]\ncommonName=supplied\n')
    openssl('ca','-batch','-config','ca.conf','-in','leaf.csr','-out','leaf.pem','-startdate','20200101000000Z','-enddate','20200102000000Z','-extfile','leaf.ext','-notext')
else:openssl('x509','-req','-in','leaf.csr','-CA','ca.pem','-CAkey','ca.fixture.key','-CAcreateserial','-out','leaf.pem','-days','1','-extfile','leaf.ext')
for key in root.glob('*.fixture.key'):os.chmod(key,0o600)
listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(8);listener.settimeout(.05)
stop=threading.Event();changed=threading.Condition();facts={'accepted':0,'upgrades':0,'identifies':0,'authenticated':0,'reads':0,'requestTypes':[],'tls':[],'alpn':[],'closed':False}
def fact(key):
    with changed:facts[key]+=1;changed.notify_all()
def exact(s,n):
    result=b''
    while len(result)<n:
        if stop.is_set():raise EOFError()
        try:b=s.recv(n-len(result))
        except socket.timeout:continue
        if not b:raise EOFError()
        result+=b
    return result
def send(s,obj):
    b=json.dumps(obj,separators=(',',':')).encode();s.sendall(bytes([0x81,len(b)])+b if len(b)<126 else b'\x81\x7e'+struct.pack('!H',len(b))+b)
def receive(s):
    a,b=exact(s,2);n=b&127
    if n==126:n=struct.unpack('!H',exact(s,2))[0]
    if n==127:n=struct.unpack('!Q',exact(s,8))[0]
    if n>1048576:raise ValueError('fixture frame bound')
    mask=exact(s,4) if b&128 else None
    body=exact(s,n)
    if mask:body=bytes(x^mask[i%4] for i,x in enumerate(body))
    if a&15==8:raise EOFError()
    return json.loads(body)
def serve():
    try:
        while not stop.is_set():
            try:raw,_=listener.accept()
            except socket.timeout:continue
            fact('accepted');raw.settimeout(2)
            try:
                if mode=='holdtls':stop.wait(3);continue
                ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                ctx.minimum_version=ctx.maximum_version=ssl.TLSVersion.TLSv1_2 if mode=='valid12' else ssl.TLSVersion.TLSv1_3
                if mode!='valid12':ctx.num_tickets=2
                ctx.load_cert_chain(root/'leaf.pem',root/'leaf.fixture.key');ctx.set_alpn_protocols(['http/1.1'])
                with ctx.wrap_socket(raw,server_side=True) as peer:
                    peer.settimeout(.05)
                    with changed:facts['tls'].append(peer.version());facts['alpn'].append(peer.selected_alpn_protocol())
                    request=b''
                    while not request.endswith(b'\r\n\r\n'):
                        request+=exact(peer,1)
                        if len(request)>8192:raise ValueError('fixture upgrade bound')
                    fact('upgrades')
                    if mode=='holdupgrade':stop.wait(3);continue
                    headers=dict(line.split(':',1) for line in request.decode().split('\r\n')[1:] if ':' in line)
                    key=next(v.strip() for k,v in headers.items() if k.lower()=='sec-websocket-key')
                    accept=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
                    peer.sendall(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n').encode())
                    send(peer,{'op':0,'d':{'obsWebSocketVersion':'5.7.4','rpcVersion':1,'authentication':{'challenge':'fixture-challenge','salt':'fixture-salt'}}})
                    identify=receive(peer);fact('identifies')
                    secret=base64.b64encode(hashlib.sha256(b'non-user-fixture-passwordfixture-salt').digest())
                    auth=base64.b64encode(hashlib.sha256(secret+b'fixture-challenge').digest()).decode()
                    if identify.get('op')!=1 or identify.get('d',{}).get('authentication')!=auth:raise ValueError('fixture authentication failed')
                    fact('authenticated')
                    if mode=='holdidentify':stop.wait(3);continue
                    send(peer,{'op':2,'d':{'negotiatedRpcVersion':1}})
                    while not stop.is_set():
                        message=receive(peer)
                        if message.get('op')!=6:raise ValueError('fixture request kind')
                        d=message['d']
                        with changed:
                            if len(facts['requestTypes'])>=64:raise ValueError('fixture request-count bound')
                            facts['requestTypes'].append(d['requestType'])
                        if d['requestType']!='GetVersion':raise ValueError('fixture refuses non-read')
                        fact('reads')
                        send(peer,{'op':7,'d':{'requestId':d['requestId'],'requestType':'GetVersion','requestStatus':{'result':True,'code':100},'responseData':{'obsVersion':'fixture-native-tls','obsWebSocketVersion':'5.7.4','rpcVersion':1,'availableRequests':['GetVersion'],'supportedImageFormats':['png'],'platform':'macos'}}})
            except (EOFError,OSError,ssl.SSLError):pass
            finally:raw.close()
    finally:
        listener.close()
        with changed:facts['closed']=True;changed.notify_all()
thread=threading.Thread(target=serve);thread.start()
print(json.dumps({'port':listener.getsockname()[1],'ca':str(root/'ca.pem'),'state':str(root/'state'),'der':str(root/'ca.der')}),flush=True)
try:
    for command in sys.stdin:
        command=command.strip()
        if command=='stop':break
        if command.startswith('wait '):
            name=command[5:]
            with changed:changed.wait_for(lambda:facts.get(name,0)>0,timeout=2)
        with changed:print(json.dumps(facts),flush=True)
finally:
    stop.set();thread.join(3)
    if thread.is_alive():raise RuntimeError('fixture thread did not join')
    print(json.dumps(facts),flush=True)
    import shutil
    shutil.rmtree(root)
