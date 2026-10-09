const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const rooms = new Map();
const html = path.join(__dirname, 'index.html');
const server = http.createServer((req,res)=>{
  const p=(req.url||'/').split('?')[0];
  if(p==='/health'){res.writeHead(200,{'Content-Type':'application/json; charset=utf-8'});return res.end(JSON.stringify({ok:true,rooms:rooms.size}));}
  if(p==='/'||p==='/index.html'){
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
    return res.end(fs.readFileSync(html));
  }
  res.writeHead(404);res.end('Not found');
});
const wss = new WebSocket.Server({server,path:'/ws'});
function send(ws,obj){if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));}
function broadcast(room,obj,except){const r=rooms.get(room);if(!r)return;for(const c of r.clients)if(c!==except)send(c,obj);}
function players(room){const r=rooms.get(room);return r?r.clients.size:0;}
function announce(room){broadcast(room,{type:'players',players:players(room)});}
wss.on('connection',ws=>{
  let room=null;
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch{return;}
    if(m.type==='create'||m.type==='join'){
      room=String(m.room||'').trim().toUpperCase();
      if(!/^[A-Z0-9-]{3,24}$/.test(room))return send(ws,{type:'error',message:'ルームコードが不正です'});
      if(!rooms.has(room))rooms.set(room,{clients:new Set(),state:m.state||null,host:ws});
      const r=rooms.get(room);
      r.clients.add(ws);
      if(m.type==='create')r.host=ws;
      send(ws,{type:'joined',room,host:ws===r.host,players:r.clients.size});
      if(r.state)send(ws,{type:'state',state:r.state,players:r.clients.size});
      announce(room);
      return;
    }
    if(m.type==='state'&&room){
      const r=rooms.get(room);if(!r)return;
      if(ws!==r.host)return;
      r.state=m.state;
      broadcast(room,{type:'state',state:r.state,players:r.clients.size},ws);
      return;
    }
    if(m.type==='ping')send(ws,{type:'pong'});
    if(m.type==='leave')ws.close();
  });
  ws.on('close',()=>{
    if(!room||!rooms.has(room))return;
    const r=rooms.get(room);r.clients.delete(ws);
    if(r.host===ws)r.host=r.clients.values().next().value||null;
    if(r.clients.size===0)rooms.delete(room);else announce(room);
  });
});
server.listen(PORT,()=>console.log(`Chappy DX online server: http://localhost:${PORT}`));
