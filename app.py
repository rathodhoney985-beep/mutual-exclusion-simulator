import random
import threading
import webbrowser

from flask import Flask, jsonify, request

app = Flask(__name__)

ALGOS = {
    "centralized": "Centralized (Coordinator)",
    "lamport": "Lamport's Algorithm",
    "ricart": "Ricart-Agrawala",
    "token": "Token Ring",
}
MAX_TICKS = 3000


class Proc:
    def __init__(self, pid):
        self.id = pid
        self.state = "idle"
        self.clock = 0
        self.k = 0
        self.req_ts = 0
        self.replies = set()
        self.deferred = []
        self.queue = []
        self.has_token = False
        self.exit_at = 0
        self.dur = 1
        self.cur = None


class Sim:
    def __init__(self, algo, n, reqs, cs_len, max_delay, seed):
        self.algo, self.n, self.reqs, self.max_delay = algo, n, reqs, max_delay
        sched = random.Random(seed)
        horizon = 4 + 3 * reqs
        self.times = [sorted(sched.randint(0, horizon) for _ in range(reqs)) for _ in range(n)]
        self.durs = [[sched.randint(max(1, cs_len - 1), cs_len + 1) for _ in range(reqs)] for _ in range(n)]
        self.rng = random.Random(seed + 7919)
        self.P = [Proc(i) for i in range(n)]
        self.C = n
        self.holder, self.cq = None, []
        self.msgs, self.mid, self.last = [], 0, {}
        self.counts, self.sent = {}, 0
        self.t, self.log, self.records, self.frames = 0, [], [], []
        self.violations, self.end_t, self.completed = 0, 0, False

    def send(self, src, dst, typ, ts=None):
        delay = self.rng.randint(1, self.max_delay)
        due = max(self.t + delay, self.last.get((src, dst), 0))
        self.last[(src, dst)] = due
        self.mid += 1
        self.msgs.append(dict(id=self.mid, src=src, dst=dst, type=typ, ts=ts, sent=self.t, deliver=due))
        self.counts[typ] = self.counts.get(typ, 0) + 1
        self.sent += 1

    def bcast(self, src, typ, ts):
        for d in range(self.n):
            if d != src:
                self.send(src, d, typ, ts)
        self.lg(f"P{src} sends {typ}(ts={ts}) to all")

    def lg(self, text):
        self.log.append(text)

    def request(self, p):
        idx = p.k
        p.k += 1
        p.state, p.dur = "req", self.durs[p.id][idx]
        p.cur = {"pid": p.id, "req": self.t, "enter": None, "exit": None}
        self.records.append(p.cur)
        if self.algo in ("ricart", "lamport"):
            p.clock += 1
            p.req_ts, p.replies = p.clock, set()
            if self.algo == "lamport":
                p.queue.append((p.req_ts, p.id))
            self.lg(f"P{p.id} wants the critical section (ts={p.req_ts})")
            self.bcast(p.id, "REQUEST", p.req_ts)
        elif self.algo == "centralized":
            self.lg(f"P{p.id} wants the critical section")
            self.send(p.id, self.C, "REQUEST")
            self.lg(f"P{p.id} sends REQUEST to coordinator")
        else:
            self.lg(f"P{p.id} wants the critical section, waits for the token")

    def enter(self, p):
        if any(q.state == "cs" for q in self.P if q is not p):
            self.violations += 1
        p.state, p.exit_at = "cs", self.t + p.dur
        p.cur["enter"] = self.t
        self.lg(f"P{p.id} ENTERS the critical section for {p.dur} ticks")

    def exit_cs(self, p):
        p.state = "idle"
        p.cur["exit"] = self.t
        self.lg(f"P{p.id} EXITS the critical section")
        if self.algo == "ricart":
            for d in p.deferred:
                self.send(p.id, d, "REPLY")
            if p.deferred:
                self.lg(f"P{p.id} sends deferred REPLY to " + ", ".join(f"P{d}" for d in p.deferred))
            p.deferred = []
        elif self.algo == "lamport":
            p.queue = [e for e in p.queue if e[1] != p.id]
            self.bcast(p.id, "RELEASE", p.clock)
        elif self.algo == "centralized":
            self.send(p.id, self.C, "RELEASE")
            self.lg(f"P{p.id} sends RELEASE to coordinator")
        else:
            self.pass_token(p)

    def pass_token(self, p):
        nxt = (p.id + 1) % self.n
        p.has_token = False
        self.send(p.id, nxt, "TOKEN")
        self.lg(f"P{p.id} passes the token to P{nxt}")

    def try_lamport(self, p):
        if p.state == "req" and len(p.replies) == self.n - 1 and min(p.queue) == (p.req_ts, p.id):
            self.enter(p)

    def on_msg(self, m):
        typ, src, dst = m["type"], m["src"], m["dst"]
        if self.algo == "centralized" and dst == self.C:
            if typ == "REQUEST":
                if self.holder is None:
                    self.holder = src
                    self.send(self.C, src, "GRANT")
                    self.lg(f"Coordinator grants P{src}")
                else:
                    self.cq.append(src)
                    self.lg(f"Coordinator is busy, queues P{src}")
            else:
                self.holder = None
                self.lg(f"Coordinator receives RELEASE from P{src}")
                if self.cq:
                    nx = self.cq.pop(0)
                    self.holder = nx
                    self.send(self.C, nx, "GRANT")
                    self.lg(f"Coordinator grants P{nx}")
            return
        p = self.P[dst]
        if self.algo == "centralized":
            self.enter(p)
        elif self.algo == "token":
            p.has_token = True
            if p.state == "req":
                self.enter(p)
            else:
                self.pass_token(p)
        elif self.algo == "ricart":
            if typ == "REQUEST":
                p.clock = max(p.clock, m["ts"]) + 1
                if p.state == "cs" or (p.state == "req" and (p.req_ts, p.id) < (m["ts"], src)):
                    p.deferred.append(src)
                    self.lg(f"P{dst} defers REPLY to P{src}")
                else:
                    self.send(dst, src, "REPLY")
                    self.lg(f"P{dst} sends REPLY to P{src}")
            else:
                p.replies.add(src)
                if p.state == "req" and len(p.replies) == self.n - 1:
                    self.enter(p)
        else:
            p.clock = max(p.clock, m["ts"] or 0) + 1
            if typ == "REQUEST":
                p.queue.append((m["ts"], src))
                self.send(dst, src, "REPLY", p.clock)
                self.lg(f"P{dst} queues request of P{src} and sends REPLY")
            elif typ == "REPLY":
                p.replies.add(src)
            else:
                p.queue = [e for e in p.queue if e[1] != src]
            self.try_lamport(p)

    def snapshot(self, arrived):
        in_cs = [p.id for p in self.P if p.state == "cs"]
        if len(in_cs) > 1:
            self.violations += 1
        shown = [dict(src=m["src"], dst=m["dst"], type=m["type"], sent=m["sent"], deliver=m["deliver"], arr=False) for m in self.msgs]
        shown += [dict(src=m["src"], dst=m["dst"], type=m["type"], sent=m["sent"], deliver=m["deliver"], arr=True) for m in arrived]
        f = {"t": self.t, "s": [p.state for p in self.P], "tok": [p.has_token for p in self.P],
             "cs": in_cs, "msgs": shown, "log": self.log, "sent": self.sent}
        if self.algo == "centralized":
            f["coord"] = {"holder": self.holder, "queue": list(self.cq)}
        self.frames.append(f)

    def finished(self):
        served = all(p.k == self.reqs and p.state == "idle" for p in self.P)
        return served and (self.algo == "token" or not self.msgs)

    def run(self):
        if self.algo == "token":
            self.msgs.append(dict(id=0, src=None, dst=0, type="TOKEN", ts=None, sent=0, deliver=0))
        while self.t <= MAX_TICKS:
            self.log = []
            for p in self.P:
                if p.state == "cs" and p.exit_at <= self.t:
                    self.exit_cs(p)
            due = sorted((m for m in self.msgs if m["deliver"] == self.t), key=lambda m: m["id"])
            self.msgs = [m for m in self.msgs if m["deliver"] != self.t]
            for m in due:
                self.on_msg(m)
            for p in self.P:
                if p.state == "idle" and p.k < self.reqs and self.times[p.id][p.k] <= self.t:
                    self.request(p)
            self.snapshot(due)
            if self.finished():
                self.completed = True
                break
            self.t += 1
        self.end_t = self.t
        return self

    def metrics(self):
        rec = [r for r in self.records if r["enter"] is not None]
        waits = [r["enter"] - r["req"] for r in rec]
        order = sorted(rec, key=lambda r: (r["enter"], r["pid"]))
        total = self.sent
        return {
            "ticks": self.end_t, "entries": len(rec), "msgs": total, "counts": self.counts,
            "per_cs": round(total / len(rec), 2) if rec else 0,
            "avg_wait": round(sum(waits) / len(waits), 2) if waits else 0,
            "max_wait": max(waits) if waits else 0,
            "violations": self.violations, "completed": self.completed,
            "order": [r["pid"] for r in order],
            "records": [{"pid": r["pid"], "req": r["req"], "enter": r["enter"], "exit": r["exit"],
                         "wait": (r["enter"] - r["req"]) if r["enter"] is not None else None} for r in self.records],
        }


PAGE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Distributed Mutual Exclusion Simulator</title>
<style>
:root{--bg:#f4f5fb;--card:#fff;--tx:#1b1f2a;--mu:#667085;--bd:#e4e7ee;--pr:#4f46e5;--ok:#15803d;--okb:#dcfce7;--bad:#b91c1c;--badb:#fee2e2;--soft:#f1f0fb}
@media (prefers-color-scheme:dark){:root{--bg:#0f1219;--card:#181c27;--tx:#e8eaf2;--mu:#9aa3b5;--bd:#2a3042;--pr:#818cf8;--ok:#4ade80;--okb:#14351f;--bad:#f87171;--badb:#3b1818;--soft:#222740}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.w{max-width:1000px;margin:0 auto;padding:16px}
header{background:linear-gradient(135deg,#312e81,#6d28d9 60%,#a855f7);color:#fff;border-radius:18px;padding:24px;margin-bottom:16px}
header h1{margin:8px 0 4px;font-size:1.8rem;line-height:1.2}header p{margin:0;opacity:.92}
.tag{display:inline-block;background:rgba(255,255,255,.2);padding:3px 12px;border-radius:99px;font-size:.78rem;font-weight:600}
.c{background:var(--card);border:1px solid var(--bd);border-radius:14px;padding:18px;margin-bottom:16px}
h2{font-size:1.1rem;margin:0 0 12px}
.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px}
label{display:block;font-weight:600;font-size:.85rem;margin-bottom:4px}
select,input[type=number]{width:100%;background:var(--bg);color:var(--tx);border:1px solid var(--bd);border-radius:9px;padding:9px;font:inherit}
input[type=range]{width:100%;accent-color:var(--pr)}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:14px}
button{background:var(--pr);color:#fff;border:0;border-radius:9px;padding:10px 16px;font:inherit;font-weight:600;cursor:pointer}
button.s{background:transparent;color:var(--pr);border:1px solid var(--pr)}
.info{background:var(--soft);border-radius:10px;padding:10px 14px;margin-top:14px;font-size:.92rem}
.banner{border-radius:10px;padding:8px 14px;font-weight:600;margin-bottom:10px;font-size:.92rem}
.safe{background:var(--okb);color:var(--ok)}.unsafe{background:var(--badb);color:var(--bad)}
svg{width:100%;height:auto;max-height:430px;background:var(--soft);border-radius:12px}
.ctl{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:10px}
.ctl input[type=range]{flex:1;min-width:140px}
.ctl button{padding:8px 14px}
.stat{font-size:.9rem;color:var(--mu);margin-top:8px}
.leg{display:flex;gap:14px;flex-wrap:wrap;font-size:.82rem;color:var(--mu);margin-top:8px}
.leg i{display:inline-block;width:11px;height:11px;border-radius:50%;margin-right:5px;vertical-align:-1px}
#log{max-height:260px;overflow:auto;font:13px/1.55 ui-monospace,Consolas,monospace;background:var(--soft);border-radius:10px;padding:10px}
#log .now{background:rgba(79,70,229,.18);border-radius:4px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}
.st{border:1px solid var(--bd);border-radius:12px;padding:10px;text-align:center}
.st b{display:block;font-size:1.6rem}.st span{color:var(--mu);font-size:.8rem}
table{width:100%;border-collapse:collapse;font-size:.9rem}
th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--bd)}
th{color:var(--mu);font-size:.75rem;text-transform:uppercase}
.sc{overflow-x:auto}
.bar{display:flex;align-items:center;gap:8px;margin:5px 0;font-size:.85rem}
.bar span:first-child{width:150px;flex:none}.bar div{height:16px;border-radius:5px;background:var(--pr)}
.sm{color:var(--mu);font-size:.85rem}
</style>
</head>
<body>
<div class="w">
<header>
  <h1>Distributed Mutual Exclusion Simulator</h1>
</header>

<div class="c">
  <h2>Simulation settings</h2>
  <div class="g">
    <div><label for="algo">Algorithm</label>
      <select id="algo">
        <option value="ricart">Ricart-Agrawala</option>
        <option value="lamport">Lamport's Algorithm</option>
        <option value="centralized">Centralized (Coordinator)</option>
        <option value="token">Token Ring</option>
      </select></div>
    <div><label>Processes: <span id="nv">5</span></label><input type="range" id="n" min="3" max="8" value="5"></div>
    <div><label>Requests per process: <span id="rv">2</span></label><input type="range" id="reqs" min="1" max="3" value="2"></div>
    <div><label>Critical section length: <span id="cv">3</span> ticks</label><input type="range" id="cs" min="1" max="5" value="3"></div>
    <div><label>Max message delay: <span id="dv">2</span> ticks</label><input type="range" id="delay" min="1" max="4" value="2"></div>
    <div><label for="seed">Random seed</label><input type="number" id="seed" value="7" min="1" max="99999"></div>
  </div>
  <div class="info" id="info"></div>
  <div class="row">
    <button id="run">Run simulation</button>
    <button class="s" id="shuffle">New random seed</button>
    <button class="s" id="cmp">Compare all algorithms</button>
  </div>
</div>

<div class="c">
  <h2>Live simulation</h2>
  <div class="banner safe" id="banner">Run a simulation to begin</div>
  <svg id="svg" viewBox="0 0 500 400"></svg>
  <div class="leg">
    <span><i style="background:#c7d2fe"></i>Idle</span><span><i style="background:#fbbf24"></i>Requesting</span>
    <span><i style="background:#22c55e"></i>In critical section</span>
    <span><i style="background:#d97706"></i>REQUEST</span><span><i style="background:#2563eb"></i>REPLY</span>
    <span><i style="background:#7c3aed"></i>RELEASE</span><span><i style="background:#16a34a"></i>GRANT</span><span><i style="background:#dc2626"></i>TOKEN</span>
  </div>
  <div class="ctl">
    <button class="s" id="first">|&lt;</button><button class="s" id="prev">&lt;</button>
    <button id="play">Play</button>
    <button class="s" id="next">&gt;</button><button class="s" id="lastb">&gt;|</button>
    <select id="speed" style="width:auto"><option value="900">Slow</option><option value="450" selected>Normal</option><option value="150">Fast</option></select>
    <input type="range" id="sl" min="0" max="0" value="0">
  </div>
  <div class="stat" id="stat"></div>
</div>

<div class="c"><h2>Event log</h2><div id="log"></div></div>

<div class="c">
  <h2>Results</h2>
  <div class="cards" id="cards"></div>
  <p class="sm" id="detail"></p>
  <div class="sc"><table><thead><tr><th>Process</th><th>Requested at</th><th>Entered at</th><th>Exited at</th><th>Waited</th></tr></thead><tbody id="rt"></tbody></table></div>
</div>

<div class="c" id="cmpbox" hidden>
  <h2>Algorithm comparison (same requests for all)</h2>
  <div class="sc"><table><thead><tr><th>Algorithm</th><th>Messages</th><th>Msgs per entry</th><th>Theory</th><th>Avg wait</th><th>Max wait</th><th>Total time</th></tr></thead><tbody id="ct"></tbody></table></div>
  <div id="bars"></div>
</div>
</div>

<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
const INFO={
 ricart:["Ricart-Agrawala","2(N-1) messages per entry","A process multicasts REQUEST with a timestamp. Others reply at once, or defer the reply if they are in the CS or have an older request. It enters after all N-1 replies."],
 lamport:["Lamport's algorithm","3(N-1) messages per entry","Every process keeps a queue ordered by timestamp. It enters when its request is first in the queue and all replies have arrived. Leaving sends RELEASE. Needs FIFO channels."],
 centralized:["Centralized","3 messages per entry","One coordinator grants permission: REQUEST, GRANT, RELEASE. Simple and fast, but the coordinator is a single point of failure."],
 token:["Token ring","1 to N-1 token hops per entry","One token circulates around the ring. Only the holder may enter the CS. No starvation, but the token keeps moving even when nobody wants it."]};
const THEORY=(a,n)=>({ricart:2*(n-1),lamport:3*(n-1),centralized:3,token:"1 to "+(n-1)}[a]);
const COL={REQUEST:"#d97706",REPLY:"#2563eb",RELEASE:"#7c3aed",GRANT:"#16a34a",TOKEN:"#dc2626"};
const SHORT={REQUEST:"REQ",REPLY:"REP",RELEASE:"REL",GRANT:"GRANT",TOKEN:"TOKEN"};
let R=null,cur=0,timer=null,logAll=[];
const P=()=>({algo:$("algo").value,n:+$("n").value,reqs:+$("reqs").value,cs:+$("cs").value,delay:+$("delay").value,seed:+$("seed").value});
async function post(url){const r=await fetch(url,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(P())});if(!r.ok)throw new Error("bad");return r.json()}
function stop(){clearInterval(timer);timer=null;$("play").textContent="Play"}
function showInfo(){const i=INFO[$("algo").value];$("info").innerHTML="<b>"+i[0]+"</b> &middot; "+i[1]+"<br>"+i[2]}
[["n","nv"],["reqs","rv"],["cs","cv"],["delay","dv"]].forEach(([a,b])=>$(a).oninput=()=>$(b).textContent=$(a).value);
$("algo").onchange=showInfo;

async function run(){
  stop();
  try{R=await post("/simulate")}catch(e){alert("Could not reach the Python server. Is app.py running?");return}
  cur=0;$("sl").max=R.frames.length-1;$("sl").value=0;
  logAll=[];R.frames.forEach(f=>f.log.forEach(x=>logAll.push({t:f.t,x})));
  showResults();render();
}
function pos(i,n){const a=-Math.PI/2+i*2*Math.PI/n;return[250+145*Math.cos(a),200+145*Math.sin(a)]}
const line=(a,b)=>`<line x1="${a[0]}" y1="${a[1]}" x2="${b[0]}" y2="${b[1]}" style="stroke:var(--bd)" stroke-width="2"/>`;
function render(){
  if(!R)return;
  const f=R.frames[cur],n=R.n,al=R.algo,pts=[...Array(n)].map((_,i)=>pos(i,n)),ctr=[250,200];
  let s="";
  if(al==="centralized")pts.forEach(p=>s+=line(p,ctr));
  else if(al==="token")pts.forEach((p,i)=>s+=line(p,pts[(i+1)%n]));
  else for(let i=0;i<n;i++)for(let j=i+1;j<n;j++)s+=line(pts[i],pts[j]);
  if(al==="centralized"){
    const q=f.coord.queue.length?f.coord.queue.map(x=>"P"+x).join(", "):"empty";
    s+=`<circle cx="250" cy="200" r="30" fill="#6d28d9"/><text x="250" y="206" text-anchor="middle" fill="#fff" font-size="18" font-weight="700">C</text>`;
    s+=`<text x="250" y="248" text-anchor="middle" style="fill:var(--tx)" font-size="11">queue: ${q}</text>`;
  }else{
    const occ=f.cs.length?"P"+f.cs.join(", P"):"free";
    s+=`<rect x="190" y="178" width="120" height="44" rx="10" style="fill:${f.cs.length?"#22c55e":"var(--card)"};stroke:var(--bd)"/>`;
    s+=`<text x="250" y="196" text-anchor="middle" font-size="10" style="fill:${f.cs.length?"#fff":"var(--mu)"}">CRITICAL SECTION</text>`;
    s+=`<text x="250" y="213" text-anchor="middle" font-size="14" font-weight="700" style="fill:${f.cs.length?"#fff":"var(--tx)"}">${occ}</text>`;
  }
  pts.forEach((p,i)=>{
    const st=f.s[i],fill=st==="cs"?"#22c55e":st==="req"?"#fbbf24":"#c7d2fe",tc=st==="cs"?"#fff":"#1e1b4b";
    const ring=f.tok[i]?`stroke="#dc2626" stroke-width="5"`:`stroke="#6366f1" stroke-width="2"`;
    s+=`<circle cx="${p[0]}" cy="${p[1]}" r="26" fill="${fill}" ${ring}/>`;
    s+=`<text x="${p[0]}" y="${p[1]+6}" text-anchor="middle" font-size="16" font-weight="700" fill="${tc}">P${i}</text>`;
    if(f.tok[i])s+=`<text x="${p[0]}" y="${p[1]-32}" text-anchor="middle" font-size="10" font-weight="700" fill="#dc2626">TOKEN</text>`;
  });
  f.msgs.forEach(m=>{
    if(m.src===null)return;
    const a=m.src===n?ctr:pts[m.src],b=m.dst===n?ctr:pts[m.dst];
    const pr=m.arr?1:(m.deliver>m.sent?(f.t-m.sent)/(m.deliver-m.sent):1);
    const x=a[0]+(b[0]-a[0])*pr,y=a[1]+(b[1]-a[1])*pr;
    s+=`<circle cx="${x}" cy="${y}" r="7" fill="${COL[m.type]}" stroke="#fff" stroke-width="1.5"/>`;
    s+=`<text x="${x}" y="${y-10}" text-anchor="middle" font-size="9" font-weight="700" style="fill:var(--tx)">${SHORT[m.type]}</text>`;
  });
  $("svg").innerHTML=s;
  const last=R.frames.length-1;
  $("stat").textContent=`Time t = ${f.t} (of ${R.frames[last].t})   |   In critical section: ${f.cs.length?"P"+f.cs.join(", P"):"none"}   |   Messages sent so far: ${f.sent}`;
  $("sl").value=cur;
  const lines=logAll.filter(l=>l.t<=f.t).slice(-60);
  $("log").innerHTML=lines.map(l=>`<div class="${l.t===f.t?"now":""}"><b>t=${l.t}</b>&nbsp; ${esc(l.x)}</div>`).join("");
  $("log").scrollTop=$("log").scrollHeight;
}
function showResults(){
  const m=R.metrics;
  $("banner").className="banner "+(m.violations===0?"safe":"unsafe");
  $("banner").textContent=m.violations===0?"Mutual exclusion SAFE: never more than one process in the critical section":"Mutual exclusion VIOLATED "+m.violations+" time(s)";
  $("cards").innerHTML=[["Total time",m.ticks+" ticks"],["CS entries",m.entries],["Messages",m.msgs],["Msgs per entry",m.per_cs],["Avg wait",m.avg_wait],["Max wait",m.max_wait]].map(x=>`<div class="st"><b>${x[1]}</b><span>${x[0]}</span></div>`).join("");
  $("detail").innerHTML="<b>Messages by type:</b> "+Object.entries(m.counts).map(([k,v])=>k+" "+v).join(", ")+"<br><b>Order of entry:</b> "+m.order.map(p=>"P"+p).join(" &rarr; ")+(m.completed?"":"<br><b>Warning:</b> simulation stopped before all requests were served.");
  $("rt").innerHTML=m.records.map(r=>`<tr><td>P${r.pid}</td><td>${r.req}</td><td>${r.enter}</td><td>${r.exit}</td><td>${r.wait}</td></tr>`).join("");
}
async function compare(){
  try{
    const d=await post("/compare");$("cmpbox").hidden=false;const n=P().n;
    $("ct").innerHTML=d.results.map(r=>`<tr><td>${r.name}</td><td>${r.msgs}</td><td>${r.per_cs}</td><td>${THEORY(r.algo,n)}</td><td>${r.avg_wait}</td><td>${r.max_wait}</td><td>${r.ticks}</td></tr>`).join("");
    const chart=(t,k)=>{const mx=Math.max(...d.results.map(r=>r[k]),1);return `<h3 style="font-size:.95rem;margin:14px 0 4px">${t}</h3>`+d.results.map(r=>`<div class="bar"><span>${r.name}</span><div style="width:${r[k]/mx*60}%"></div><span>${r[k]}</span></div>`).join("")};
    $("bars").innerHTML=chart("Messages per critical-section entry (lower is cheaper)","per_cs")+chart("Average waiting time in ticks (lower is faster)","avg_wait")+chart("Total time to finish all requests","ticks");
    $("cmpbox").scrollIntoView({behavior:"smooth"});
  }catch(e){alert("Could not reach the Python server. Is app.py running?")}
}
$("run").onclick=run;$("cmp").onclick=compare;
$("shuffle").onclick=()=>{$("seed").value=1+Math.floor(Math.random()*9999);run()};
$("play").onclick=()=>{
  if(!R)return;
  if(timer){stop();return}
  if(cur>=R.frames.length-1)cur=0;
  $("play").textContent="Pause";
  timer=setInterval(()=>{if(cur<R.frames.length-1){cur++;render()}else stop()},+$("speed").value);
};
$("speed").onchange=()=>{if(timer){stop();$("play").click()}};
$("next").onclick=()=>{stop();if(R&&cur<R.frames.length-1){cur++;render()}};
$("prev").onclick=()=>{stop();if(R&&cur>0){cur--;render()}};
$("first").onclick=()=>{stop();cur=0;render()};
$("lastb").onclick=()=>{stop();if(R){cur=R.frames.length-1;render()}};
$("sl").oninput=()=>{stop();cur=+$("sl").value;render()};
showInfo();run();
</script>
</body>
</html>
'''

def params(data):
    def num(key, default, lo, hi):
        try:
            return max(lo, min(hi, int(data.get(key, default))))
        except (TypeError, ValueError):
            return default
    algo = data.get("algo") if data.get("algo") in ALGOS else "ricart"
    return (algo, num("n", 5, 3, 8), num("reqs", 2, 1, 3), num("cs", 3, 1, 5),
            num("delay", 2, 1, 4), num("seed", 7, 1, 99999))


@app.route("/")
def home():
    return PAGE


@app.route("/simulate", methods=["POST"])
def simulate():
    algo, n, reqs, cs, delay, seed = params(request.get_json(silent=True) or {})
    sim = Sim(algo, n, reqs, cs, delay, seed).run()
    return jsonify({"algo": algo, "name": ALGOS[algo], "n": n, "frames": sim.frames, "metrics": sim.metrics()})


@app.route("/compare", methods=["POST"])
def compare():
    _, n, reqs, cs, delay, seed = params(request.get_json(silent=True) or {})
    results = []
    for algo, name in ALGOS.items():
        m = Sim(algo, n, reqs, cs, delay, seed).run().metrics()
        results.append({"algo": algo, "name": name, "msgs": m["msgs"], "per_cs": m["per_cs"],
                        "avg_wait": m["avg_wait"], "max_wait": m["max_wait"], "ticks": m["ticks"],
                        "violations": m["violations"]})
    return jsonify({"results": results})


if __name__ == "__main__":
    threading.Timer(1.2, lambda: webbrowser.open("http://127.0.0.1:5055")).start()
    app.run(port=5055, debug=False)