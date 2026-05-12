// ============================================================
// VERAPIMPO — Polymarket Intelligence Agent v2.0
// 6-Agent Consensus | 3-Layer Early Warning | Paper Trading
// ============================================================

"use strict";

const { Telegraf } = require("telegraf");
const Database     = require("better-sqlite3");
const cron         = require("node-cron");
const express      = require("express");
const cors         = require("cors");
const fs           = require("fs");
const path         = require("path");

// ── CONFIG ────────────────────────────────────────────────
const PAPER = true; // flip to false to go live

const cfg = {
  telegram : process.env.VERAPIMPO_TELEGRAM_TOKEN || "",
  chatId   : process.env.VERAPIMPO_CHAT_ID        || "",
  chatId2  : process.env.VERAPIMPO_CHAT_ID_2      || "",
  dashPass : process.env.DASHBOARD_PASSWORD        || "verapimpo2026",
};

const TG_CHANNELS = [
  { username:"Whale200",  type:"whale", label:"Whale200"  },
  { username:"burjbnews", type:"news",  label:"Burj News" },
];

// ── GUARDRAILS ────────────────────────────────────────────
const G = {
  BUDGET:500, MAX_POS:50, MAX_POSITIONS:8, LOSS_HALT:150,
  BLACKOUT_HRS:2, CEIL:0.85, FLOOR:0.15, MIN_LIQ:10000,
  MAX_MKT_SHARE:0.03, MIN_EDGE:0.05, MAX_DAILY:10,
  MAX_CAT_DEP:0.30, EXIT_TARGET:0.85, VOL_SPIKE:3.0, STOP_PCT:0.15,
};

// ── DATABASE ──────────────────────────────────────────────
const db = new Database("./verapimpo.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT, market_question TEXT, category TEXT, side TEXT DEFAULT 'YES',
    entry_price REAL, current_price REAL, size REAL, shares REAL,
    stop_price REAL, target_price REAL, agents_fired TEXT, layer TEXT, tier INTEGER,
    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0,
    exit_reason TEXT, opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closes_at DATETIME, closed_at DATETIME
  );
  CREATE TABLE IF NOT EXISTS whales (
    id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT UNIQUE, rank INTEGER,
    win_rate REAL, avg_profit REAL, total_trades INTEGER, recent_trades INTEGER,
    profit_30d REAL, tier INTEGER, days_in_top50 INTEGER DEFAULT 0,
    confirmed INTEGER DEFAULT 0, last_seen DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS whale_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT, whale_address TEXT, market_id TEXT,
    market_question TEXT, side TEXT, price REAL, size REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, headline TEXT, summary TEXT,
    url TEXT, image_url TEXT, published_at DATETIME, market_id TEXT,
    market_question TEXT, status TEXT DEFAULT 'monitored',
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY, question TEXT, category TEXT,
    yes_price REAL, no_price REAL, volume REAL,
    end_date DATETIME, score REAL DEFAULT 0, last_scanned DATETIME
  );
  CREATE TABLE IF NOT EXISTS tg_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT, channel_type TEXT,
    message_id TEXT, text TEXT, date DATETIME, market_id TEXT,
    relevance_score REAL DEFAULT 0, fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel, message_id)
  );
  CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, message TEXT, market_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS agent_state (key TEXT PRIMARY KEY, value TEXT);
`);

// Pre-compiled statements
const stmt = {
  getState    : db.prepare("SELECT value FROM agent_state WHERE key=?"),
  setState    : db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)"),
  openTrades  : db.prepare("SELECT * FROM paper_trades WHERE status='open'"),
  deployed    : db.prepare("SELECT COALESCE(SUM(size),0) as t FROM paper_trades WHERE status='open'"),
  catDeployed : db.prepare("SELECT COALESCE(SUM(size),0) as t FROM paper_trades WHERE status='open' AND category=?"),
  closedCount : db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed'"),
  winsCount   : db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0"),
  insertTrade : db.prepare("INSERT INTO paper_trades (market_id,market_question,category,side,entry_price,current_price,size,shares,stop_price,target_price,agents_fired,layer,tier,closes_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"),
  updatePrice : db.prepare("UPDATE paper_trades SET current_price=? WHERE id=?"),
  closeTrade  : db.prepare("UPDATE paper_trades SET status='closed',pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?"),
  upsertMkt   : db.prepare("INSERT OR REPLACE INTO markets (id,question,category,yes_price,no_price,volume,end_date,score,last_scanned) VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)"),
  cachedMkts  : db.prepare("SELECT * FROM markets ORDER BY score DESC, last_scanned DESC LIMIT 35"),
  whalesConf  : db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank"),
  logAlert    : db.prepare("INSERT INTO alerts (type,message,market_id) VALUES (?,?,?)"),
  newsExists  : db.prepare("SELECT id FROM news_items WHERE source=? AND headline=?"),
  insertNews  : db.prepare("INSERT INTO news_items (source,headline,summary,url,image_url,published_at,market_id,market_question,status) VALUES (?,?,?,?,?,?,?,?,?)"),
  updateNewsStatus : db.prepare("UPDATE news_items SET status='triggered' WHERE market_id=? AND status='monitored'"),
  tgRecent    : db.prepare("SELECT * FROM tg_messages WHERE fetched_at > datetime('now','-6 hours') ORDER BY relevance_score DESC LIMIT 100"),
};

const getState = (k,d=null) => { const r=stmt.getState.get(k); return r?JSON.parse(r.value):d; };
const setState = (k,v)      => stmt.setState.run(k,JSON.stringify(v));

["total_loss","total_pnl"].forEach(k => { if(getState(k)===null) setState(k,0); });
if(getState("paused")===null)       setState("paused",false);
if(getState("daily_trades")===null) setState("daily_trades",0);
if(!getState("trade_date"))         setState("trade_date","");

// Dedupe news on boot
try { db.exec("DELETE FROM news_items WHERE id NOT IN (SELECT MIN(id) FROM news_items GROUP BY source,headline)"); } catch {}

// ── POSITION BACKUP ───────────────────────────────────────
const BACKUP = "./verapimpo_backup.json";

function saveFullBackup() {
  try {
    const backup = {
      open_positions : stmt.openTrades.all(),
      closed_trades  : db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 500").all(),
      agent_state    : db.prepare("SELECT * FROM agent_state").all(),
      whales         : db.prepare("SELECT * FROM whales ORDER BY rank").all(),
      saved_at       : new Date().toISOString(),
    };
    fs.writeFileSync(BACKUP, JSON.stringify(backup));
  } catch(e) { console.error("Backup:",e.message); }
}

// Keep alias for call sites
const savePositionBackup = saveFullBackup;

function restoreFromBackup() {
  try {
    if (!fs.existsSync(BACKUP)) { console.log("No backup found."); return 0; }
    const backup = JSON.parse(fs.readFileSync(BACKUP,"utf8"));
    let restored = 0;

    // Restore agent state (P&L, losses, counters)
    if (backup.agent_state?.length) {
      for (const s of backup.agent_state) {
        const existing = stmt.getState.get(s.key);
        if (!existing) stmt.setState.run(s.key, s.value);
      }
      console.log("Agent state restored");
    }

    // Restore closed trades (history)
    if (backup.closed_trades?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO paper_trades (id,market_id,market_question,category,side,entry_price,current_price,size,shares,stop_price,target_price,agents_fired,layer,tier,status,pnl,pnl_pct,exit_reason,closes_at,opened_at,closed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      for (const t of backup.closed_trades) {
        ins.run(t.id,t.market_id,t.market_question,t.category||"general",t.side||"YES",
                t.entry_price,t.current_price||t.entry_price,t.size,t.shares,
                t.stop_price,t.target_price,t.agents_fired,t.layer,t.tier,
                "closed",t.pnl||0,t.pnl_pct||0,t.exit_reason,t.closes_at,t.opened_at,t.closed_at);
      }
      console.log("Closed trades restored:", backup.closed_trades.length);
    }

    // Restore open positions
    if (backup.open_positions?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO paper_trades (id,market_id,market_question,category,side,entry_price,current_price,size,shares,stop_price,target_price,agents_fired,layer,tier,status,pnl,pnl_pct,closes_at,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      for (const p of backup.open_positions) {
        ins.run(p.id,p.market_id,p.market_question,p.category||"general",p.side||"YES",
                p.entry_price,p.current_price||p.entry_price,p.size,p.shares,
                p.stop_price,p.target_price,p.agents_fired,p.layer,p.tier,
                "open",0,0,p.closes_at,p.opened_at);
        restored++;
      }
      console.log("Open positions restored:", restored);
    }

    // Restore whale rankings
    if (backup.whales?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO whales (address,rank,win_rate,avg_profit,total_trades,recent_trades,profit_30d,tier,days_in_top50,confirmed,last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
      for (const w of backup.whales) {
        ins.run(w.address,w.rank,w.win_rate,w.avg_profit,w.total_trades,w.recent_trades,w.profit_30d,w.tier,w.days_in_top50,w.confirmed,w.last_seen);
      }
      console.log("Whales restored:", backup.whales.length);
    }

    return restored;
  } catch(e) { console.error("Restore:",e.message); return 0; }
}

const restored = restoreFromBackup();

// ── HELPERS ───────────────────────────────────────────────
const POLY_HDR = {
  "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
  "Accept":"application/json, text/plain, */*",
  "Origin":"https://polymarket.com",
  "Referer":"https://polymarket.com/",
};

async function safeFetch(url, extraHeaders={}, timeout=10000) {
  const ctrl = new AbortController();
  const t = setTimeout(()=>ctrl.abort(), timeout);
  try {
    const res = await fetch(url, {signal:ctrl.signal, headers:{...POLY_HDR,...extraHeaders}});
    if (!res.ok) { console.log(`HTTP ${res.status} ${url.slice(0,70)}`); return null; }
    return await res.json();
  } catch { return null; }
  finally { clearTimeout(t); }
}

async function safeFetchText(url, timeout=8000) {
  const ctrl = new AbortController();
  const t = setTimeout(()=>ctrl.abort(), timeout);
  try { return await (await fetch(url,{signal:ctrl.signal})).text(); }
  catch { return ""; }
  finally { clearTimeout(t); }
}

const delay = ms => new Promise(r=>setTimeout(r,ms));

function stripHtml(s) {
  return (s||"").replace(/<[^>]+>/g," ").replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/\s+/g," ").trim();
}

function extractPrice(m) {
  try {
    const op = m.outcomePrices || m._yesPrice || m.yes_price;
    if (typeof op==="string")  return parseFloat(JSON.parse(op)[0])||0;
    if (Array.isArray(op))     return parseFloat(op[0])||0;
    if (typeof op==="number")  return op;
    return parseFloat(m.bestBid||m.lastTradePrice||m.yes_price||0);
  } catch { return parseFloat(m.bestBid||m.yes_price||0); }
}

const getOpenTrades  = ()    => stmt.openTrades.all();
const getDeployed    = ()    => stmt.deployed.get().t||0;
const getCatDeployed = (cat) => stmt.catDeployed.get(cat).t||0;
const logAlert       = (type,msg,mid="") => { try{stmt.logAlert.run(type,msg,mid);}catch{} };

function resetDailyCounters() {
  const today = new Date().toDateString();
  if (getState("trade_date")!==today) { setState("trade_date",today); setState("daily_trades",0); }
}

function normalizeMarket(m) {
  return { ...m, _yesPrice:extractPrice(m), _liq:parseFloat(m.liquidity||m.liquidityNum||0),
    _endDate:m.endDate||m.end_date_iso||m.endDateIso||null };
}

// ── TELEGRAM ──────────────────────────────────────────────
const bot = cfg.telegram ? new Telegraf(cfg.telegram) : null;
const isAuthorized = id => [cfg.chatId,cfg.chatId2].filter(Boolean).map(String).includes(String(id));

async function tg(msg) {
  if (!bot) return;
  for (const id of [cfg.chatId,cfg.chatId2].filter(Boolean)) {
    try { await bot.telegram.sendMessage(id, msg, {parse_mode:"HTML"}); }
    catch(e) { console.error("TG:",e.message); }
  }
}

// ── POLYMARKET API ────────────────────────────────────────
const GAMMA = "https://gamma-api.polymarket.com";

async function getMarkets(limit=100, offset=0) {
  const url  = `${GAMMA}/markets?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`;
  const data = await safeFetch(url,{},12000);
  if (Array.isArray(data)&&data.length) return data;
  const url2  = `${GAMMA}/events?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`;
  const data2 = await safeFetch(url2,{},12000);
  if (!Array.isArray(data2)) return [];
  const mkts=[];
  for (const ev of data2) { if(Array.isArray(ev.markets)) mkts.push(...ev.markets); else if(ev.id) mkts.push(ev); }
  return mkts;
}

async function getMarketById(id) { return safeFetch(`${GAMMA}/markets/${id}`,{},8000); }
async function getRecentTrades(id,limit=30) { const d=await safeFetch(`${GAMMA}/trades?market=${id}&limit=${limit}`,{},8000); return Array.isArray(d)?d:[]; }

// ── LAYER 1: ORDER BOOK ───────────────────────────────────
async function detectOrderBookPressure(market) {
  try {
    let tokenId=null;
    const raw=market.clobTokenIds;
    if (typeof raw==="string") tokenId=JSON.parse(raw)?.[0];
    else if (Array.isArray(raw)) tokenId=raw[0];
    if (!tokenId||tokenId==="[") return {pressure:0,signal:false};
    const book=await safeFetch(`https://clob.polymarket.com/book?token_id=${tokenId}`,{},6000);
    if (!book?.bids?.length||!book?.asks?.length) return {pressure:0,signal:false};
    const bidVol=book.bids.slice(0,5).reduce((s,b)=>s+parseFloat(b.size||0),0);
    const askVol=book.asks.slice(0,5).reduce((s,a)=>s+parseFloat(a.size||0),0);
    const spread=parseFloat(book.asks[0]?.price||0)-parseFloat(book.bids[0]?.price||0);
    const pressure=bidVol/(askVol||1);
    return {pressure:+pressure.toFixed(2),spread:+spread.toFixed(4),signal:pressure>1.8&&spread<0.03};
  } catch { return {pressure:0,signal:false}; }
}

// ── LAYER 2: WHALE ACTIVITY ───────────────────────────────
async function getWhaleActivity(market) {
  try {
    const trades=await getRecentTrades(market.id,30);
    if (!trades.length) return {whales:[],clustering:0,uniqueWhales:[]};
    const confirmed=stmt.whalesConf.all();
    if (!confirmed.length) return {whales:[],clustering:0,uniqueWhales:[]};
    const whaleMap=new Map(confirmed.map(w=>[w.address.toLowerCase(),w]));
    const hits=[]; const cutoff=Date.now()-600000;
    for (const t of trades) {
      if (new Date(t.timestamp||t.created_at).getTime()<cutoff) continue;
      for (const addr of [t.maker,t.taker].filter(Boolean)) {
        const whale=whaleMap.get(addr.toLowerCase());
        if (whale) {
          hits.push({whale,side:t.side,price:parseFloat(t.price||0),size:parseFloat(t.size||0)});
          try { db.prepare("INSERT OR IGNORE INTO whale_trades (whale_address,market_id,market_question,side,price,size) VALUES (?,?,?,?,?,?)").run(addr,market.id,market.question,t.side,t.price,t.size); } catch {}
        }
      }
    }
    const uniqueWhales=[...new Set(hits.map(h=>h.whale.address))];
    return {whales:hits,clustering:uniqueWhales.length,uniqueWhales};
  } catch { return {whales:[],clustering:0,uniqueWhales:[]}; }
}

function detectMarketDivergence(market, allMarkets) {
  const q=( market.question||"").toLowerCase();
  const price=market._yesPrice||0.5;
  const words=q.split(" ").filter(w=>w.length>4);
  let div=0;
  for (const m of allMarkets) {
    if (m.id===market.id) continue;
    if (!words.some(w=>(m.question||"").toLowerCase().includes(w))) continue;
    const gap=Math.abs(price-(m._yesPrice||0.5));
    if (gap>0.12) div+=gap;
    if (div>0.5) break;
  }
  return {divergence:+div.toFixed(3),signal:div>0.15};
}

async function checkResolutionSource(market) {
  const q=(market.question||"").toLowerCase();
  if (q.includes("bitcoin")||q.includes("btc")||q.includes("ethereum")) {
    const ids=q.includes("ethereum")?"ethereum":"bitcoin";
    const cg=await safeFetch(`https://api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd`,{},6000);
    const cur=cg?.bitcoin?.usd||cg?.ethereum?.usd;
    if (cur) {
      const match=q.match(/\$?([\d,]+)k?/);
      if (match) {
        const thr=parseFloat(match[1].replace(",",""))*(match[0].includes("k")?1000:1);
        return {signal:cur>=thr,edge:0.15,detail:`${ids} $${cur.toLocaleString()} vs $${thr.toLocaleString()}`};
      }
    }
  }
  return {signal:false,edge:0};
}

// ── NEWS CACHE ────────────────────────────────────────────
const NEWS_SOURCES = [
  {name:"AP News",      url:"https://rsshub.app/apnews/topics/apf-politics"},
  {name:"Reuters",      url:"https://feeds.reuters.com/reuters/businessNews"},
  {name:"BBC Business", url:"http://feeds.bbci.co.uk/news/business/rss.xml"},
  {name:"Politico",     url:"https://www.politico.com/rss/politicopicks.xml"},
  {name:"CoinDesk",     url:"https://www.coindesk.com/arc/outboundfeeds/rss/"},
  {name:"ESPN",         url:"https://www.espn.com/espn/rss/news"},
];

let newsCache=[]; let newsCacheAt=0; const NEWS_TTL=5*60*1000;

async function refreshNewsCache() {
  if (Date.now()-newsCacheAt<NEWS_TTL&&newsCache.length) return;
  const items=[];
  for (const src of NEWS_SOURCES) {
    try {
      const xml=await safeFetchText(src.url);
      if (!xml) continue;
      for (const m of (xml.match(/<item>[\s\S]*?<\/item>/g)||[]).slice(0,10)) {
        const title =stripHtml(m.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||"");
        const desc  =stripHtml(m.match(/<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/description>/)?.[1]||"").slice(0,200);
        const link  =m.match(/<link>(.*?)<\/link>/)?.[1]||"";
        const imgUrl=m.match(/<media:content[^>]+url="([^"]+)"/)?.[1]||"";
        const pubDate=m.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
        if (title.length>5) items.push({source:src.name,title:title.toLowerCase(),desc:desc.toLowerCase(),link,imgUrl,pubDate});
      }
    } catch {}
    await delay(200);
  }
  newsCache=items; newsCacheAt=Date.now();
  console.log("News cache:",items.length,"articles");
}

function matchNews(market) {
  const q=(market.question||"").toLowerCase();
  const words=q.split(" ").filter(w=>w.length>4&&!["will","have","does","than","from","that","this","with","what","when","where","which"].includes(w));
  if (!words.length) return [];
  const hits=[];
  for (const item of newsCache) {
    const full=item.title+" "+item.desc;
    if (words.filter(w=>full.includes(w)).length>=2) { hits.push(item); if(hits.length>=5) break; }
  }
  for (const h of hits) {
    if (!stmt.newsExists.get(h.source,h.title)) {
      try { stmt.insertNews.run(h.source,h.title,h.desc,h.link,h.imgUrl,h.pubDate,market.id,market.question,"monitored"); } catch {}
    }
  }
  return hits;
}

// ── AGENT 1: WHALE COPY ───────────────────────────────────
function agentWhaleCopy(wa) {
  if (!wa.whales?.length) return {vote:false,confidence:0,detail:"No whales"};
  const best=[...wa.whales].sort((a,b)=>a.whale.tier-b.whale.tier)[0];
  if (!best) return {vote:false,confidence:0,detail:"No whales"};
  const tier=best.whale.tier;
  const conf=Math.min(1,(tier===1?0.9:tier===2?0.7:0.5)+(wa.clustering>=3?0.15:wa.clustering>=2?0.08:0));
  return {vote:true,confidence:conf,tier,clustering:wa.clustering,size_mult:tier===1?0.5:tier===2?0.25:0.15,detail:`T${tier} whale | ${(best.whale.win_rate*100).toFixed(0)}% WR`};
}

// ── AGENT 2: STEALTH ──────────────────────────────────────
function agentStealth(wa, newsVote, sentVote) {
  if (!wa.whales?.length||newsVote||sentVote) return {vote:false,confidence:0,detail:"Not stealthy"};
  const best=[...wa.whales].sort((a,b)=>a.whale.tier-b.whale.tier)[0];
  const conf=best?.whale.tier===1?0.85:best?.whale.tier===2?0.65:0.45;
  return {vote:conf>0.5,confidence:conf,detail:`STEALTH T${best?.whale.tier}`};
}

// ── AGENT 3: NEWS ─────────────────────────────────────────
function agentNewscat(market) {
  const hits=matchNews(market);
  if (!hits.length) return {vote:false,confidence:0,detail:"No news"};
  const now=Date.now();
  const recent=hits.filter(h=>new Date(h.pubDate)>new Date(now-6*3600000)).length;
  const vRecent=hits.filter(h=>new Date(h.pubDate)>new Date(now-3600000)).length;
  const conf=Math.min(0.9,recent*0.15+vRecent*0.25);
  return {vote:conf>0.2,confidence:conf,detail:`${hits.length} articles | ${vRecent} <1h`};
}

// ── AGENT 4: SENTIMENT ────────────────────────────────────
let apeCache=[]; let apeCacheAt=0; const APE_TTL=10*60*1000;

async function agentSentiment(market) {
  try {
    if (Date.now()-apeCacheAt>APE_TTL||!apeCache.length) {
      const [s,c]=await Promise.all([
        safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"),
        safeFetch("https://apewisdom.io/api/v1.0/filter/all-crypto/page/1"),
      ]);
      apeCache=[...(s?.results||[]),...(c?.results||[])]; apeCacheAt=Date.now();
    }
    const q=(market.question||"").toLowerCase();
    const words=q.split(" ").filter(w=>w.length>3);
    let sentiment=0.5;
    for (const r of apeCache) {
      const ticker=(r.ticker||"").toLowerCase();
      if (words.some(w=>ticker.includes(w)||w.includes(ticker))) {
        sentiment=Math.min(0.9,0.5+(50-Math.min(r.rank,50))/100); break;
      }
    }
    const pos=["win","beats","rises","leads","gains"].filter(w=>q.includes(w)).length;
    const neg=["loses","drops","falls","misses","fails"].filter(w=>q.includes(w)).length;
    sentiment=Math.max(0.1,Math.min(0.9,sentiment+(pos-neg)*0.05));
    return {vote:sentiment>0.55,confidence:sentiment,detail:`Sentiment ${(sentiment*100).toFixed(0)}%`};
  } catch { return {vote:true,confidence:0.5,detail:"Neutral"}; }
}

// ── AGENT 5: TECHNICAL ────────────────────────────────────
function agentTechnical(market) {
  const price=market._yesPrice||extractPrice(market);
  const vol=parseFloat(market.volume||0);
  const vol24=parseFloat(market.volume24hr||0);
  const liq=market._liq||parseFloat(market.liquidity||0);
  if (!price||price<=0) return {vote:false,confidence:0,detail:"No price"};
  const momentum=vol24>0&&vol>0?vol24/(vol/30):0;
  const score=[momentum>1.5,price>=G.FLOOR&&price<=G.CEIL,price<0.70,liq>=G.MIN_LIQ].filter(Boolean).length/4;
  return {vote:score>=0.6,confidence:score,price,detail:`${(price*100).toFixed(1)}% | Liq $${liq.toLocaleString()} | Mom ${momentum.toFixed(2)}x`};
}

// ── AGENT 6: RESOLUTION MATH ─────────────────────────────
function agentResolutionMath(market) {
  const price=market._yesPrice||extractPrice(market);
  const endDate=market._endDate?new Date(market._endDate):null;
  const daysLeft=endDate?(endDate-new Date())/86400000:999;
  const vol=parseFloat(market.volume||0);
  if (daysLeft<0) return {vote:false,confidence:0,detail:"Resolved"};
  if (daysLeft<G.BLACKOUT_HRS/24) return {vote:false,confidence:0,detail:"Blackout"};
  const score=[Math.abs(price-0.5)<0.25,daysLeft>=1&&daysLeft<=30,vol<50000,daysLeft>G.BLACKOUT_HRS/24,price<0.5&&(0.75-price)/price>G.MIN_EDGE].filter(Boolean).length/5;
  return {vote:score>=0.6,confidence:score,daysLeft:+daysLeft.toFixed(1),detail:`${daysLeft.toFixed(1)}d | ${(price*100).toFixed(1)}%`};
}

// ── CONSENSUS ─────────────────────────────────────────────
function runConsensus(agents, whaleRes, stealthRes) {
  const votes=agents.filter(a=>a.vote).length; const total=agents.length;
  const tier=whaleRes?.tier||99;
  if (tier===1&&whaleRes?.vote) return {action:"ENTER",size_mult:0.5,label:"TIER1_WHALE",votes,total};
  if (stealthRes?.vote&&tier<=2) return {action:"ENTER",size_mult:0.25,label:"STEALTH",votes,total};
  if (votes>=4) return {action:"ENTER",size_mult:1.0,label:votes>=5?"FULL_CONSENSUS":"STRONG",votes,total};
  if (votes>=3) return {action:"ENTER",size_mult:0.5,label:"MODERATE",votes,total};
  return {action:"SKIP",size_mult:0,label:"INSUFFICIENT",votes,total};
}

// ── GUARDRAILS ────────────────────────────────────────────
function checkGuardrails(market, consensus) {
  resetDailyCounters();
  const open=getOpenTrades(); const deployed=getDeployed();
  const price=market._yesPrice||extractPrice(market);
  const liq=market._liq||parseFloat(market.liquidity||0);
  const endDate=market._endDate?new Date(market._endDate):new Date(0);
  const hoursLeft=(endDate-new Date())/3600000;
  if (getState("paused"))                          return {ok:false,reason:"Paused"};
  if ((getState("total_loss")||0)>=G.LOSS_HALT)    return {ok:false,reason:"Loss halt"};
  if (open.length>=G.MAX_POSITIONS)                return {ok:false,reason:"Max positions"};
  if (open.find(t=>t.market_id===market.id))       return {ok:false,reason:"Already open"};
  if ((getState("daily_trades")||0)>=G.MAX_DAILY)  return {ok:false,reason:"Daily limit"};
  if (price>=G.CEIL||price<=G.FLOOR)               return {ok:false,reason:`Price OOB`};
  if (liq<G.MIN_LIQ)                               return {ok:false,reason:"Low liquidity"};
  if (hoursLeft<G.BLACKOUT_HRS)                    return {ok:false,reason:"Blackout"};
  const catDep=getCatDeployed(market.category||"general");
  if (catDep>=G.BUDGET*G.MAX_CAT_DEP)              return {ok:false,reason:"Category cap"};
  const size=Math.min(G.MAX_POS,(G.BUDGET-deployed)*consensus.size_mult);
  if (size<5)                                       return {ok:false,reason:"Budget low"};
  if (size/liq>G.MAX_MKT_SHARE)                    return {ok:false,reason:"Market share"};
  return {ok:true,size:+size.toFixed(2)};
}

// ── TRADE EXECUTION ───────────────────────────────────────
async function executePaperTrade(market, consensus, agents, guard) {
  const price=market._yesPrice||extractPrice(market);
  if (!price||price<=0) { console.log("Blocked: price 0"); return; }
  const size=guard.size;
  const shares=+(size/price).toFixed(4);
  const stopPrice=+(Math.max(G.FLOOR,price*(1-G.STOP_PCT))).toFixed(4);
  const target=+(Math.min(G.CEIL,price+(1-price)*G.EXIT_TARGET)).toFixed(4);
  const fired=agents.filter(a=>a.vote).map(a=>a.name).join(",");
  stmt.insertTrade.run(market.id,market.question,market.category||"general","YES",price,price,size,shares,stopPrice,target,fired,consensus.label,consensus.votes,market._endDate);
  setState("daily_trades",(getState("daily_trades")||0)+1);
  savePositionBackup();
  const msg=`<b>VERAPIMPO TRADE</b> [PAPER]\n\n<b>${market.question?.slice(0,80)}</b>\n\nEntry: YES @ ${(price*100).toFixed(1)}%\nSize: $${size.toFixed(2)} | Shares: ${shares}\nTarget: ${(target*100).toFixed(1)}% | Stop: ${(stopPrice*100).toFixed(1)}%\nSignal: ${consensus.label} (${consensus.votes}/${consensus.total})\nAgents: ${fired}\nResolves: ${market._endDate?new Date(market._endDate).toLocaleDateString():"?"}`;
  await tg(msg); logAlert("TRADE_OPENED",msg,market.id);
  console.log(`TRADE: ${market.question?.slice(0,50)} @ ${(price*100).toFixed(1)}% $${size}`);
}

// ── POSITION MONITOR ──────────────────────────────────────
async function monitorPositions() {
  const open=getOpenTrades(); if (!open.length) return;
  for (const trade of open) {
    try {
      const market=await getMarketById(trade.market_id); if (!market) { await delay(200); continue; }
      const price=extractPrice(market);
      if (!price||price<=0||isNaN(price)) { await delay(200); continue; }
      const endDate=new Date(market.endDate||market.end_date_iso||0);
      const hoursLeft=(endDate-new Date())/3600000;
      const pnl=(price-trade.entry_price)*trade.shares;
      const pnl_pct=((price-trade.entry_price)/trade.entry_price)*100;
      const vol24=parseFloat(market.volume24hr||0);
      const baseVol=parseFloat(market.volume||0)/30;
      stmt.updatePrice.run(price,trade.id);
      let exitReason=null;
      if (price>=trade.target_price)                             exitReason="TARGET_HIT";
      else if (price<=trade.stop_price)                          exitReason="STOP_HIT";
      else if (hoursLeft>0&&hoursLeft<=G.BLACKOUT_HRS)          exitReason="RESOLUTION_BLACKOUT";
      else if (baseVol>1000&&vol24>baseVol*G.VOL_SPIKE)         exitReason="VOLUME_SPIKE";
      if (exitReason) {
        stmt.closeTrade.run(+pnl.toFixed(4),+pnl_pct.toFixed(2),exitReason,trade.id);
        saveFullBackup();
        const totalPnl=(getState("total_pnl")||0)+pnl;
        setState("total_pnl",+totalPnl.toFixed(4));
        if (pnl<0) setState("total_loss",(getState("total_loss")||0)+Math.abs(pnl));
        const msg=`<b>VERAPIMPO EXIT</b> [PAPER]\n\n<b>${trade.market_question?.slice(0,80)}</b>\n\n${(trade.entry_price*100).toFixed(1)}% → ${(price*100).toFixed(1)}%\nP&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)} (${pnl_pct>=0?"+":""}${pnl_pct.toFixed(1)}%)</b>\nReason: ${exitReason} | Total: $${totalPnl.toFixed(2)}`;
        await tg(msg); logAlert("TRADE_CLOSED",msg,trade.market_id);
      }
    } catch(e) { console.error("Monitor:",e.message); }
    await delay(300);
  }
}

// ── TELEGRAM CHANNEL SCRAPER ──────────────────────────────
let tgCacheAt=0; const TG_TTL=15*60*1000;

async function scrapeTelegramChannels() {
  if (Date.now()-tgCacheAt<TG_TTL) return;
  tgCacheAt=Date.now();
  const mkts=db.prepare("SELECT id,question FROM markets ORDER BY last_scanned DESC LIMIT 50").all();
  for (const ch of TG_CHANNELS) {
    try {
      const html=await safeFetchText(`https://t.me/s/${ch.username}`,10000);
      if (!html||html.length<300) continue;
      const blocks=[...html.matchAll(/<div class="tgme_widget_message_text js-message_text"[^>]*>([\s\S]*?)<\/div>/g)];
      const dates=[...html.matchAll(/datetime="([^"]+)"/g)].map(m=>m[1]);
      for (let i=0;i<Math.min(blocks.length,10);i++) {
        const text=stripHtml(blocks[i][1]).slice(0,500);
        if (text.length<10) continue;
        const date=dates[i]||new Date().toISOString();
        if (new Date(date)<new Date(Date.now()-12*3600000)) continue;
        const lower=text.toLowerCase();
        let score=0;
        ["yes","win","pump","bull","buy","up"].forEach(w=>{if(lower.includes(w))score++;});
        ["bitcoin","btc","eth","crypto","polymarket"].forEach(w=>{if(lower.includes(w))score+=2;});
        ["whale","wallet","million","transfer"].forEach(w=>{if(lower.includes(w))score+=2;});
        (text.match(/\$[\d,.]+[MmBbKk]?/g)||[]).forEach(()=>score++);
        (text.match(/0x[a-fA-F0-9]{40}/g)||[]).forEach(()=>score+=3);
        let marketId=null;
        for (const m of mkts) {
          const mwords=(m.question||"").toLowerCase().split(" ").filter(w=>w.length>4);
          if (mwords.filter(w=>lower.includes(w)).length>=2){marketId=m.id;break;}
        }
        const msgId=`${ch.username}_${i}_${date}`;
        try { db.prepare("INSERT OR IGNORE INTO tg_messages (channel,channel_type,message_id,text,date,market_id,relevance_score) VALUES (?,?,?,?,?,?,?)").run(ch.label,ch.type,msgId,text,date,marketId,score); } catch {}
      }
    } catch(e) { console.error(`TG ${ch.label}:`,e.message); }
    await delay(1000);
  }
}

function getTelegramBoost(marketId,question) {
  const words=(question||"").toLowerCase().split(" ").filter(w=>w.length>4);
  const msgs=stmt.tgRecent.all();
  let whale=0,news=0;
  for (const m of msgs) {
    const lower=(m.text||"").toLowerCase();
    if (words.filter(w=>lower.includes(w)).length>=2||m.market_id===marketId) {
      if (m.channel_type==="whale") whale+=m.relevance_score*0.3;
      if (m.channel_type==="news")  news+=m.relevance_score*0.2;
    }
  }
  return {whale:Math.min(1,whale),news:Math.min(1,news)};
}

// ── WHALE REFRESH ─────────────────────────────────────────
async function refreshWhales() {
  console.log("Whale refresh starting...");
  try {
    const markets=await getMarkets(50,0); const stats={};
    for (const market of markets.slice(0,10)) {
      await delay(500);
      const trades=await getRecentTrades(market.id,30);
      for (const t of trades) {
        for (const addr of [t.maker,t.taker].filter(Boolean)) {
          if (!stats[addr]) stats[addr]={wins:0,total:0,profit:0,recent:0};
          stats[addr].total++;
          if (parseFloat(t.price||0)>0.5){stats[addr].wins++;stats[addr].profit+=parseFloat(t.size||0)*parseFloat(t.price||0);}
          if (new Date(t.timestamp||t.created_at)>new Date(Date.now()-86400000*7)) stats[addr].recent++;
        }
      }
    }
    const scored=Object.entries(stats).filter(([,s])=>s.total>=10&&s.recent>=1)
      .map(([addr,s])=>({addr,...s,winRate:s.wins/s.total,score:s.wins/s.total*0.5+(s.profit/10000)*0.3+(s.recent/20)*0.2}))
      .sort((a,b)=>b.score-a.score).slice(0,50);
    const prev=new Set(db.prepare("SELECT address FROM whales").all().map(w=>w.address));
    const curr=new Set(scored.map(w=>w.addr));
    let added=0,dropped=0;
    for (const [i,w] of scored.entries()) {
      const tier=i<5?1:i<20?2:3;
      const d=db.prepare("SELECT days_in_top50 FROM whales WHERE address=?").get(w.addr);
      const days=(d?.days_in_top50||0)+1;
      db.prepare("INSERT OR REPLACE INTO whales (address,rank,win_rate,avg_profit,total_trades,recent_trades,profit_30d,tier,days_in_top50,confirmed,last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)")
        .run(w.addr,i+1,w.winRate,w.profit/w.total,w.total,w.recent,w.profit,tier,days,days>=3?1:0);
      if (!prev.has(w.addr)) added++;
    }
    for (const addr of prev) { if (!curr.has(addr)){db.prepare("DELETE FROM whales WHERE address=?").run(addr);dropped++;} }
    const conf=db.prepare("SELECT COUNT(*) as c FROM whales WHERE confirmed=1").get()?.c||0;
    await tg(`<b>Whale Refresh</b>\nNew: ${added} | Dropped: ${dropped} | Confirmed: ${conf}`);
    logAlert("WHALE_REFRESH",`Ranked: ${scored.length} Confirmed: ${conf}`);
    console.log("Whale refresh:",scored.length,"ranked,",conf,"confirmed");
  } catch(e) { console.error("Whale refresh:",e.message); }
}

// ── MAIN SCAN ─────────────────────────────────────────────
async function runScan() {
  if (getState("paused")) return;
  const openBefore=getOpenTrades();
  if (openBefore.length>=G.MAX_POSITIONS) {
    await monitorPositions();
    await tg(`<b>VeraPimpo</b> — Full (${openBefore.length}/${G.MAX_POSITIONS}) — monitoring only`);
    return;
  }
  console.log("Scan",new Date().toISOString());
  await Promise.all([refreshNewsCache(),scrapeTelegramChannels()]);
  const raw=await getMarkets(100,0);
  if (!raw.length){console.log("No markets");return;}
  const viable=raw.map(normalizeMarket).filter(m=>m._yesPrice>G.FLOOR&&m._yesPrice<G.CEIL&&m._liq>=G.MIN_LIQ&&(m._endDate?(new Date(m._endDate)-new Date())/3600000>G.BLACKOUT_HRS:true)).slice(0,20);
  console.log(viable.length,"viable from",raw.length,"markets");
  for (const m of viable) {
    try { stmt.upsertMkt.run(m.id||m.conditionId,m.question,m.category||"general",m._yesPrice,1-m._yesPrice,parseFloat(m.volume||0),m._endDate||null,Math.round(Math.abs(m._yesPrice-0.5)*100+Math.min(m._liq/100000,0.3)*30)); } catch {}
  }
  const tgBoosts=new Map(viable.map(m=>[m.id,getTelegramBoost(m.id,m.question)]));
  let traded=0,scanned=0;
  for (const market of viable) {
    if (getOpenTrades().length>=G.MAX_POSITIONS) break;
    try {
      scanned++;
      const tgB=tgBoosts.get(market.id)||{whale:0,news:0};
      const [obP,wa,resS]=await Promise.all([detectOrderBookPressure(market),getWhaleActivity(market),checkResolutionSource(market)]);
      detectMarketDivergence(market,viable);
      const whaleRes=agentWhaleCopy(wa);
      const newsRes=agentNewscat(market);
      const techRes=agentTechnical(market);
      const mathRes=agentResolutionMath(market);
      const sentRes=await agentSentiment(market);
      const stealthRes=agentStealth(wa,newsRes.vote,sentRes.vote);
      if (tgB.whale>0.5&&whaleRes.vote) whaleRes.confidence=Math.min(1,whaleRes.confidence+0.15);
      if (tgB.news>0.5&&newsRes.vote)   newsRes.confidence=Math.min(1,newsRes.confidence+0.15);
      const allAgents=[{...whaleRes,name:"Whale"},{...stealthRes,name:"Stealth"},{...newsRes,name:"News"},{...sentRes,name:"Sentiment"},{...techRes,name:"Technical"},{...mathRes,name:"ResolutionMath"}];
      const consensus=runConsensus(allAgents,whaleRes,stealthRes);
      if (consensus.action==="ENTER") {
        stmt.updateNewsStatus.run(market.id);
        const guard=checkGuardrails(market,consensus);
        if (guard.ok){await executePaperTrade(market,consensus,allAgents,guard);traded++;}
        else console.log("Blocked",market.question?.slice(0,40),":",guard.reason);
      }
    } catch(e) { console.error("Scan:",market.question?.slice(0,30),e.message); }
    await delay(600);
  }
  const finalOpen=getOpenTrades();
  await tg(`<b>VeraPimpo Scan</b>\n${traded} traded | ${scanned} scanned | ${viable.length} viable\nPositions: ${finalOpen.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}/$${G.BUDGET}`);
  await monitorPositions();
}

// ── BOT COMMANDS ──────────────────────────────────────────
if (bot) {
  bot.use(async(ctx,next)=>{if(ctx.from&&!isAuthorized(ctx.from.id)){await ctx.reply("Unauthorized.");return;}return next();});
  bot.start(ctx=>ctx.replyWithHTML(`<b>VeraPimpo v2.0</b>\nPaper Mode — $${G.BUDGET}\n\n/portfolio /scan /sync /positions /history /whales /whalesscan /markets /news /strategy /pause /resume /status /live`));
  bot.command("portfolio",ctx=>{const dep=getDeployed(),pnl=getState("total_pnl")||0,loss=getState("total_loss")||0,open=getOpenTrades(),total=stmt.closedCount.get()?.c||0,wins=stmt.winsCount.get()?.c||0;ctx.replyWithHTML(`<b>VeraPimpo Portfolio</b> [PAPER]\n\nBudget: $${G.BUDGET} | Deployed: $${dep.toFixed(2)}\nP&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)}</b> | Losses: $${loss.toFixed(2)}/$${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS} | WR: ${total>0?((wins/total)*100).toFixed(1):"N/A"}% (${total} closed)\nToday: ${getState("daily_trades")||0}/${G.MAX_DAILY} trades`);});
  bot.command("positions",ctx=>{const open=getOpenTrades();if(!open.length){ctx.reply("No open positions.");return;}let msg=`<b>Open Positions (${open.length}/${G.MAX_POSITIONS})</b>\n\n`;for(const t of open){const pnl=(t.current_price-t.entry_price)*t.shares;msg+=`<b>${t.market_question?.slice(0,55)}</b>\n${(t.entry_price*100).toFixed(1)}% → ${(t.current_price*100).toFixed(1)}% | ${pnl>=0?"+":""}$${pnl.toFixed(2)}\nAgents: ${t.agents_fired}\n\n`;}ctx.replyWithHTML(msg);});
  bot.command("history",ctx=>{const rows=db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 15").all();if(!rows.length){ctx.reply("No closed trades.");return;}let msg=`<b>Last ${rows.length} Trades</b>\n\n`;for(const t of rows)msg+=`${t.pnl>=0?"🟢":"🔴"} ${t.market_question?.slice(0,45)} | ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)} [${t.exit_reason}]\n`;ctx.replyWithHTML(msg);});
  bot.command("whales",ctx=>{const w=db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank LIMIT 10").all();if(!w.length){ctx.reply("No confirmed whales yet.");return;}let msg="<b>Top Whales</b>\n\n";for(const r of w)msg+=`#${r.rank} ${r.address.slice(0,10)}... | T${r.tier} | ${(r.win_rate*100).toFixed(0)}% WR\n`;ctx.replyWithHTML(msg);});
  bot.command("whalesscan",async ctx=>{ctx.replyWithHTML("<i>Whale scan started...</i>");await refreshWhales();ctx.replyWithHTML("Done. /whales for rankings.");});
  bot.command("scan",ctx=>{ctx.replyWithHTML("<i>Manual scan triggered...</i>");runScan();});
  bot.command("sync",ctx=>{const r=restorePositionsFromBackup(),open=getOpenTrades();ctx.replyWithHTML(`<b>Sync</b>\nRestored: ${r} | Open: ${open.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}`);});
  bot.command("markets",ctx=>{const cached=stmt.cachedMkts.all();if(!cached.length){ctx.reply("No markets cached. /scan first.");return;}let msg="<b>Top Markets</b>\n\n";for(const m of cached.slice(0,8))msg+=`<b>${m.question?.slice(0,60)}</b>\nYES: ${((m.yes_price||0)*100).toFixed(1)}%\n\n`;ctx.replyWithHTML(msg);});
  bot.command("news",ctx=>{const rows=db.prepare("SELECT * FROM news_items ORDER BY fetched_at DESC LIMIT 10").all();if(!rows.length){ctx.reply("No news. /scan first.");return;}let msg="<b>News</b>\n\n";for(const n of rows)msg+=`[${n.status?.toUpperCase()}] ${n.source}\n${n.headline?.slice(0,80)}\n\n`;ctx.replyWithHTML(msg);});
  bot.command("strategy",ctx=>{const agents=["Whale","Stealth","News","Sentiment","Technical","ResolutionMath"];let msg="<b>Strategy Performance</b>\n\n";for(const s of agents){const total=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;const wins=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;const pnl=db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0;msg+=`<b>${s}</b>: ${total} trades | ${total>0?((wins/total)*100).toFixed(0):"-"}% WR | $${pnl.toFixed(2)}\n`;}ctx.replyWithHTML(msg);});
  bot.command("pause",ctx=>{setState("paused",true);ctx.reply("Paused.");});
  bot.command("resume",ctx=>{setState("paused",false);ctx.reply("Resumed.");});
  bot.command("status",ctx=>{const open=getOpenTrades();ctx.replyWithHTML(`<b>VeraPimpo Status</b>\n\nMode: ${getState("paused")?"PAUSED":"ACTIVE"} | Paper: ${PAPER?"YES":"NO"}\nBudget: $${G.BUDGET} | Halt: $${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}\nToday: ${getState("daily_trades")||0}/${G.MAX_DAILY} trades`);});
  bot.command("live",ctx=>ctx.replyWithHTML(`<b>Going Live</b>\n\n1. Polygon wallet\n2. USDC via Rain\n3. WALLET_PRIVATE_KEY → Railway\n4. PAPER=false\n5. Redeploy`));
  bot.help(ctx=>ctx.replyWithHTML(`/portfolio /scan /sync /positions /history /whales /whalesscan /markets /news /strategy /pause /resume /status /live`));
}

// ── REST API ──────────────────────────────────────────────
const app=express();
app.use(cors()); app.use(express.json());
app.use("/api",(req,res,next)=>{if((req.headers["x-dashboard-password"]||req.query.password)!==cfg.dashPass)return res.status(401).json({error:"Unauthorized"});next();});
app.get("/api/overview",(_,res)=>{const open=getOpenTrades(),total=stmt.closedCount.get()?.c||0,wins=stmt.winsCount.get()?.c||0,today=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE date(opened_at)=date('now')").get()?.c||0;res.json({budget:G.BUDGET,deployed:getDeployed(),available:G.BUDGET-getDeployed(),pnl:getState("total_pnl")||0,loss:getState("total_loss")||0,lossHalt:G.LOSS_HALT,openCount:open.length,maxPositions:G.MAX_POSITIONS,winRate:total>0?wins/total:0,totalTrades:total,todayTrades:today,maxDailyTrades:G.MAX_DAILY,paper:PAPER,paused:getState("paused")||false});});
app.get("/api/positions",(_,res)=>res.json(stmt.openTrades.all()));
app.get("/api/history",(req,res)=>res.json(db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?").all(parseInt(req.query.limit)||50)));
app.get("/api/whales",(_,res)=>res.json(db.prepare("SELECT * FROM whales ORDER BY rank").all()));
app.get("/api/whale-trades",(_,res)=>res.json(db.prepare("SELECT * FROM whale_trades ORDER BY timestamp DESC LIMIT 50").all()));
app.get("/api/markets",(_,res)=>res.json(stmt.cachedMkts.all()));
app.get("/api/news",(req,res)=>{const limit=parseInt(req.query.limit)||50;const news=db.prepare("SELECT *,'news' as feed_type FROM news_items ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.7));const tgMsg=db.prepare("SELECT id,channel as source,text as headline,text as summary,NULL as url,NULL as image_url,date as published_at,market_id,NULL as market_question,'monitored' as status,'telegram' as feed_type FROM tg_messages WHERE relevance_score>1 ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.3));res.json([...news,...tgMsg].sort((a,b)=>new Date(b.fetched_at||0)-new Date(a.fetched_at||0)).slice(0,limit));});
app.get("/api/tg-messages",(_,res)=>res.json(stmt.tgRecent.all()));
app.get("/api/strategy",(_,res)=>{const agents=["Whale","Stealth","News","Sentiment","Technical","ResolutionMath"];res.json(agents.map(s=>({name:s,trades:db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,wins:db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,pnl:db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0,avg_pnl:db.prepare("SELECT AVG(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0})));});
app.get("/api/alerts",(_,res)=>res.json(db.prepare("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100").all()));
app.get("/api/scan-log",(_,res)=>res.json(db.prepare("SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT 50").all()));
app.post("/api/sync",(req,res)=>{const r=restorePositionsFromBackup();res.json({restored:r,open:getOpenTrades().length,deployed:getDeployed()});});
app.post("/api/scan",(_,res)=>{res.json({started:true});runScan();});
app.post("/api/whalesscan",(_,res)=>{res.json({started:true});refreshWhales();});
app.post("/api/pause",(_,res)=>{setState("paused",true);res.json({paused:true});});
app.post("/api/resume",(_,res)=>{setState("paused",false);res.json({paused:false});});
app.get("/",(req,res)=>{try{res.setHeader("Content-Type","text/html");res.send(fs.readFileSync(path.join(__dirname,"dashboard.html"),"utf8"));}catch{res.status(500).send("Upload dashboard.html to the repo root.");}});
app.get("/health",(_,res)=>res.json({status:"ok",paper:PAPER,version:"2.0.0"}));
app.listen(3000,()=>console.log("VeraPimpo API on port 3000"));

// ── SCHEDULES ─────────────────────────────────────────────
let scanning=false;
setInterval(async()=>{if(scanning||getState("paused"))return;scanning=true;try{await runScan();}finally{scanning=false;}},480000);
setInterval(async()=>{if(!getState("paused"))await monitorPositions();},180000);
setInterval(saveFullBackup, 600000); // Full backup every 10 minutes
cron.schedule("0 3 * * *",refreshWhales,{timezone:"UTC"});
cron.schedule("0 6 * * *",async()=>{const open=getOpenTrades();await tg(`<b>VeraPimpo Daily</b>\nP&L: ${(getState("total_pnl")||0)>=0?"+":""}$${(getState("total_pnl")||0).toFixed(2)}\nLosses: $${(getState("total_loss")||0).toFixed(2)}/$${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS}`);},{timezone:"UTC"});

// ── LAUNCH ────────────────────────────────────────────────
(async()=>{
  console.log("VeraPimpo v2.0 starting...");
  if(bot){await bot.launch();console.log("Telegram bot active");}
  if(restored>0) await tg(`<b>VeraPimpo Restarted</b>\n${restored} position(s) restored.`);
  await tg(`<b>VeraPimpo v2.0 Online</b>\n\nPaper: YES | Budget: $${G.BUDGET} | Halt: $${G.LOSS_HALT}\n6 agents | 14 guardrails | First scan in 60s`);
  setTimeout(async()=>{await refreshWhales();await runScan();},60000);
})().catch(console.error);

process.once("SIGINT",()=>{if(bot)bot.stop("SIGINT");process.exit(0);});
process.once("SIGTERM",()=>{if(bot)bot.stop("SIGTERM");process.exit(0);});
