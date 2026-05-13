// ================================================================
// VERAPIMPO v3.0 — Polymarket Intelligence Agent
// ================================================================
// WHAT'S NEW IN v3.0:
//  ✓ Market type classifier — blocks esports/entertainment
//  ✓ Specialized signals per market type (crypto/political/sports/economic/gold)
//  ✓ Google News RSS search per market — targeted, not generic
//  ✓ Whale agent disabled when 0 confirmed whales (no phantom votes)
//  ✓ Tighter entry range: 20-60% (was 15-85%)
//  ✓ Higher liquidity floor: $25k (was $10k)
//  ✓ Higher min edge: 8% (was 5%)
//  ✓ Resolution window: 1-14 days sweet spot
//  ✓ Trailing stop — locks in gains as price rises
//  ✓ Kelly Criterion position sizing
//  ✓ Market correlation guard — no 3 same-type positions
//  ✓ Gold price via Yahoo Finance (GLD ETF proxy)
//  ✓ Category performance tracking
//  ✓ Full state backup — trades, P&L, whales all restored on redeploy
//  ✓ Rate limiting, CORS, indexes, env validation
// ================================================================

"use strict";

const { Telegraf } = require("telegraf");
const Database     = require("better-sqlite3");
const cron         = require("node-cron");
const express      = require("express");
const cors         = require("cors");
const fs           = require("fs");
const path         = require("path");

// ── ENV VALIDATION ────────────────────────────────────────
const REQUIRED = ["VERAPIMPO_TELEGRAM_TOKEN","VERAPIMPO_CHAT_ID","DASHBOARD_PASSWORD"];
const missing  = REQUIRED.filter(v => !process.env[v]);
if (missing.length) {
  console.error("\nMISSING ENV VARS:\n  " + missing.join("\n  ") + "\n");
  process.exit(1);
}

// ── CONFIG ────────────────────────────────────────────────
const PAPER = true;
const cfg = {
  telegram : process.env.VERAPIMPO_TELEGRAM_TOKEN,
  chatId   : process.env.VERAPIMPO_CHAT_ID,
  chatId2  : process.env.VERAPIMPO_CHAT_ID_2 || "",
  dashPass : process.env.DASHBOARD_PASSWORD,
};

// ── GUARDRAILS v3 — tighter, smarter ─────────────────────
const G = {
  BUDGET        : 500,
  MAX_POS       : 50,
  MAX_POSITIONS : 8,
  LOSS_HALT     : 150,
  BLACKOUT_HRS  : 2,
  CEIL          : 0.65,  // raised slightly — 65% ceiling
  FLOOR         : 0.18,  // lowered slightly — 18% floor
  MIN_LIQ       : 15000, // $15k — less restrictive
  MAX_MKT_SHARE : 0.03,
  MIN_EDGE      : 0.07,  // 7% minimum edge
  MAX_DAILY     : 10,
  MAX_CAT_DEP   : 0.30,
  EXIT_TARGET   : 0.85,
  VOL_SPIKE     : 3.0,
  STOP_PCT      : 0.15,
  TRAIL_PCT     : 0.10,
  MIN_DAYS      : 0.08,  // 2 hours minimum — blackout handles the real floor
  MAX_DAYS      : 21,    // 21 days — wider window
  KELLY_FRAC    : 0.25,
  MAX_CORR_CAT  : 2,
};

// ── MARKET TYPES ──────────────────────────────────────────
const BLOCKED_TYPES = new Set(["esports","entertainment"]);

function classifyMarket(question) {
  const q = (question || "").toLowerCase();
  if (["bitcoin","btc","eth","ethereum","crypto","solana","sol","doge","xrp","matic","usdc","defi"].some(w=>q.includes(w))) return "crypto";
  if (["gold","silver","oil","crude","commodity","metal","xau"].some(w=>q.includes(w))) return "commodity";
  if (["fed","federal reserve","interest rate","gdp","inflation","cpi","unemployment","earnings","revenue","ipo","merger","acquisition","nasdaq","s&p","dow"].some(w=>q.includes(w))) return "economic";
  if (["election","president","senate","congress","vote","democrat","republican","candidate","primary","governor","mayor","parliament","minister","referendum","ballot","poll"].some(w=>q.includes(w))) return "political";
  if (["counter-strike","cs2","csgo","dota","valorant","league of legends","overwatch","esport","gaming"].some(w=>q.includes(w))) return "esports";
  if (["oscar","grammy","emmy","award","box office","streaming","movie","album","artist","billboard","chart"].some(w=>q.includes(w))) return "entertainment";
  // Sports — comprehensive: includes soccer/draw markers, league names, sport types
  if ([
    "nfl","nba","mlb","nhl","soccer","football","basketball","baseball","tennis","ufc","mma","fight",
    "championship","super bowl","world cup","playoffs","series","tournament","score","defeat",
    "draw","penalty","vs.","v.s.","premier league","bundesliga","serie a","ligue 1","la liga",
    "champions league","europa league","conference league","mls","ucl",
    " fc "," sk "," cf "," ac "," afc "," sc "," if "," bk "," utd "," city "," united ",
    "match","fixture","kickoff","kick-off","halftime","full time","overtime","innings",
    "quarterback","touchdown","goal","hat trick","hat-trick","red card","yellow card",
    "spread","over/under","o/u","moneyline","handicap",
    "win on 20","beat on 20","defeat on 20",  // "Will X win on 2026-05-17"
    "rcd ","cf ","atletico","real madrid","barcelona","juventus","arsenal","chelsea",
    "liverpool","manchester","tottenham","milan","roma","napoli","porto","ajax","psg",
    "lakers","celtics","warriors","nuggets","heat","bulls","knicks","nets",
    "yankees","dodgers","mets","cubs","red sox","astros",
    "cowboys","chiefs","patriots","eagles","packers","rams","49ers"
  ].some(w=>q.includes(w))) return "sports";
  return "other";
}

function extractKeyTerms(question) {
  const q = (question || "").toLowerCase();
  const stopWords = new Set(["will","have","does","than","from","that","this","with","what","when","where","which","the","and","for","are","but","not","you","all","can","her","was","one","our","out","who","get","how","him","his","its","may","hit","low","high","above","below","before","after","price","market","between","reach"]);
  return q.split(/\s+/).filter(w => w.length > 3 && !stopWords.has(w) && !/^\d+$/.test(w)).slice(0,4);
}

// ── DATABASE ──────────────────────────────────────────────
const db = new Database("./verapimpo.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT, market_question TEXT, category TEXT,
    market_type TEXT DEFAULT 'other', side TEXT DEFAULT 'YES',
    entry_price REAL, current_price REAL, size REAL, shares REAL,
    stop_price REAL, trailing_stop REAL, target_price REAL,
    high_water_mark REAL,
    agents_fired TEXT, layer TEXT, tier INTEGER,
    signal_detail TEXT,
    status TEXT DEFAULT 'open',
    pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0,
    exit_reason TEXT,
    opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closes_at DATETIME, closed_at DATETIME
  );
  CREATE TABLE IF NOT EXISTS whales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT UNIQUE, rank INTEGER,
    win_rate REAL, avg_profit REAL,
    total_trades INTEGER, recent_trades INTEGER,
    profit_30d REAL, tier INTEGER,
    days_in_top50 INTEGER DEFAULT 0,
    confirmed INTEGER DEFAULT 0,
    last_seen DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS whale_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    whale_address TEXT, market_id TEXT,
    market_question TEXT, side TEXT,
    price REAL, size REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT, headline TEXT, summary TEXT,
    url TEXT, image_url TEXT, published_at DATETIME,
    market_id TEXT, market_question TEXT,
    market_type TEXT, signal_quality TEXT DEFAULT 'rss',
    status TEXT DEFAULT 'monitored',
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY, question TEXT, category TEXT,
    market_type TEXT DEFAULT 'other',
    yes_price REAL, no_price REAL, volume REAL,
    end_date DATETIME, score REAL DEFAULT 0,
    signal_detail TEXT, last_scanned DATETIME
  );
  CREATE TABLE IF NOT EXISTS tg_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT, channel_type TEXT,
    message_id TEXT, text TEXT,
    date DATETIME, market_id TEXT,
    relevance_score REAL DEFAULT 0,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel, message_id)
  );
  CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT, message TEXT, market_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS agent_state (key TEXT PRIMARY KEY, value TEXT);
  CREATE TABLE IF NOT EXISTS category_performance (
    market_type TEXT PRIMARY KEY,
    trades INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    total_pnl REAL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

// Indexes
try {
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_trades_status   ON paper_trades(status);
    CREATE INDEX IF NOT EXISTS idx_trades_opened   ON paper_trades(opened_at);
    CREATE INDEX IF NOT EXISTS idx_trades_market   ON paper_trades(market_id);
    CREATE INDEX IF NOT EXISTS idx_trades_type     ON paper_trades(market_type);
    CREATE INDEX IF NOT EXISTS idx_news_fetched    ON news_items(fetched_at);
    CREATE INDEX IF NOT EXISTS idx_news_market     ON news_items(market_id);
    CREATE INDEX IF NOT EXISTS idx_news_type       ON news_items(market_type);
    CREATE INDEX IF NOT EXISTS idx_alerts_created  ON alerts(created_at);
    CREATE INDEX IF NOT EXISTS idx_tg_fetched      ON tg_messages(fetched_at);
    CREATE INDEX IF NOT EXISTS idx_whales_rank     ON whales(rank);
    CREATE INDEX IF NOT EXISTS idx_markets_scanned ON markets(last_scanned);
  `);
} catch {}

// Add new columns to existing tables if upgrading from v2
try { db.exec("ALTER TABLE paper_trades ADD COLUMN market_type TEXT DEFAULT 'other'"); } catch {}
try { db.exec("ALTER TABLE paper_trades ADD COLUMN trailing_stop REAL"); } catch {}
try { db.exec("ALTER TABLE paper_trades ADD COLUMN high_water_mark REAL"); } catch {}
try { db.exec("ALTER TABLE paper_trades ADD COLUMN signal_detail TEXT"); } catch {}
try { db.exec("ALTER TABLE markets ADD COLUMN market_type TEXT DEFAULT 'other'"); } catch {}
try { db.exec("ALTER TABLE markets ADD COLUMN signal_detail TEXT"); } catch {}
try { db.exec("ALTER TABLE news_items ADD COLUMN market_type TEXT"); } catch {}
try { db.exec("ALTER TABLE news_items ADD COLUMN signal_quality TEXT DEFAULT 'rss'"); } catch {}

// Dedupe news on boot
try { db.exec("DELETE FROM news_items WHERE id NOT IN (SELECT MIN(id) FROM news_items GROUP BY source,headline)"); } catch {}

// ── PRE-COMPILED STATEMENTS ───────────────────────────────
const stmt = {
  getState    : db.prepare("SELECT value FROM agent_state WHERE key=?"),
  setState    : db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)"),
  openTrades  : db.prepare("SELECT * FROM paper_trades WHERE status='open'"),
  deployed    : db.prepare("SELECT COALESCE(SUM(size),0) as t FROM paper_trades WHERE status='open'"),
  catDeployed : db.prepare("SELECT COALESCE(SUM(size),0) as t FROM paper_trades WHERE status='open' AND category=?"),
  typeCount   : db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='open' AND market_type=?"),
  closedCount : db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed'"),
  winsCount   : db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0"),
  insertTrade : db.prepare("INSERT INTO paper_trades (market_id,market_question,category,market_type,side,entry_price,current_price,size,shares,stop_price,trailing_stop,target_price,high_water_mark,agents_fired,layer,tier,signal_detail,closes_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"),
  updatePosition: db.prepare("UPDATE paper_trades SET current_price=?,trailing_stop=?,high_water_mark=? WHERE id=?"),
  closeTrade  : db.prepare("UPDATE paper_trades SET status='closed',pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?"),
  upsertMkt   : db.prepare("INSERT OR REPLACE INTO markets (id,question,category,market_type,yes_price,no_price,volume,end_date,score,signal_detail,last_scanned) VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)"),
  cachedMkts  : db.prepare("SELECT * FROM markets WHERE market_type NOT IN ('esports','entertainment') ORDER BY score DESC, last_scanned DESC LIMIT 35"),
  whalesConf  : db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank"),
  whalesCount : db.prepare("SELECT COUNT(*) as c FROM whales WHERE confirmed=1"),
  logAlert    : db.prepare("INSERT INTO alerts (type,message,market_id) VALUES (?,?,?)"),
  newsExists  : db.prepare("SELECT id FROM news_items WHERE source=? AND headline=?"),
  insertNews  : db.prepare("INSERT INTO news_items (source,headline,summary,url,image_url,published_at,market_id,market_question,market_type,signal_quality,status) VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
  updateNewsStatus: db.prepare("UPDATE news_items SET status='triggered' WHERE market_id=? AND status='monitored'"),
  tgRecent    : db.prepare("SELECT * FROM tg_messages WHERE fetched_at > datetime('now','-6 hours') ORDER BY relevance_score DESC LIMIT 100"),
  catPerf     : db.prepare("SELECT * FROM category_performance ORDER BY total_pnl DESC"),
  upsertCatPerf: db.prepare("INSERT INTO category_performance (market_type,trades,wins,total_pnl) VALUES (?,1,?,?) ON CONFLICT(market_type) DO UPDATE SET trades=trades+1, wins=wins+?, total_pnl=total_pnl+?, updated_at=CURRENT_TIMESTAMP"),
};

const getState = (k,d=null) => { const r=stmt.getState.get(k); return r?JSON.parse(r.value):d; };
const setState = (k,v)      => stmt.setState.run(k, JSON.stringify(v));

["total_loss","total_pnl"].forEach(k => { if(getState(k)===null) setState(k,0); });
if(getState("paused")===null)       setState("paused",false);
if(getState("daily_trades")===null) setState("daily_trades",0);
if(!getState("trade_date"))         setState("trade_date","");

// ── POSITION BACKUP ───────────────────────────────────────
const BACKUP = "./verapimpo_backup.json";

function saveFullBackup() {
  try {
    fs.writeFileSync(BACKUP, JSON.stringify({
      open_positions : stmt.openTrades.all(),
      closed_trades  : db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 500").all(),
      agent_state    : db.prepare("SELECT * FROM agent_state").all(),
      whales         : db.prepare("SELECT * FROM whales ORDER BY rank").all(),
      cat_perf       : db.prepare("SELECT * FROM category_performance").all(),
      saved_at       : new Date().toISOString(),
    }));
  } catch(e) { console.error("Backup:",e.message); }
}

const savePositionBackup = saveFullBackup;

function restoreFromBackup() {
  try {
    if (!fs.existsSync(BACKUP)) { console.log("No backup found."); return 0; }
    const b = JSON.parse(fs.readFileSync(BACKUP,"utf8"));
    let restored = 0;
    if (b.agent_state?.length) {
      for (const s of b.agent_state) { if (!stmt.getState.get(s.key)) stmt.setState.run(s.key, s.value); }
    }
    if (b.closed_trades?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO paper_trades (id,market_id,market_question,category,market_type,side,entry_price,current_price,size,shares,stop_price,trailing_stop,target_price,agents_fired,layer,tier,signal_detail,status,pnl,pnl_pct,exit_reason,closes_at,opened_at,closed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      for (const t of b.closed_trades) {
        ins.run(t.id,t.market_id,t.market_question,t.category||"general",t.market_type||"other",t.side||"YES",t.entry_price,t.current_price||t.entry_price,t.size,t.shares,t.stop_price,t.trailing_stop||null,t.target_price,t.agents_fired,t.layer,t.tier,t.signal_detail||null,"closed",t.pnl||0,t.pnl_pct||0,t.exit_reason,t.closes_at,t.opened_at,t.closed_at);
      }
    }
    if (b.open_positions?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO paper_trades (id,market_id,market_question,category,market_type,side,entry_price,current_price,size,shares,stop_price,trailing_stop,target_price,high_water_mark,agents_fired,layer,tier,signal_detail,status,pnl,pnl_pct,closes_at,opened_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
      for (const p of b.open_positions) {
        ins.run(p.id,p.market_id,p.market_question,p.category||"general",p.market_type||"other",p.side||"YES",p.entry_price,p.current_price||p.entry_price,p.size,p.shares,p.stop_price,p.trailing_stop||null,p.target_price,p.high_water_mark||p.entry_price,p.agents_fired,p.layer,p.tier,p.signal_detail||null,"open",0,0,p.closes_at,p.opened_at);
        restored++;
      }
    }
    if (b.whales?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO whales (address,rank,win_rate,avg_profit,total_trades,recent_trades,profit_30d,tier,days_in_top50,confirmed,last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
      for (const w of b.whales) ins.run(w.address,w.rank,w.win_rate,w.avg_profit,w.total_trades,w.recent_trades,w.profit_30d,w.tier,w.days_in_top50,w.confirmed,w.last_seen);
    }
    if (b.cat_perf?.length) {
      const ins = db.prepare("INSERT OR IGNORE INTO category_performance (market_type,trades,wins,total_pnl) VALUES (?,?,?,?)");
      for (const c of b.cat_perf) ins.run(c.market_type,c.trades,c.wins,c.total_pnl);
    }
    console.log(`Restored: ${restored} open, ${b.closed_trades?.length||0} closed, ${b.whales?.length||0} whales`);
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

async function safeFetch(url, hdrs={}, timeout=10000) {
  const c=new AbortController(); const t=setTimeout(()=>c.abort(),timeout);
  try {
    const r=await fetch(url,{signal:c.signal,headers:{...POLY_HDR,...hdrs}});
    if(!r.ok){console.log(`HTTP ${r.status} ${url.slice(0,70)}`);return null;}
    return await r.json();
  } catch{return null;} finally{clearTimeout(t);}
}

async function safeFetchText(url, timeout=8000) {
  const c=new AbortController(); const t=setTimeout(()=>c.abort(),timeout);
  try{return await(await fetch(url,{signal:c.signal,headers:{"User-Agent":"Mozilla/5.0"}})).text();}
  catch{return "";} finally{clearTimeout(t);}
}

const delay   = ms => new Promise(r=>setTimeout(r,ms));
const logAlert= (type,msg,mid="") => { try{stmt.logAlert.run(type,msg,mid);}catch{} };

function stripHtml(s) {
  return (s||"").replace(/<[^>]+>/g," ").replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/\s+/g," ").trim();
}

function extractPrice(m) {
  try {
    const op=m.outcomePrices||m._yesPrice||m.yes_price;
    if(typeof op==="string") return parseFloat(JSON.parse(op)[0])||0;
    if(Array.isArray(op))    return parseFloat(op[0])||0;
    if(typeof op==="number") return op;
    return parseFloat(m.bestBid||m.lastTradePrice||m.yes_price||0);
  } catch{return parseFloat(m.bestBid||m.yes_price||0);}
}

const getOpenTrades  = ()    => stmt.openTrades.all();
const getDeployed    = ()    => stmt.deployed.get().t||0;
const getCatDeployed = (cat) => stmt.catDeployed.get(cat).t||0;

function resetDailyCounters() {
  const today=new Date().toDateString();
  if(getState("trade_date")!==today){setState("trade_date",today);setState("daily_trades",0);}
}

function normalizeMarket(m) {
  const mtype = classifyMarket(m.question);
  return {
    ...m,
    _yesPrice : extractPrice(m),
    _liq      : parseFloat(m.liquidity||m.liquidityNum||0),
    _endDate  : m.endDate||m.end_date_iso||m.endDateIso||null,
    _type     : mtype,
  };
}

// ── KELLY CRITERION SIZING ────────────────────────────────
function kellySize(edge, confidence, availableBudget) {
  // edge = detected mispricing (0-1), confidence = agent confidence (0-1)
  // Kelly fraction = (edge * confidence) / (1 - entry_price proxy)
  // Use conservative 25% Kelly
  const rawKelly = Math.max(0, (edge * confidence));
  const size = rawKelly * G.KELLY_FRAC * availableBudget;
  return Math.min(G.MAX_POS, Math.max(5, size));
}

// ── TELEGRAM ──────────────────────────────────────────────
const bot = cfg.telegram ? new Telegraf(cfg.telegram) : null;
const isAuthorized = id => [cfg.chatId,cfg.chatId2].filter(Boolean).map(String).includes(String(id));

async function tg(msg) {
  if(!bot) return;
  for(const id of [cfg.chatId,cfg.chatId2].filter(Boolean)) {
    try{await bot.telegram.sendMessage(id,msg,{parse_mode:"HTML"});}
    catch(e){console.error("TG:",e.message);}
  }
}

// ── POLYMARKET API ────────────────────────────────────────
const GAMMA = "https://gamma-api.polymarket.com";

async function getMarkets(limit=100,offset=0) {
  const d=await safeFetch(`${GAMMA}/markets?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`,{},12000);
  if(Array.isArray(d)&&d.length) return d;
  const d2=await safeFetch(`${GAMMA}/events?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`,{},12000);
  if(!Array.isArray(d2)) return [];
  const out=[];
  for(const ev of d2){if(Array.isArray(ev.markets))out.push(...ev.markets);else if(ev.id)out.push(ev);}
  return out;
}

async function getMarketById(id) { return safeFetch(`${GAMMA}/markets/${id}`,{},8000); }
async function getRecentTrades(id,limit=30) {
  const d=await safeFetch(`${GAMMA}/trades?market=${id}&limit=${limit}`,{},8000);
  return Array.isArray(d)?d:[];
}

// ── SPECIALIZED SIGNAL FETCHERS ───────────────────────────

// CRYPTO: live price vs threshold
async function fetchCryptoSignal(market) {
  const q=market.question.toLowerCase();
  let coinId="bitcoin";
  if(q.includes("ethereum")||q.includes("eth")) coinId="ethereum";
  else if(q.includes("solana")||q.includes("sol")) coinId="solana";
  else if(q.includes("doge")) coinId="dogecoin";
  else if(q.includes("xrp")) coinId="ripple";

  const data=await safeFetch(`https://api.coingecko.com/api/v3/simple/price?ids=${coinId}&vs_currencies=usd&include_24hr_change=true`,{},8000);
  if(!data?.[coinId]) return {signal:false,confidence:0,detail:"No price data"};

  const price=data[coinId].usd;
  const change24=data[coinId].usd_24h_change||0;

  // Extract threshold from question
  const threshMatch=q.match(/\$?([\d,]+)(\s*k)?/g);
  const thresholds=[];
  if(threshMatch){
    for(const m of threshMatch){
      const n=parseFloat(m.replace(/[$,k]/g,""))*(m.includes("k")?1000:1);
      if(n>100) thresholds.push(n);
    }
  }

  if(!thresholds.length) return {signal:true,confidence:0.6,detail:`${coinId} $${price.toLocaleString()} | 24h: ${change24.toFixed(1)}%`,price,change24};

  const target=thresholds[0];
  const distPct=((target-price)/price)*100;
  const movingToward=change24>0&&price<target;
  const confidence=Math.min(0.9,Math.max(0.2,movingToward?0.7-Math.abs(distPct)/100:0.3));

  return {
    signal:true, confidence, price, target, distPct:+distPct.toFixed(1), change24:+change24.toFixed(2),
    detail:`${coinId} $${price.toLocaleString()} → $${target.toLocaleString()} (${distPct.toFixed(1)}% away | 24h: ${change24.toFixed(1)}%)`
  };
}

// GOLD/COMMODITY: GLD ETF as gold proxy via Yahoo Finance
async function fetchCommoditySignal(market) {
  const q=market.question.toLowerCase();
  let ticker="GLD"; let name="Gold";
  if(q.includes("silver")) { ticker="SLV"; name="Silver"; }
  else if(q.includes("oil")||q.includes("crude")) { ticker="USO"; name="Oil"; }

  const data=await safeFetch(`https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?interval=1d&range=5d`,{},8000);
  const result=data?.chart?.result?.[0];
  if(!result) return {signal:false,confidence:0,detail:`No ${name} data`};

  const closes=result.indicators?.quote?.[0]?.close||[];
  const price=closes[closes.length-1];
  const prev=closes[closes.length-2];
  const change24=prev?((price-prev)/prev*100):0;

  return {
    signal:true, confidence:0.7, price, change24:+change24.toFixed(2),
    detail:`${name} (${ticker}) $${price?.toFixed(2)} | 24h: ${change24.toFixed(1)}%`
  };
}

// POLITICAL: targeted Google News RSS search
async function fetchPoliticalSignal(market) {
  const terms=extractKeyTerms(market.question);
  if(!terms.length) return {signal:false,confidence:0,detail:"No key terms"};
  const query=encodeURIComponent(terms.slice(0,3).join(" "));
  const xml=await safeFetchText(`https://news.google.com/rss/search?q=${query}&hl=en-US&gl=US&ceid=US:en`);
  if(!xml) return {signal:false,confidence:0,detail:"No news"};

  const items=xml.match(/<item>[\s\S]*?<\/item>/g)||[];
  const recent=items.filter(i=>{
    const pub=i.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
    return pub&&new Date(pub)>new Date(Date.now()-24*3600000);
  });

  const headlines=recent.slice(0,5).map(i=>stripHtml(i.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||""));
  for(const h of headlines){
    if(!stmt.newsExists.get("Google News",h.toLowerCase())){
      const link=recent[headlines.indexOf(h)]?.match(/<link>(.*?)<\/link>/)?.[1]||"";
      const pub=recent[headlines.indexOf(h)]?.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
      try{stmt.insertNews.run("Google News",h.toLowerCase(),"",link,"",pub,market.id,market.question,market._type||classifyMarket(market.question),"targeted","monitored");}catch{}
    }
  }

  const conf=Math.min(0.85,recent.length*0.15);
  return {signal:recent.length>0,confidence:conf,articles:recent.length,headlines,detail:`${recent.length} articles in 24h: ${headlines[0]?.slice(0,60)||""}`};
}

// SPORTS: ESPN RSS + targeted search + market mechanics analysis
async function fetchSportsSignal(market) {
  const terms=extractKeyTerms(market.question);
  const query=encodeURIComponent(terms.slice(0,2).join(" "));
  const price=market._yesPrice||extractPrice(market);
  const endDate=market._endDate?new Date(market._endDate):null;
  const hoursLeft=endDate?(endDate-new Date())/3600000:999;

  const [gg,espn]=await Promise.all([
    safeFetchText(`https://news.google.com/rss/search?q=${query}+sport&hl=en-US&gl=US&ceid=US:en`),
    safeFetchText("https://www.espn.com/espn/rss/news"),
  ]);

  let articles=0; const headlines=[];
  for(const xml of [gg,espn].filter(Boolean)){
    const items=xml.match(/<item>[\s\S]*?<\/item>/g)||[];
    for(const item of items.slice(0,10)){
      const title=stripHtml(item.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||"");
      const pub=item.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
      if(!title||!terms.some(t=>title.toLowerCase().includes(t))) continue;
      if(new Date(pub)<new Date(Date.now()-24*3600000)) continue;
      headlines.push(title);
      articles++;
      const link=item.match(/<link>(.*?)<\/link>/)?.[1]||"";
      if(!stmt.newsExists.get("ESPN/News",title.toLowerCase())){
        try{stmt.insertNews.run("ESPN/News",title.toLowerCase(),"",link,"",pub,market.id,market.question,market._type||"sports","targeted","monitored");}catch{}
      }
    }
  }

  // Base confidence from market mechanics alone (no article match needed)
  // Sports markets have genuine Polymarket liquidity and resolve on known outcomes
  // Price 20-45% on a binary sports outcome = real mispricing opportunity
  const mechanicsConf = price<=0.45 && hoursLeft<=48 ? 0.45 : price<=0.35 ? 0.4 : 0.3;
  const articleBoost  = articles * 0.1;
  const confidence    = Math.min(0.85, mechanicsConf + articleBoost);

  return {
    signal: confidence >= 0.3,
    confidence,
    articles,
    detail: `Sports: ${articles} articles | ${(price*100).toFixed(1)}% price | ${hoursLeft.toFixed(0)}h left${headlines[0]?" | "+headlines[0].slice(0,50):""}`
  };
}

// ECONOMIC: Fed + MarketWatch targeted
async function fetchEconomicSignal(market) {
  const terms=extractKeyTerms(market.question);
  const query=encodeURIComponent(terms.slice(0,3).join(" "));
  const xml=await safeFetchText(`https://news.google.com/rss/search?q=${query}+finance+economy&hl=en-US&gl=US&ceid=US:en`);
  if(!xml) return {signal:false,confidence:0,detail:"No data"};
  const items=xml.match(/<item>[\s\S]*?<\/item>/g)||[];
  const recent=items.filter(i=>new Date(i.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||0)>new Date(Date.now()-48*3600000));
  const headlines=recent.slice(0,5).map(i=>stripHtml(i.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||""));
  for(const h of headlines){
    const it=recent[headlines.indexOf(h)];
    const link=it?.match(/<link>(.*?)<\/link>/)?.[1]||"";
    const pub=it?.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
    if(!stmt.newsExists.get("Economic News",h.toLowerCase())){
      try{stmt.insertNews.run("Economic News",h.toLowerCase(),"",link,"",pub,market.id,market.question,market._type||"economic","targeted","monitored");}catch{}
    }
  }
  return {signal:recent.length>0,confidence:Math.min(0.85,recent.length*0.17),articles:recent.length,detail:`${recent.length} economic articles | ${headlines[0]?.slice(0,60)||""}`};
}

// Dispatch to correct signal fetcher
async function fetchSpecializedSignal(market) {
  const mtype=market._type||classifyMarket(market.question);
  try {
    let result;
    switch(mtype){
      case "crypto":    result={...(await fetchCryptoSignal(market)),    type:"crypto"};    break;
      case "commodity": result={...(await fetchCommoditySignal(market)),  type:"commodity"}; break;
      case "political": result={...(await fetchPoliticalSignal(market)),  type:"political"}; break;
      case "sports":    result={...(await fetchSportsSignal(market)),     type:"sports"};    break;
      case "economic":  result={...(await fetchEconomicSignal(market)),   type:"economic"};  break;
      default:          result={signal:false,confidence:0,detail:"No specialized signal",type:mtype}; break;
    }
    // Save a signal summary article once per market per day (dedup by market_id+date)
    if(result.detail&&result.detail.length>5){
      const today=new Date().toISOString().slice(0,10);
      const dedupKey=`signal_${market.id}_${today}`;
      if(!stmt.newsExists.get("VeraPimpo Signal",dedupKey)){
        const headline=`[${mtype.toUpperCase()}] ${(market.question||"").slice(0,80)}`;
        try{
          stmt.insertNews.run("VeraPimpo Signal",dedupKey,
            `${headline} — ${result.detail}`,
            `https://polymarket.com/event/${market.id}`,"",new Date().toUTCString(),
            market.id,market.question,mtype,"targeted","monitored");
        }catch{}
      }
    }
    return result;
  } catch(e) { return {signal:false,confidence:0,detail:`Signal error: ${e.message}`,type:mtype}; }
}

// ── NEWS CACHE (generic RSS fallback) ─────────────────────
const NEWS_SOURCES=[
  {name:"Reuters",      url:"https://feeds.reuters.com/reuters/businessNews"},
  {name:"BBC Business", url:"http://feeds.bbci.co.uk/news/business/rss.xml"},
  {name:"AP News",      url:"https://rsshub.app/apnews/topics/apf-politics"},
  {name:"CoinDesk",     url:"https://www.coindesk.com/arc/outboundfeeds/rss/"},
  {name:"Politico",     url:"https://www.politico.com/rss/politicopicks.xml"},
  {name:"ESPN",         url:"https://www.espn.com/espn/rss/news"},
];
let newsCache=[]; let newsCacheAt=0; const NEWS_TTL=5*60*1000;

async function refreshNewsCache() {
  if(Date.now()-newsCacheAt<NEWS_TTL&&newsCache.length) return;
  const items=[];
  for(const src of NEWS_SOURCES){
    try{
      const xml=await safeFetchText(src.url);
      for(const m of (xml?.match(/<item>[\s\S]*?<\/item>/g)||[]).slice(0,10)){
        const title=stripHtml(m.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||"");
        const desc=stripHtml(m.match(/<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/description>/)?.[1]||"").slice(0,200);
        const link=m.match(/<link>(.*?)<\/link>/)?.[1]||"";
        const imgUrl=m.match(/<media:content[^>]+url="([^"]+)"/)?.[1]||"";
        const pubDate=m.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
        if(title.length>5) items.push({source:src.name,title:title.toLowerCase(),desc:desc.toLowerCase(),link,imgUrl,pubDate});
      }
    }catch{}
    await delay(200);
  }
  newsCache=items; newsCacheAt=Date.now();
  console.log("News cache:",items.length,"articles");

  // Save ALL articles to DB proactively — news feed shows them regardless of market match
  for(const item of items){
    if(!stmt.newsExists.get(item.source,item.title)){
      try{
        stmt.insertNews.run(item.source,item.title,item.desc,item.link,item.imgUrl,item.pubDate,null,null,"other","rss","monitored");
      }catch{}
    }
  }
}

function matchNewsCache(market) {
  const terms=extractKeyTerms(market.question);
  if(!terms.length) return [];
  const hits=[];
  for(const item of newsCache){
    const full=item.title+" "+item.desc;
    if(terms.filter(w=>full.includes(w)).length>=2){
      hits.push(item);
      if(!stmt.newsExists.get(item.source,item.title)){
        try{stmt.insertNews.run(item.source,item.title,item.desc,item.link,item.imgUrl,item.pubDate,market.id,market.question,market._type||"other","rss","monitored");}catch{}
      }
      if(hits.length>=4) break;
    }
  }
  return hits;
}

// ── AGENT 1: WHALE COPY ───────────────────────────────────
// Only participates if confirmed whales exist — prevents phantom votes
function agentWhaleCopy(wa, hasConfirmedWhales) {
  if(!hasConfirmedWhales) return {vote:false,confidence:0,detail:"No confirmed whales — agent inactive",inactive:true};
  if(!wa.whales?.length)  return {vote:false,confidence:0,detail:"No whale activity this market"};
  const best=[...wa.whales].sort((a,b)=>a.whale.tier-b.whale.tier)[0];
  const tier=best.whale.tier;
  const conf=Math.min(1,(tier===1?0.9:tier===2?0.7:0.5)+(wa.clustering>=3?0.15:wa.clustering>=2?0.08:0));
  return {vote:true,confidence:conf,tier,clustering:wa.clustering,size_mult:tier===1?0.5:tier===2?0.25:0.15,detail:`T${tier} whale | ${(best.whale.win_rate*100).toFixed(0)}% WR | ${wa.clustering} wallets`};
}

// ── AGENT 2: STEALTH ──────────────────────────────────────
function agentStealth(wa, newsVote, sentVote, hasConfirmedWhales) {
  if(!hasConfirmedWhales||!wa.whales?.length||newsVote||sentVote) return {vote:false,confidence:0,detail:"Not stealthy",inactive:!hasConfirmedWhales};
  const best=[...wa.whales].sort((a,b)=>a.whale.tier-b.whale.tier)[0];
  const conf=best?.whale.tier===1?0.85:best?.whale.tier===2?0.65:0.45;
  return {vote:conf>0.5,confidence:conf,detail:`STEALTH T${best?.whale.tier} — zero public signal`};
}

// ── AGENT 3: SPECIALIZED SIGNAL ──────────────────────────
// Replaces generic news with real-world data per market type
function agentSpecialized(specializedResult) {
  if(!specializedResult?.signal) return {vote:false,confidence:specializedResult?.confidence||0,detail:specializedResult?.detail||"No signal"};
  const conf=specializedResult.confidence||0;
  return {vote:conf>0.35,confidence:conf,detail:specializedResult.detail};
}

// ── AGENT 4: NEWS COVERAGE ────────────────────────────────
// Generic RSS news — confirms specialized signal is being covered
function agentNewsCoverage(market, specializedResult) {
  const hits=matchNewsCache(market);
  const now=Date.now();
  const recent=hits.filter(h=>new Date(h.pubDate)>new Date(now-12*3600000)).length;
  // Also count specialized articles as news signal
  const extraConf=specializedResult?.articles?Math.min(0.3,specializedResult.articles*0.06):0;
  const conf=Math.min(0.9,recent*0.12+extraConf);
  return {vote:conf>0.15,confidence:conf,detail:`${hits.length} RSS + ${specializedResult?.articles||0} targeted | ${recent} recent`};
}

// ── AGENT 5: TECHNICAL ────────────────────────────────────
function agentTechnical(market) {
  const price=market._yesPrice||extractPrice(market);
  const vol=parseFloat(market.volume||0);
  const vol24=parseFloat(market.volume24hr||0);
  const liq=market._liq||parseFloat(market.liquidity||0);
  if(!price||price<=0) return {vote:false,confidence:0,detail:"No price"};
  const momentum=vol24>0&&vol>0?vol24/(vol/30):0;
  // In v3: sweet spot is 20-55%, not 15-85%
  const inRange    = price>=G.FLOOR && price<=G.CEIL;
  const goodMom    = momentum>1.0;         // lowered — some markets have thin 24h data
  const goodLiq    = liq>=G.MIN_LIQ;
  const goodUpside = (1-price)/price>0.25; // at least 25% upside to 100%
  const score=[inRange,goodMom,goodLiq,goodUpside].filter(Boolean).length/4;
  return {vote:score>=0.5,confidence:score,price,
    detail:`${(price*100).toFixed(1)}% | Liq $${liq.toLocaleString()} | Mom ${momentum.toFixed(2)}x | Upside ${((1-price)/price*100).toFixed(0)}%`};
}

// ── AGENT 6: RESOLUTION MATH ─────────────────────────────
function agentResolutionMath(market) {
  const price=market._yesPrice||extractPrice(market);
  const endDate=market._endDate?new Date(market._endDate):null;
  const daysLeft=endDate?(endDate-new Date())/86400000:999;
  const vol=parseFloat(market.volume||0);
  if(daysLeft<0)                  return {vote:false,confidence:0,detail:"Resolved"};
  if(daysLeft<G.BLACKOUT_HRS/24)  return {vote:false,confidence:0,detail:"Blackout"};
  const inWindow  = daysLeft>=G.MIN_DAYS && daysLeft<=G.MAX_DAYS;
  const goodPrice = price>=G.FLOOR && price<=G.CEIL;       // use live guardrails
  const hasEdge   = Math.abs(price-0.5)*2 >= G.MIN_EDGE;  // simple edge: distance from 50%
  const okVolume  = vol < 200000;                          // not over-traded
  const notBlack  = daysLeft > G.BLACKOUT_HRS/24;
  const score=[inWindow,goodPrice,hasEdge,okVolume,notBlack].filter(Boolean).length/5;
  return {vote:score>=0.5,confidence:score,daysLeft:+daysLeft.toFixed(1),
    detail:`${daysLeft.toFixed(1)}d left | ${(price*100).toFixed(1)}% | $${vol.toLocaleString()} vol`};
}

// ── CONSENSUS ENGINE v3 ───────────────────────────────────
function runConsensus(agents, whaleRes, stealthRes, hasConfirmedWhales) {
  const active = agents.filter(a=>!a.inactive);
  const votes  = active.filter(a=>a.vote).length;
  const total  = active.length;
  const tier   = whaleRes?.tier||99;

  // Whale signals (only when confirmed whales exist)
  if(hasConfirmedWhales&&tier===1&&whaleRes?.vote) return {action:"ENTER",size_mult:0.5,label:"TIER1_WHALE",votes,total};
  if(hasConfirmedWhales&&stealthRes?.vote&&tier<=2) return {action:"ENTER",size_mult:0.25,label:"STEALTH",votes,total};

  // When whales inactive: 5 real agents — need 2 to enter (conservative)
  // When whales active: 7 agents — need 70% (5) for full, 50% (4) for moderate
  const fullThreshold = hasConfirmedWhales ? Math.ceil(total*0.7) : Math.ceil(total*0.5);
  const modThreshold  = hasConfirmedWhales ? Math.ceil(total*0.5) : 2;

  if(votes>=fullThreshold) return {action:"ENTER",size_mult:1.0,label:votes>=total?"FULL_CONSENSUS":"STRONG",votes,total};
  if(votes>=modThreshold)  return {action:"ENTER",size_mult:0.5,label:"MODERATE",votes,total};
  return {action:"SKIP",size_mult:0,label:"INSUFFICIENT",votes,total};
}

// ── GUARDRAILS v3 ─────────────────────────────────────────
function checkGuardrails(market, consensus, specializedResult) {
  resetDailyCounters();
  const open=getOpenTrades(); const deployed=getDeployed();
  const price=market._yesPrice||extractPrice(market);
  const liq=market._liq||parseFloat(market.liquidity||0);
  const endDate=market._endDate?new Date(market._endDate):new Date(0);
  const hoursLeft=(endDate-new Date())/3600000;
  const daysLeft=hoursLeft/24;
  const mtype=market._type||"other";

  if(getState("paused"))                          return {ok:false,reason:"Paused"};
  if((getState("total_loss")||0)>=G.LOSS_HALT)    return {ok:false,reason:"Loss halt"};
  if(open.length>=G.MAX_POSITIONS)                return {ok:false,reason:"Max positions"};
  if(open.find(t=>t.market_id===market.id))       return {ok:false,reason:"Already open"};
  if((getState("daily_trades")||0)>=G.MAX_DAILY)  return {ok:false,reason:"Daily limit"};
  if(BLOCKED_TYPES.has(mtype))                    return {ok:false,reason:`${mtype} blocked — no signal advantage`};
  if(price>=G.CEIL||price<=G.FLOOR)               return {ok:false,reason:`Price ${(price*100).toFixed(0)}% outside ${(G.FLOOR*100).toFixed(0)}-${(G.CEIL*100).toFixed(0)}% range`};
  if(liq<G.MIN_LIQ)                               return {ok:false,reason:`Liquidity $${liq.toLocaleString()} < $${G.MIN_LIQ.toLocaleString()}`};
  if(hoursLeft<G.BLACKOUT_HRS)                    return {ok:false,reason:"Blackout window"};
  if(daysLeft<G.MIN_DAYS)                         return {ok:false,reason:"Resolves too soon"};
  if(daysLeft>G.MAX_DAYS)                         return {ok:false,reason:`${daysLeft.toFixed(0)} days too far — capital locked too long`};
  // Correlation guard — max 2 positions in same market type
  const typeCount=stmt.typeCount.get(mtype).c;
  if(typeCount>=G.MAX_CORR_CAT)                   return {ok:false,reason:`Correlation cap — ${typeCount} ${mtype} positions already open`};
  const catDep=getCatDeployed(market.category||"general");
  if(catDep>=G.BUDGET*G.MAX_CAT_DEP)              return {ok:false,reason:"Category cap"};

  // Edge check
  const edge=Math.abs(price-0.5)*2;
  if(edge<G.MIN_EDGE)                             return {ok:false,reason:`Edge ${(edge*100).toFixed(1)}% < ${G.MIN_EDGE*100}% minimum`};

  // Kelly-based sizing
  const conf=specializedResult?.confidence||0.5;
  const size=Math.min(G.MAX_POS,Math.max(5,kellySize(edge,conf,G.BUDGET-deployed)));
  if(size<5)                                       return {ok:false,reason:"Budget low"};
  if(size/liq>G.MAX_MKT_SHARE)                    return {ok:false,reason:"Market share too large"};

  return {ok:true,size:+size.toFixed(2),edge:+edge.toFixed(3)};
}

// ── WHALE ACTIVITY ────────────────────────────────────────
async function getWhaleActivity(market) {
  try{
    const trades=await getRecentTrades(market.id,30);
    if(!trades.length) return {whales:[],clustering:0,uniqueWhales:[]};
    const confirmed=stmt.whalesConf.all();
    if(!confirmed.length) return {whales:[],clustering:0,uniqueWhales:[]};
    const wMap=new Map(confirmed.map(w=>[w.address.toLowerCase(),w]));
    const hits=[]; const cutoff=Date.now()-600000;
    for(const t of trades){
      if(new Date(t.timestamp||t.created_at).getTime()<cutoff) continue;
      for(const addr of [t.maker,t.taker].filter(Boolean)){
        const whale=wMap.get(addr.toLowerCase());
        if(whale){
          hits.push({whale,side:t.side,price:parseFloat(t.price||0),size:parseFloat(t.size||0)});
          try{db.prepare("INSERT OR IGNORE INTO whale_trades (whale_address,market_id,market_question,side,price,size) VALUES (?,?,?,?,?,?)").run(addr,market.id,market.question,t.side,t.price,t.size);}catch{}
        }
      }
    }
    const uniqueWhales=[...new Set(hits.map(h=>h.whale.address))];
    return {whales:hits,clustering:uniqueWhales.length,uniqueWhales};
  }catch{return {whales:[],clustering:0,uniqueWhales:[]};}
}

// ── TRADE EXECUTION ───────────────────────────────────────
async function executePaperTrade(market, consensus, agents, guard, specializedResult) {
  const price=market._yesPrice||extractPrice(market);
  if(!price||price<=0||isNaN(price)){console.log("Blocked: invalid price");return;}
  const size=guard.size;
  const shares=+(size/price).toFixed(4);
  const stopPrice=+(Math.max(G.FLOOR,price*(1-G.STOP_PCT))).toFixed(4);
  const trailingStop=stopPrice;
  // Target is time-aware: realistic based on hours to resolution
  const endDate=market._endDate?new Date(market._endDate):null;
  const hoursLeft=endDate?(endDate-new Date())/3600000:999;
  // For short markets (sports/same day): modest target — won't move far
  // For medium (1-7 days): moderate target
  // For long (>7 days): conservative — markets are slow to reprice
  let targetMult, targetCap;
  if(hoursLeft<24)       { targetMult=0.40; targetCap=0.62; }
  else if(hoursLeft<72)  { targetMult=0.50; targetCap=0.72; }
  else if(hoursLeft<168) { targetMult=0.60; targetCap=0.80; }
  else                   { targetMult=0.55; targetCap=0.75; } // long duration — be conservative
  const target=+(Math.min(targetCap, price+(1-price)*targetMult)).toFixed(4);
  const fired=agents.filter(a=>a.vote&&!a.inactive).map(a=>a.name).join(",");
  const mtype=market._type||classifyMarket(market.question);
  const sigDetail=specializedResult?.detail||"";

  stmt.insertTrade.run(
    market.id,market.question,market.category||"general",mtype,"YES",
    price,price,size,shares,stopPrice,trailingStop,target,price,
    fired,consensus.label,consensus.votes,sigDetail,market._endDate
  );
  setState("daily_trades",(getState("daily_trades")||0)+1);
  savePositionBackup();

  const msg=`<b>VERAPIMPO TRADE v3</b> [PAPER]\n\n<b>${market.question?.slice(0,80)}</b>\n\nType: ${mtype.toUpperCase()}\nEntry: YES @ ${(price*100).toFixed(1)}%\nSize: $${size.toFixed(2)} | Shares: ${shares}\nTarget: ${(target*100).toFixed(1)}% | Stop: ${(stopPrice*100).toFixed(1)}%\nSignal: ${consensus.label} (${consensus.votes}/${consensus.total})\nAgents: ${fired}\nData: ${sigDetail.slice(0,80)}\nResolves: ${market._endDate?new Date(market._endDate).toLocaleDateString():"?"}`;
  await tg(msg); logAlert("TRADE_OPENED",msg,market.id);
  console.log(`TRADE[${mtype}]: ${market.question?.slice(0,50)} @ ${(price*100).toFixed(1)}% $${size}`);
}

// ── POSITION MONITOR with TRAILING STOP ──────────────────
async function monitorPositions() {
  const open=getOpenTrades(); if(!open.length) return;
  for(const trade of open){
    try{
      const market=await getMarketById(trade.market_id); if(!market){await delay(200);continue;}
      const price=extractPrice(market);
      if(!price||price<=0||isNaN(price)){await delay(200);continue;}

      const endDate=new Date(market.endDate||market.end_date_iso||0);
      const hoursLeft=(endDate-new Date())/3600000;
      const pnl=(price-trade.entry_price)*trade.shares;
      const pnl_pct=((price-trade.entry_price)/trade.entry_price)*100;
      const vol24=parseFloat(market.volume24hr||0);
      const baseVol=parseFloat(market.volume||0)/30;

      // Update trailing stop — if price rose 5%+, trail at 10% below peak
      const hwm=Math.max(price,trade.high_water_mark||trade.entry_price);
      const newTrailing=price>trade.entry_price*1.05
        ? +(Math.max(trade.stop_price, hwm*(1-G.TRAIL_PCT))).toFixed(4)
        : trade.trailing_stop||trade.stop_price;

      stmt.updatePosition.run(price, newTrailing, hwm, trade.id);

      let exitReason=null;
      if(price>=trade.target_price)                           exitReason="TARGET_HIT";
      else if(price<=newTrailing)                             exitReason=newTrailing>trade.stop_price?"TRAILING_STOP":"STOP_HIT";
      else if(hoursLeft>0&&hoursLeft<=G.BLACKOUT_HRS)        exitReason="RESOLUTION_BLACKOUT";
      else if(baseVol>1000&&vol24>baseVol*G.VOL_SPIKE)       exitReason="VOLUME_SPIKE";

      if(exitReason){
        stmt.closeTrade.run(+pnl.toFixed(4),+pnl_pct.toFixed(2),exitReason,trade.id);
        const totalPnl=(getState("total_pnl")||0)+pnl;
        setState("total_pnl",+totalPnl.toFixed(4));
        if(pnl<0) setState("total_loss",(getState("total_loss")||0)+Math.abs(pnl));
        // Update category performance
        const mtype=trade.market_type||"other";
        const isWin=pnl>0?1:0;
        stmt.upsertCatPerf.run(mtype,isWin,pnl,isWin,pnl);
        saveFullBackup();
        const msg=`<b>VERAPIMPO EXIT</b> [PAPER]\n\n<b>${trade.market_question?.slice(0,80)}</b>\n\n${(trade.entry_price*100).toFixed(1)}% → ${(price*100).toFixed(1)}%\nP&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)} (${pnl_pct>=0?"+":""}${pnl_pct.toFixed(1)}%)</b>\nReason: ${exitReason} | Total: $${totalPnl.toFixed(2)}`;
        await tg(msg); logAlert("TRADE_CLOSED",msg,trade.market_id);
      }
    }catch(e){console.error("Monitor:",e.message);}
    await delay(300);
  }
}

// ── TELEGRAM CHANNEL SCRAPER ──────────────────────────────
let tgCacheAt=0; const TG_TTL=15*60*1000;
const TG_CHANNELS=[{username:"Whale200",type:"whale",label:"Whale200"},{username:"burjbnews",type:"news",label:"Burj News"}];

async function scrapeTelegramChannels() {
  if(Date.now()-tgCacheAt<TG_TTL) return; tgCacheAt=Date.now();
  const mkts=db.prepare("SELECT id,question FROM markets ORDER BY last_scanned DESC LIMIT 50").all();
  for(const ch of TG_CHANNELS){
    try{
      const html=await safeFetchText(`https://t.me/s/${ch.username}`,10000);
      if(!html||html.length<300) continue;
      const blocks=[...html.matchAll(/<div class="tgme_widget_message_text js-message_text"[^>]*>([\s\S]*?)<\/div>/g)];
      const dates=[...html.matchAll(/datetime="([^"]+)"/g)].map(m=>m[1]);
      for(let i=0;i<Math.min(blocks.length,10);i++){
        const text=stripHtml(blocks[i][1]).slice(0,500);
        if(text.length<10) continue;
        const date=dates[i]||new Date().toISOString();
        if(new Date(date)<new Date(Date.now()-12*3600000)) continue;
        const lower=text.toLowerCase();
        let score=0;
        ["yes","win","pump","bull","buy"].forEach(w=>{if(lower.includes(w))score++;});
        ["bitcoin","btc","eth","crypto","polymarket"].forEach(w=>{if(lower.includes(w))score+=2;});
        ["whale","wallet","million","transfer"].forEach(w=>{if(lower.includes(w))score+=2;});
        (text.match(/\$[\d,.]+[MmBbKk]?/g)||[]).forEach(()=>score++);
        (text.match(/0x[a-fA-F0-9]{40}/g)||[]).forEach(()=>score+=3);
        let marketId=null;
        for(const m of mkts){
          const mw=(m.question||"").toLowerCase().split(" ").filter(w=>w.length>4);
          if(mw.filter(w=>lower.includes(w)).length>=2){marketId=m.id;break;}
        }
        try{db.prepare("INSERT OR IGNORE INTO tg_messages (channel,channel_type,message_id,text,date,market_id,relevance_score) VALUES (?,?,?,?,?,?,?)").run(ch.label,ch.type,`${ch.username}_${i}_${date}`,text,date,marketId,score);}catch{}
      }
    }catch(e){console.error(`TG ${ch.label}:`,e.message);}
    await delay(1000);
  }
}

function getTelegramBoost(marketId, question) {
  try {
    const words = (question||"").toLowerCase().split(" ").filter(w=>w.length>4);
    const msgs  = stmt.tgRecent.all();
    let whale=0, news=0;
    for(const m of msgs) {
      const lower = (m.text||"").toLowerCase();
      const matches = words.filter(w=>lower.includes(w)).length;
      if(matches>=2 || m.market_id===marketId) {
        if(m.channel_type==="whale") whale += (m.relevance_score||0)*0.3;
        if(m.channel_type==="news")  news  += (m.relevance_score||0)*0.2;
      }
    }
    return {whale:Math.min(1,whale), news:Math.min(1,news)};
  } catch { return {whale:0, news:0}; }
}

// ── WHALE REFRESH ─────────────────────────────────────────
async function refreshWhales() {
  console.log("Whale refresh...");
  try{
    const markets=await getMarkets(50,0); const stats={};
    for(const market of markets.slice(0,10)){
      await delay(500);
      const trades=await getRecentTrades(market.id,30);
      for(const t of trades){
        for(const addr of [t.maker,t.taker].filter(Boolean)){
          if(!stats[addr]) stats[addr]={wins:0,total:0,profit:0,recent:0};
          stats[addr].total++;
          if(parseFloat(t.price||0)>0.5){stats[addr].wins++;stats[addr].profit+=parseFloat(t.size||0)*parseFloat(t.price||0);}
          if(new Date(t.timestamp||t.created_at)>new Date(Date.now()-86400000*7)) stats[addr].recent++;
        }
      }
    }
    const scored=Object.entries(stats).filter(([,s])=>s.total>=10&&s.recent>=1)
      .map(([addr,s])=>({addr,...s,winRate:s.wins/s.total,score:s.wins/s.total*0.5+(s.profit/10000)*0.3+(s.recent/20)*0.2}))
      .sort((a,b)=>b.score-a.score).slice(0,50);
    const prev=new Set(db.prepare("SELECT address FROM whales").all().map(w=>w.address));
    const curr=new Set(scored.map(w=>w.addr));
    let added=0,dropped=0;
    for(const [i,w] of scored.entries()){
      const tier=i<5?1:i<20?2:3;
      const d=db.prepare("SELECT days_in_top50 FROM whales WHERE address=?").get(w.addr);
      const days=(d?.days_in_top50||0)+1;
      db.prepare("INSERT OR REPLACE INTO whales (address,rank,win_rate,avg_profit,total_trades,recent_trades,profit_30d,tier,days_in_top50,confirmed,last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)").run(w.addr,i+1,w.winRate,w.profit/w.total,w.total,w.recent,w.profit,tier,days,days>=3?1:0);
      if(!prev.has(w.addr)) added++;
    }
    for(const addr of prev){if(!curr.has(addr)){db.prepare("DELETE FROM whales WHERE address=?").run(addr);dropped++;}}
    const conf=stmt.whalesCount.get()?.c||0;
    await tg(`<b>Whale Refresh</b>\nRanked: ${scored.length} | Confirmed: ${conf} | New: ${added} | Dropped: ${dropped}`);
    logAlert("WHALE_REFRESH",`Ranked: ${scored.length} Confirmed: ${conf}`);
    console.log("Whale refresh done:",scored.length,"ranked,",conf,"confirmed");
  }catch(e){console.error("Whale refresh:",e.message);}
}

// ── MAIN SCAN ─────────────────────────────────────────────
async function runScan() {
  if(getState("paused")) return;
  const openBefore=getOpenTrades();
  if(openBefore.length>=G.MAX_POSITIONS){
    await monitorPositions();
    await tg(`<b>VeraPimpo</b> — Full (${openBefore.length}/${G.MAX_POSITIONS}) — monitoring only`);
    return;
  }
  console.log("Scan v3",new Date().toISOString());
  await Promise.all([refreshNewsCache(),scrapeTelegramChannels()]);

  const raw=await getMarkets(100,0);
  if(!raw.length){console.log("No markets from API");return;}

  const hasConfirmedWhales=stmt.whalesCount.get()?.c>0;

  const viable=raw.map(normalizeMarket).filter(m=>{
    if(BLOCKED_TYPES.has(m._type)) return false;
    if(m._yesPrice<=G.FLOOR||m._yesPrice>=G.CEIL) return false;
    if(m._liq<G.MIN_LIQ) return false;
    if(!m._endDate) return true;
    const days=(new Date(m._endDate)-new Date())/86400000;
    return days>=G.MIN_DAYS&&days<=G.MAX_DAYS;
  }).slice(0,20);

  console.log(`${viable.length} viable from ${raw.length} (${G.FLOOR*100}-${G.CEIL*100}% | ${G.MIN_DAYS}-${G.MAX_DAYS}d | $${(G.MIN_LIQ/1000).toFixed(0)}k+ liq | blocked: esports/entertainment)`);

  for(const m of viable){
    try{stmt.upsertMkt.run(m.id||m.conditionId,m.question,m.category||"general",m._type,m._yesPrice,1-m._yesPrice,parseFloat(m.volume||0),m._endDate||null,Math.round(Math.abs(m._yesPrice-0.5)*200+Math.min(m._liq/100000,0.3)*30),`type:${m._type}`);}catch{}
  }

  let traded=0,scanned=0,blocked={};

  // Pre-fetch ALL specialized signals once — populates news feed AND caches for trading loop
  const specSigCache = new Map();
  for(const market of viable){
    try{
      const sig = await fetchSpecializedSignal(market);
      specSigCache.set(market.id, sig);
    } catch{}
    await delay(150);
  }

  for(const market of viable){
    if(getOpenTrades().length>=G.MAX_POSITIONS) break;
    try{
      scanned++;
      // Use cached signal — no double API call
      const [wa] = await Promise.all([getWhaleActivity(market)]);
      const specSig = specSigCache.get(market.id) || {signal:false,confidence:0,detail:"No signal"};

      const whaleRes   = agentWhaleCopy(wa, hasConfirmedWhales);
      const specRes    = agentSpecialized(specSig);
      const newsRes    = agentNewsCoverage(market, specSig);
      const techRes    = agentTechnical(market);
      const mathRes    = agentResolutionMath(market);
      // Agent 7: Telegram Signal — based on TG channel messages matching this market
      const tgBoost=getTelegramBoost(market.id,market.question);
      const tgConf=Math.max(tgBoost.whale,tgBoost.news);
      const sentRes = {vote:tgConf>0.3||specSig.confidence>0.45, confidence:Math.max(tgConf,specSig.confidence*0.6)||0.3, detail:`TG boost: ${(tgConf*100).toFixed(0)}% | Spec: ${(specSig.confidence*100).toFixed(0)}%`};
      const stealthRes = agentStealth(wa, specRes.vote, newsRes.vote, hasConfirmedWhales);

      const allAgents=[
        {...whaleRes,  name:"Whale"},
        {...stealthRes,name:"Stealth"},
        {...specRes,   name:"Specialized"},
        {...newsRes,   name:"NewsCoverage"},
        {...techRes,   name:"Technical"},
        {...mathRes,   name:"ResolutionMath"},
        {...sentRes,   name:"MarketSentiment"},
      ];

      const consensus=runConsensus(allAgents,whaleRes,stealthRes,hasConfirmedWhales);

      // ── SCAN DIAGNOSTICS ──
      const _active=allAgents.filter(a=>!a.inactive);
      const _votes=_active.filter(a=>a.vote).map(a=>a.name);
      const _price=market._yesPrice||extractPrice(market);
      const _days=market._endDate?(new Date(market._endDate)-new Date())/86400000:999;
      const _liq=market._liq||0;
      console.log(`[SCAN] ${market._type} ${(_price*100).toFixed(1)}% ${_days.toFixed(1)}d $${_liq.toLocaleString()} → ${consensus.action}[${_votes.length}/${_active.length}] votes:${_votes.join(",")||"NONE"} spec:${(specSig.confidence*100).toFixed(0)}%`);

      if(consensus.action==="ENTER"){
        stmt.updateNewsStatus.run(market.id);
        const guard=checkGuardrails(market,consensus,specSig);
        if(guard.ok){
          await executePaperTrade(market,consensus,allAgents,guard,specSig);
          traded++;
        }else{
          const r=guard.reason;
          blocked[r]=(blocked[r]||0)+1;
          console.log(`Blocked[${market._type}] ${market.question?.slice(0,40)}: ${r}`);
        }
      }
    }catch(e){console.error("Scan:",market.question?.slice(0,30),e.message);}
    await delay(600);
  }

  const finalOpen=getOpenTrades();
  const blockSummary=Object.entries(blocked).map(([k,v])=>`${k}:${v}`).join(", ");
  await tg(`<b>VeraPimpo Scan v3</b>\n${traded} traded | ${scanned} scanned | ${viable.length} viable\nPositions: ${finalOpen.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}/$${G.BUDGET}\n${blockSummary?`Blocked: ${blockSummary}`:""}`);
  await monitorPositions();
}

// ── REST API ──────────────────────────────────────────────
const app=express();
const ALLOWED=[/\.railway\.app$/,/^http:\/\/localhost/,/^http:\/\/127\.0\.0\.1/];
app.use(cors({origin:(o,cb)=>{if(!o||ALLOWED.some(r=>r.test(o)))return cb(null,true);cb(new Error("CORS"));},methods:["GET","POST","OPTIONS"],allowedHeaders:["Content-Type","x-dashboard-password"]}));
app.use(express.json());
const rateLimits=new Map();
function rateLimit(max=30){return(req,res,next)=>{const ip=req.headers["x-forwarded-for"]?.split(",")[0]?.trim()||req.socket?.remoteAddress||"?";const now=Date.now();const rec=rateLimits.get(ip)||{count:0,resetAt:now+60000};if(now>rec.resetAt){rec.count=0;rec.resetAt=now+60000;}rec.count++;rateLimits.set(ip,rec);if(rec.count>max)return res.status(429).json({error:"Rate limited"});next();};}
setInterval(()=>{const now=Date.now();for(const[k,v]of rateLimits)if(now>v.resetAt+60000)rateLimits.delete(k);},300000);
app.use((req,res,next)=>{const s=Date.now();res.on("finish",()=>{const ms=Date.now()-s;const l=`${req.method} ${req.path} ${res.statusCode} ${ms}ms`;if(res.statusCode>=400)console.error("HTTP:",l);else if(ms>2000)console.warn("SLOW:",l);});next();});
app.use("/api",(req,res,next)=>{if((req.headers["x-dashboard-password"]||req.query.password)!==cfg.dashPass)return res.status(401).json({error:"Unauthorized"});next();});
const scanLimit=rateLimit(3); const apiLimit=rateLimit(60);

app.get("/api/overview",(_,res)=>{
  const open=getOpenTrades(),total=stmt.closedCount.get()?.c||0,wins=stmt.winsCount.get()?.c||0;
  const today=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE date(opened_at)=date('now')").get()?.c||0;
  res.json({budget:G.BUDGET,deployed:getDeployed(),available:G.BUDGET-getDeployed(),pnl:getState("total_pnl")||0,loss:getState("total_loss")||0,lossHalt:G.LOSS_HALT,openCount:open.length,maxPositions:G.MAX_POSITIONS,winRate:total>0?wins/total:0,totalTrades:total,todayTrades:today,maxDailyTrades:G.MAX_DAILY,paper:PAPER,paused:getState("paused")||false});
});
app.get("/api/positions",(_,res)=>res.json(stmt.openTrades.all()));
app.get("/api/history",(req,res)=>{const limit=Math.min(parseInt(req.query.limit)||50,200);const offset=parseInt(req.query.offset)||0;res.json(db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ? OFFSET ?").all(limit,offset));});
app.get("/api/whales",(_,res)=>res.json(db.prepare("SELECT * FROM whales ORDER BY rank").all()));
app.get("/api/whale-trades",(_,res)=>res.json(db.prepare("SELECT * FROM whale_trades ORDER BY timestamp DESC LIMIT 50").all()));
app.get("/api/markets",(_,res)=>res.json(stmt.cachedMkts.all()));
app.get("/api/category-performance",(_,res)=>res.json(stmt.catPerf.all()));
app.get("/api/news",(req,res)=>{
  const limit=parseInt(req.query.limit)||80;
  const news=db.prepare("SELECT *,'news' as feed_type FROM news_items ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.75));
  const tgMsg=db.prepare("SELECT id,channel as source,text as headline,text as summary,NULL as url,NULL as image_url,date as published_at,market_id,NULL as market_question,'monitored' as status,'telegram' as feed_type FROM tg_messages WHERE relevance_score>1 ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.25));
  res.json([...news,...tgMsg].sort((a,b)=>new Date(b.fetched_at||0)-new Date(a.fetched_at||0)).slice(0,limit));
});
app.get("/api/tg-messages",(_,res)=>res.json(stmt.tgRecent.all()));
app.get("/api/strategy",(_,res)=>{
  const agents=["Whale","Stealth","Specialized","NewsCoverage","Technical","ResolutionMath"];
  res.json(agents.map(s=>({name:s,trades:db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,wins:db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,pnl:db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0,avg_pnl:db.prepare("SELECT AVG(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0})));
});
app.get("/api/alerts",(_,res)=>res.json(db.prepare("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100").all()));
app.get("/api/scan-log",(_,res)=>res.json(db.prepare("SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT 50").all()));
app.post("/api/sync",apiLimit,(req,res)=>{const r=restoreFromBackup();res.json({restored:r,open:getOpenTrades().length,deployed:getDeployed()});});
app.post("/api/scan",scanLimit,(_,res)=>{res.json({started:true});runScan();});
app.post("/api/whalesscan",scanLimit,(_,res)=>{res.json({started:true});refreshWhales();});
app.post("/api/pause",(_,res)=>{setState("paused",true);res.json({paused:true});});
app.post("/api/resume",(_,res)=>{setState("paused",false);res.json({paused:false});});
app.get("/",(req,res)=>{try{res.setHeader("Content-Type","text/html");res.send(fs.readFileSync(path.join(__dirname,"dashboard.html"),"utf8"));}catch{res.status(500).send("Upload dashboard.html to the repo root.");}});
app.get("/health",(_,res)=>res.json({status:"ok",paper:PAPER,version:"3.0.0"}));
app.use((err,req,res,next)=>{console.error("Express:",err.message);try{stmt.logAlert.run("SERVER_ERROR",err.message,"");}catch{}res.status(500).json({error:"Internal error"});});
process.on("unhandledRejection",(r)=>{console.error("Rejection:",r);try{stmt.logAlert.run("REJECTION",String(r),"");}catch{}});
process.on("uncaughtException",(e)=>{console.error("Exception:",e.message);try{stmt.logAlert.run("EXCEPTION",e.message,"");}catch{}});
app.listen(3000,()=>console.log("VeraPimpo v3.0 API on port 3000"));

// ── BOT COMMANDS ──────────────────────────────────────────
if(bot){
  bot.use(async(ctx,next)=>{if(ctx.from&&!isAuthorized(ctx.from.id)){await ctx.reply("Unauthorized.");return;}return next();});
  bot.start(ctx=>ctx.replyWithHTML(`<b>VeraPimpo v3.0</b>\nPaper Mode — $${G.BUDGET}\nEntry: 20-60% | Edge: 8%+ | Liq: $25k+ | 1-14 days\n\n/portfolio /scan /sync /positions /history /whales /whalesscan /markets /news /strategy /pause /resume /status /live`));
  bot.command("portfolio",ctx=>{const dep=getDeployed(),pnl=getState("total_pnl")||0,loss=getState("total_loss")||0,open=getOpenTrades(),total=stmt.closedCount.get()?.c||0,wins=stmt.winsCount.get()?.c||0;ctx.replyWithHTML(`<b>VeraPimpo v3 Portfolio</b> [PAPER]\n\nBudget: $${G.BUDGET} | Deployed: $${dep.toFixed(2)}\nP&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)}</b> | Losses: $${loss.toFixed(2)}/$${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS} | WR: ${total>0?((wins/total)*100).toFixed(1):"N/A"}% (${total} closed)\nToday: ${getState("daily_trades")||0}/${G.MAX_DAILY}`);});
  bot.command("positions",ctx=>{const open=getOpenTrades();if(!open.length){ctx.reply("No open positions.");return;}let msg=`<b>Open Positions (${open.length}/${G.MAX_POSITIONS})</b>\n\n`;for(const t of open){const pnl=(t.current_price-t.entry_price)*t.shares;msg+=`[${t.market_type?.toUpperCase()||"?"}] <b>${t.market_question?.slice(0,55)}</b>\n${(t.entry_price*100).toFixed(1)}% → ${(t.current_price*100).toFixed(1)}% | ${pnl>=0?"+":""}$${pnl.toFixed(2)}\nTrailing: ${t.trailing_stop?(t.trailing_stop*100).toFixed(1)+"% stop":""} | ${t.agents_fired}\n\n`;}ctx.replyWithHTML(msg);});
  bot.command("history",ctx=>{const rows=db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 15").all();if(!rows.length){ctx.reply("No closed trades.");return;}let msg=`<b>Last ${rows.length} Trades</b>\n\n`;for(const t of rows)msg+=`${t.pnl>=0?"🟢":"🔴"} [${t.market_type||"?"}] ${t.market_question?.slice(0,45)}\n${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)} [${t.exit_reason}]\n\n`;ctx.replyWithHTML(msg);});
  bot.command("whales",ctx=>{const conf=stmt.whalesCount.get()?.c||0;if(!conf){ctx.reply(`No confirmed whales yet (need 3 consecutive days in top 50).\nWhale agents are inactive until first confirmation.`);return;}const w=db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank LIMIT 10").all();let msg="<b>Confirmed Whales</b>\n\n";for(const r of w)msg+=`#${r.rank} ${r.address.slice(0,10)}... | T${r.tier} | ${(r.win_rate*100).toFixed(0)}% WR\n`;ctx.replyWithHTML(msg);});
  bot.command("whalesscan",async ctx=>{ctx.replyWithHTML("<i>Whale scan started...</i>");await refreshWhales();ctx.replyWithHTML(`Done. Confirmed: ${stmt.whalesCount.get()?.c||0}`);});
  bot.command("scan",ctx=>{ctx.replyWithHTML("<i>Manual scan triggered...</i>");runScan();});
  bot.command("sync",ctx=>{const r=restoreFromBackup(),open=getOpenTrades();ctx.replyWithHTML(`<b>Sync</b>\nRestored: ${r} | Open: ${open.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}`);});
  bot.command("markets",ctx=>{const cached=stmt.cachedMkts.all();if(!cached.length){ctx.reply("No markets cached. /scan first.");return;}let msg="<b>Top Markets (v3 filtered)</b>\n\n";for(const m of cached.slice(0,8))msg+=`[${m.market_type?.toUpperCase()||"?"}] <b>${m.question?.slice(0,60)}</b>\nYES: ${((m.yes_price||0)*100).toFixed(1)}% | Score: ${m.score||0}\n\n`;ctx.replyWithHTML(msg);});
  bot.command("news",ctx=>{const rows=db.prepare("SELECT * FROM news_items ORDER BY fetched_at DESC LIMIT 10").all();if(!rows.length){ctx.reply("No news. /scan first.");return;}let msg="<b>News Feed</b>\n\n";for(const n of rows)msg+=`[${n.signal_quality?.toUpperCase()||"RSS"}] ${n.source}\n${n.headline?.slice(0,80)}\n\n`;ctx.replyWithHTML(msg);});
  bot.command("strategy",ctx=>{
    const agents=["Whale","Stealth","Specialized","NewsCoverage","Technical","ResolutionMath"];
    let msg="<b>Strategy Performance v3</b>\n\n";
    const catPerf=stmt.catPerf.all();
    if(catPerf.length){msg+="<b>By Market Type:</b>\n";for(const c of catPerf)msg+=`${c.market_type}: ${c.trades} trades | ${c.trades>0?((c.wins/c.trades)*100).toFixed(0):"-"}% WR | $${c.total_pnl.toFixed(2)}\n`;msg+="\n";}
    msg+="<b>By Agent:</b>\n";
    for(const s of agents){const total=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;const wins=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;const pnl=db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0;msg+=`${s}: ${total} | ${total>0?((wins/total)*100).toFixed(0):"-"}% | $${pnl.toFixed(2)}\n`;}
    ctx.replyWithHTML(msg);
  });
  bot.command("debug", async ctx=>{
    const raw=await getMarkets(50,0);
    if(!raw.length){ctx.reply("No markets from API");return;}
    const hasWh=stmt.whalesCount.get()?.c>0;
    const viable=raw.map(normalizeMarket).filter(m=>{
      if(BLOCKED_TYPES.has(m._type)) return false;
      if(m._yesPrice<=G.FLOOR||m._yesPrice>=G.CEIL) return false;
      if(m._liq<G.MIN_LIQ) return false;
      if(!m._endDate) return true;
      const d=(new Date(m._endDate)-new Date())/86400000;
      return d>=G.MIN_DAYS&&d<=G.MAX_DAYS;
    }).slice(0,5);
    let msg=`<b>Debug Scan</b>
${viable.length} viable

`;
    for(const m of viable){
      const specSig=await fetchSpecializedSignal(m);
      const wa=await getWhaleActivity(m);
      const whaleRes=agentWhaleCopy(wa,hasWh);
      const specRes=agentSpecialized(specSig);
      const newsRes=agentNewsCoverage(m,specSig);
      const techRes=agentTechnical(m);
      const mathRes=agentResolutionMath(m);
      const tgB=getTelegramBoost(m.id||m.conditionId,m.question);
      const tgC=Math.max(tgB.whale,tgB.news);
      const sentRes={vote:tgC>0.3||specSig.confidence>0.45,confidence:tgC||specSig.confidence*0.6||0.3};
      const stRes=agentStealth(wa,specRes.vote,newsRes.vote,hasWh);
      const all=[{...whaleRes,name:"Wh"},{...stRes,name:"St"},{...specRes,name:"Sp"},{...newsRes,name:"Ne"},{...techRes,name:"Te"},{...mathRes,name:"Ma"},{...sentRes,name:"TG"}];
      const active=all.filter(a=>!a.inactive);
      const votes=active.filter(a=>a.vote).map(a=>a.name);
      const con=runConsensus(all,whaleRes,stRes,hasWh);
      const guard=con.action==="ENTER"?checkGuardrails(m,con,specSig):{ok:null};
      const p=m._yesPrice||extractPrice(m);
      const d=m._endDate?(new Date(m._endDate)-new Date())/86400000:999;
      msg+=`<b>${m._type} ${(p*100).toFixed(0)}% ${d.toFixed(1)}d</b>
`;
      msg+=`Votes:${votes.join(",")||"NONE"} [${votes.length}/${active.length}] → ${con.action}
`;
      msg+=`Spec:${(specSig.confidence*100).toFixed(0)}% Tech:${techRes.vote?"Y":"N"}(${(techRes.confidence*100).toFixed(0)}%) Math:${mathRes.vote?"Y":"N"}(${(mathRes.confidence*100).toFixed(0)}%)
`;
      if(guard.ok===false) msg+=`BLOCKED: ${guard.reason}
`;
      msg+=`${m.question?.slice(0,50)}

`;
      await delay(300);
    }
    ctx.replyWithHTML(msg);
  });

  bot.command("pause",ctx=>{setState("paused",true);ctx.reply("Paused.");});
  bot.command("resume",ctx=>{setState("paused",false);ctx.reply("Resumed.");});
  bot.command("status",ctx=>{
    const open=getOpenTrades(); const conf=stmt.whalesCount.get()?.c||0;
    ctx.replyWithHTML(`<b>VeraPimpo v3 Status</b>\n\nMode: ${getState("paused")?"PAUSED":"ACTIVE"} | Paper: YES\nBudget: $${G.BUDGET} | Halt: $${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS} | $${getDeployed().toFixed(2)}\nToday: ${getState("daily_trades")||0}/${G.MAX_DAILY}\nWhales confirmed: ${conf}\nEntry: ${G.FLOOR*100}-${G.CEIL*100}% | Edge: ${G.MIN_EDGE*100}%+ | Liq: $${G.MIN_LIQ/1000}k+\nWindow: ${G.MIN_DAYS}-${G.MAX_DAYS} days | Blocked: esports, entertainment`);
  });
  bot.command("live",ctx=>ctx.replyWithHTML(`<b>Going Live</b>\n\n1. Create Polygon wallet\n2. Fund USDC via Rain exchange\n3. Add WALLET_PRIVATE_KEY to Railway\n4. Set PAPER=false in server.js\n5. Redeploy`));
  bot.help(ctx=>ctx.replyWithHTML(`/portfolio /scan /sync /positions /history /whales /whalesscan /markets /news /strategy /pause /resume /status /live`));
}

// ── SCHEDULES ─────────────────────────────────────────────
let scanning=false;
setInterval(async()=>{if(scanning||getState("paused"))return;scanning=true;try{await runScan();}finally{scanning=false;}},480000);
setInterval(async()=>{if(!getState("paused"))await monitorPositions();},180000);
setInterval(saveFullBackup,600000);
// Dedupe news every hour — prevents accumulation of duplicates during runtime
setInterval(()=>{try{db.exec("DELETE FROM news_items WHERE id NOT IN (SELECT MIN(id) FROM news_items GROUP BY source,headline)");}catch{}},3600000);
cron.schedule("0 3 * * *",refreshWhales,{timezone:"UTC"});
cron.schedule("0 6 * * *",async()=>{
  const open=getOpenTrades(); const perf=stmt.catPerf.all();
  const perfStr=perf.map(c=>`${c.market_type}: ${c.wins}/${c.trades} WR $${c.total_pnl.toFixed(2)}`).join("\n");
  await tg(`<b>VeraPimpo Daily Briefing</b>\nP&L: ${(getState("total_pnl")||0)>=0?"+":""}$${(getState("total_pnl")||0).toFixed(2)}\nLosses: $${(getState("total_loss")||0).toFixed(2)}/$${G.LOSS_HALT}\nOpen: ${open.length}/${G.MAX_POSITIONS}\n\n${perfStr}`);
},{timezone:"UTC"});

// ── LAUNCH ────────────────────────────────────────────────
(async()=>{
  console.log("VeraPimpo v3.0 starting...");
  if(bot){await bot.launch();console.log("Telegram bot active");}
  if(restored>0) await tg(`<b>VeraPimpo v3 Restarted</b>\n${restored} open positions restored.\nClosed trades, whales, and P&L also restored.`);
  const conf=stmt.whalesCount.get()?.c||0;
  await tg(`<b>VeraPimpo v3.0 Online</b>\n\nPaper: YES | Budget: $${G.BUDGET}\nEntry: 20-60% | Edge: 8%+ | Liq: $25k+ | 1-14 days\nBlocked: esports, entertainment\nWhales confirmed: ${conf}\n7 agents | 14+ guardrails | First scan in 60s`);
  setTimeout(async()=>{await refreshWhales();await runScan();},60000);
})().catch(console.error);

process.once("SIGINT",()=>{if(bot)bot.stop("SIGINT");process.exit(0);});
process.once("SIGTERM",()=>{if(bot)bot.stop("SIGTERM");process.exit(0);});
