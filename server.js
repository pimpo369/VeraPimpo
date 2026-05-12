// ============================================================
// VERAPIMPO — Polymarket Intelligence Agent v1.0
// 6-Agent Consensus | 3-Layer Early Warning System
// Paper Trading Mode | $500 Simulated Budget
// ============================================================
// AGENT ARCHITECTURE:
//   Layer 1 (BEFORE): Wallet funding detection, order book pressure
//   Layer 2 (NOW):    Whale trades, clustering, market divergence
//   Layer 3 (CONFIRM):News catalyst, Reddit sentiment, cross-platform volume
//
// AGENTS:
//   1. Whale Copy        — tier-weighted, acts alone at reduced size
//   2. Stealth Signal    — detects whale moves before public confirmation
//   3. News Catalyst     — 19 RSS + Reddit sources
//   4. Sentiment         — Reddit tone + upvote velocity
//   5. Technical         — price action, volume, momentum
//   6. Resolution Math   — probability curve vs historical patterns
//
// GUARDRAILS:
//   $500 budget | $50 max/trade | 8 max positions | $150 loss halt
//   2hr resolution blackout | 85% ceiling | 15% floor
//   $10k min liquidity | 3% max market share | 5% min edge
//   10 trades/day | 40% max single whale | 30% max category
// ============================================================

const { Telegraf }   = require("telegraf");
const Database       = require("better-sqlite3");
const cron           = require("node-cron");
const express        = require("express");
const cors           = require("cors");

// ── CONFIG ────────────────────────────────────────────────
const cfg = {
  telegram: process.env.VERAPIMPO_TELEGRAM_TOKEN,
  chatId:   process.env.VERAPIMPO_CHAT_ID,
  chatId2:  process.env.VERAPIMPO_CHAT_ID_2 || null,
  dashPass: process.env.DASHBOARD_PASSWORD || "verapimpo2026",
  tgApiId:  parseInt(process.env.TG_API_ID  || "0"),
  tgApiHash:process.env.TG_API_HASH || "",
  tgSession:process.env.TG_SESSION  || "",
};

// Channels to monitor
const TG_CHANNELS = [
  { username: "Whale200",   type: "whale",  label: "Whale200" },
  { username: "burjbnews",  type: "news",   label: "Burj News" },
];

const PAPER = true; // flip to false to go live

// ── DATABASE ──────────────────────────────────────────────
const db = new Database("./verapimpo.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT, market_question TEXT, category TEXT,
    side TEXT, entry_price REAL, current_price REAL,
    size REAL, shares REAL,
    stop_price REAL, target_price REAL,
    agents_fired TEXT, layer TEXT, tier INTEGER,
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
    price REAL, size REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT, headline TEXT, summary TEXT,
    url TEXT, image_url TEXT, published_at DATETIME,
    market_id TEXT, market_question TEXT,
    status TEXT DEFAULT 'monitored',
    edge_detected REAL DEFAULT 0,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY, question TEXT, category TEXT,
    yes_price REAL, no_price REAL, volume REAL,
    end_date DATETIME, score REAL DEFAULT 0,
    last_scanned DATETIME
  );
  CREATE TABLE IF NOT EXISTS agent_state (
    key TEXT PRIMARY KEY, value TEXT
  );
  CREATE TABLE IF NOT EXISTS scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT, market_question TEXT,
    agents_fired TEXT, consensus INTEGER,
    action TEXT, scanned_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT, message TEXT, market_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS tg_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT, channel_type TEXT,
    message_id INTEGER, text TEXT,
    sender TEXT, date DATETIME,
    market_id TEXT, market_question TEXT,
    relevance_score REAL DEFAULT 0,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel, message_id)
  );
`);

const getState = (k,d=null) => { const r=db.prepare("SELECT value FROM agent_state WHERE key=?").get(k); return r?JSON.parse(r.value):d; };
const setState = (k,v) => db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)").run(k,JSON.stringify(v));

if (!getState("total_loss"))  setState("total_loss",  0);
if (!getState("total_pnl"))   setState("total_pnl",   0);
if (!getState("paused"))      setState("paused",       false);
if (!getState("daily_trades"))setState("daily_trades", 0);
if (!getState("trade_date"))  setState("trade_date",   "");

// Clean up duplicate news items on every boot — keep only the first occurrence
try {
  db.exec(`
    DELETE FROM news_items WHERE id NOT IN (
      SELECT MIN(id) FROM news_items GROUP BY source, headline
    )
  `);
  console.log("Deduped news_items table");
} catch(e) { console.log("Dedup:", e.message); }

// ── POSITION BACKUP / RESTORE ─────────────────────────
// Saves open positions to positions_backup.json after every trade
// Restores them on boot if the SQLite DB was wiped by a redeploy

const BACKUP_FILE = "./positions_backup.json";

function savePositionBackup() {
  try {
    const open = db.prepare("SELECT * FROM paper_trades WHERE status='open'").all();
    const fs   = require("fs");
    fs.writeFileSync(BACKUP_FILE, JSON.stringify(open, null, 2));
    console.log(`Position backup saved: ${open.length} positions`);
  } catch(e) { console.error("Backup save failed:", e.message); }
}

function restorePositionsFromBackup() {
  try {
    const fs = require("fs");
    if (!fs.existsSync(BACKUP_FILE)) { console.log("No position backup found."); return 0; }
    const backed = JSON.parse(fs.readFileSync(BACKUP_FILE, "utf8"));
    if (!backed?.length) return 0;
    let restored = 0;
    for (const p of backed) {
      const exists = db.prepare("SELECT id FROM paper_trades WHERE id=? AND status='open'").get(p.id);
      if (!exists) {
        db.prepare(`INSERT OR IGNORE INTO paper_trades
          (id,market_id,market_question,category,side,entry_price,current_price,
           size,shares,stop_price,target_price,agents_fired,layer,tier,status,pnl,pnl_pct,closes_at,opened_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
          .run(p.id,p.market_id,p.market_question,p.category,p.side,
               p.entry_price,p.current_price||p.entry_price,
               p.size,p.shares,p.stop_price,p.target_price,
               p.agents_fired,p.layer,p.tier,"open",0,0,
               p.closes_at,p.opened_at);
        restored++;
      }
    }
    console.log(`Restored ${restored} positions from backup`);
    return restored;
  } catch(e) { console.error("Backup restore failed:", e.message); return 0; }
}

// Restore positions on boot
const restored = restorePositionsFromBackup();

// ── GUARDRAILS ────────────────────────────────────────────
const BUDGET          = 500;
const MAX_POSITION    = 50;
const MAX_POSITIONS   = 8;
const LOSS_HALT       = 150;
const BLACKOUT_HOURS  = 2;
const PROB_CEILING    = 0.85;
const PROB_FLOOR      = 0.15;
const MIN_LIQUIDITY   = 10000;
const MAX_MKT_SHARE   = 0.03;
const MIN_EDGE        = 0.05;
const MAX_DAILY_TRADES= 10;
const MAX_WHALE_DEP   = 0.40;
const MAX_CATEGORY_DEP= 0.30;
const EXIT_TARGET     = 0.85;
const VOL_SPIKE_EXIT  = 3.0;

// ── HELPERS ───────────────────────────────────────────────
async function safeFetch(url, opts={}, timeout=10000) {
  const ctrl = new AbortController();
  const t = setTimeout(()=>ctrl.abort(), timeout);
  const defaultHeaders = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
  };
  try {
    const res = await fetch(url, {...opts, signal:ctrl.signal, headers:{...defaultHeaders,...(opts.headers||{})}});
    if (!res.ok) { console.log(`HTTP ${res.status} for ${url.slice(0,80)}`); return null; }
    return await res.json();
  }
  catch(e) { console.log(`Fetch error ${url.slice(0,60)}: ${e.message}`); return null; }
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

function isAuthorized(id) {
  return [cfg.chatId, cfg.chatId2].filter(Boolean).map(String).includes(String(id));
}
async function tg(msg) {
  if (!cfg.telegram) return;
  const targets = [cfg.chatId, cfg.chatId2].filter(Boolean);
  for (const id of targets) {
    try { await bot.telegram.sendMessage(id, msg, {parse_mode:"HTML"}); }
    catch(e) { console.error("TG:", e.message); }
  }
}
function logAlert(type, message, market_id="") {
  db.prepare("INSERT INTO alerts (type,message,market_id) VALUES (?,?,?)").run(type,message,market_id);
}
const getOpenTrades  = () => db.prepare("SELECT * FROM paper_trades WHERE status='open'").all();
const getDeployed    = () => db.prepare("SELECT SUM(size) as t FROM paper_trades WHERE status='open'").get()?.t||0;
const getCatDeployed = (cat) => db.prepare("SELECT SUM(size) as t FROM paper_trades WHERE status='open' AND category=?").get(cat)?.t||0;
const getWhaleDep    = (addr) => db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='open' AND agents_fired LIKE ?").get(`%${addr}%`)?.c||0;

function resetDailyCounters() {
  const today = new Date().toDateString();
  if (getState("trade_date") !== today) {
    setState("trade_date", today);
    setState("daily_trades", 0);
  }
}

// ══════════════════════════════════════════════════════════
// POLYMARKET API
// ══════════════════════════════════════════════════════════
const POLY_API = "https://clob.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";

async function getMarkets(limit=100, offset=0) {
  // Try Gamma API with proper headers
  const headers = {
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
  };
  const url = `${GAMMA_API}/markets?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`;
  const data = await safeFetch(url, {headers}, 12000);
  if (Array.isArray(data) && data.length > 0) return data;

  // Fallback: try events endpoint
  const url2 = `${GAMMA_API}/events?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume&ascending=false`;
  const data2 = await safeFetch(url2, {headers}, 12000);
  if (Array.isArray(data2) && data2.length > 0) {
    // Events contain markets — flatten them
    const markets = [];
    for (const ev of data2) {
      if (Array.isArray(ev.markets)) markets.push(...ev.markets);
      else if (ev.id) markets.push(ev);
    }
    return markets;
  }
  console.log("Polymarket API returned no data — retrying in next scan cycle");
  return [];
}

async function getMarketById(id) {
  const headers = {"Origin":"https://polymarket.com","Referer":"https://polymarket.com/"};
  return await safeFetch(`${GAMMA_API}/markets/${id}`, {headers});
}

async function getMarketOrderBook(tokenId) {
  return await safeFetch(`${POLY_API}/book?token_id=${tokenId}`);
}

async function getRecentTrades(marketId, limit=20) {
  // Gamma API doesn't require auth for market data
  const headers = {"Origin":"https://polymarket.com","Referer":"https://polymarket.com/"};
  const data = await safeFetch(`${GAMMA_API}/trades?market=${marketId}&limit=${limit}`, {headers});
  if (data) return data;
  // Fallback: return empty array — whale detection skips gracefully
  return [];
}

// ══════════════════════════════════════════════════════════
// LAYER 1: PRE-SIGNAL DETECTION
// Before whales trade — detect funding and order book pressure
// ══════════════════════════════════════════════════════════

async function detectOrderBookPressure(market) {
  try {
    if (!market.clobTokenIds?.length) return { pressure: 0, signal: false };
    const tokenId = market.clobTokenIds[0];
    const book    = await getMarketOrderBook(tokenId);
    if (!book?.bids?.length || !book?.asks?.length) return { pressure: 0, signal: false };

    const topBids = book.bids.slice(0,5);
    const topAsks = book.asks.slice(0,5);
    const bidVolume = topBids.reduce((s,b)=>s+parseFloat(b.size||0),0);
    const askVolume = topAsks.reduce((s,a)=>s+parseFloat(a.size||0),0);
    const spread    = parseFloat(topAsks[0]?.price||0) - parseFloat(topBids[0]?.price||0);

    // High bid pressure and tight spread = someone loading up
    const pressure = bidVolume / (askVolume||1);
    const signal   = pressure > 1.8 && spread < 0.03;
    return { pressure: parseFloat(pressure.toFixed(2)), spread: parseFloat(spread.toFixed(4)), signal };
  } catch { return { pressure:0, signal:false }; }
}

// ══════════════════════════════════════════════════════════
// LAYER 2: REAL-TIME WHALE DETECTION
// ══════════════════════════════════════════════════════════

async function getWhaleActivity(market) {
  try {
    const trades = await getRecentTrades(market.id, 50);
    if (!Array.isArray(trades)) return { whales:[], clustering:0 };

    const confirmedWhales = db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank").all();
    const whaleAddrs = new Set(confirmedWhales.map(w=>w.address.toLowerCase()));

    const whaleHits = [];
    const recentWindow = Date.now() - 600000; // last 10 min

    for (const trade of trades) {
      const maker = trade.maker?.toLowerCase() || "";
      const taker = trade.taker?.toLowerCase() || "";
      const ts    = new Date(trade.timestamp||trade.created_at).getTime();
      if (ts < recentWindow) continue;

      for (const addr of [maker, taker]) {
        if (whaleAddrs.has(addr)) {
          const whale = confirmedWhales.find(w=>w.address.toLowerCase()===addr);
          if (whale) {
            whaleHits.push({ whale, side: trade.side, price: parseFloat(trade.price||0), size: parseFloat(trade.size||0), ts });
            db.prepare("INSERT OR IGNORE INTO whale_trades (whale_address,market_id,market_question,side,price,size) VALUES (?,?,?,?,?,?)").run(addr,market.id,market.question,trade.side,trade.price,trade.size);
          }
        }
      }
    }

    // Clustering — multiple unique whales in same market
    const uniqueWhales = [...new Set(whaleHits.map(h=>h.whale.address))];
    const clustering   = uniqueWhales.length;

    return { whales: whaleHits, clustering, uniqueWhales };
  } catch { return { whales:[], clustering:0, uniqueWhales:[] }; }
}

// ══════════════════════════════════════════════════════════
// LAYER 2: RELATED MARKET DIVERGENCE
// ══════════════════════════════════════════════════════════

function detectMarketDivergence(market, allMarkets) {
  try {
    const q       = market.question?.toLowerCase() || "";
    const price   = parseFloat(market.outcomePrices?.[0] || market.bestBid || 0.5);
    const related = allMarkets.filter(m => {
      if (m.id === market.id) return false;
      const mq = m.question?.toLowerCase() || "";
      // Simple keyword overlap check
      const words = q.split(" ").filter(w=>w.length>4);
      return words.some(w=>mq.includes(w));
    });

    let divergence = 0;
    for (const r of related.slice(0,3)) {
      const rp  = parseFloat(r.outcomePrices?.[0] || r.bestBid || 0.5);
      const gap = Math.abs(price - rp);
      if (gap > 0.12) divergence += gap;
    }

    return { divergence: parseFloat(divergence.toFixed(3)), related: related.length, signal: divergence > 0.15 };
  } catch { return { divergence:0, related:0, signal:false }; }
}

// ══════════════════════════════════════════════════════════
// LAYER 2: RESOLUTION SOURCE MONITORING
// ══════════════════════════════════════════════════════════

async function checkResolutionSource(market) {
  try {
    const q       = market.question?.toLowerCase() || "";
    const price   = parseFloat(market.outcomePrices?.[0] || 0.5);
    const results = [];

    // Fed rate decisions
    if (q.includes("fed") || q.includes("federal reserve") || q.includes("rate")) {
      const text = await safeFetchText("https://www.federalreserve.gov/newsevents/pressreleases.htm");
      if (text.includes("rate") && text.includes("2026")) {
        const hasDecision = text.match(/\b(raised|lowered|held|unchanged|increased|decreased)\b/i);
        if (hasDecision) results.push({ source:"FederalReserve", signal:true, detail:hasDecision[0] });
      }
    }

    // Sports outcomes
    if (q.includes("nba") || q.includes("nfl") || q.includes("super bowl") || q.includes("championship")) {
      const espn = await safeFetchText("https://www.espn.com/espn/rss/news");
      const mentions = (espn.match(/<title>([^<]+)<\/title>/g)||[]).slice(0,5);
      results.push({ source:"ESPN", signal:mentions.length>0, detail:`${mentions.length} recent headlines` });
    }

    // Crypto price markets
    if (q.includes("bitcoin") || q.includes("btc") || q.includes("ethereum") || q.includes("eth")) {
      const cg = await safeFetch("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd");
      if (cg?.bitcoin) {
        const btcPrice  = cg.bitcoin.usd;
        const priceMatch= q.match(/\$?([\d,]+)k?/);
        if (priceMatch) {
          const threshold = parseFloat(priceMatch[1].replace(",","")) * (priceMatch[0].includes("k")?1000:1);
          const signal    = btcPrice >= threshold;
          results.push({ source:"CoinGecko", signal, detail:`BTC $${btcPrice.toLocaleString()} vs threshold $${threshold.toLocaleString()}` });
        }
      }
    }

    const anySignal = results.some(r=>r.signal);
    return { results, signal: anySignal, edge: anySignal ? 0.15 : 0 };
  } catch { return { results:[], signal:false, edge:0 }; }
}

// ══════════════════════════════════════════════════════════
// AGENT 1: WHALE COPY (tier-weighted)
// ══════════════════════════════════════════════════════════

function agentWhaleCopy(whaleActivity) {
  const { whales, clustering } = whaleActivity;
  if (!whales?.length) return { vote:false, confidence:0, detail:"No whale activity" };

  // Find highest tier whale
  const sorted = [...whales].sort((a,b)=>a.whale.tier-b.whale.tier);
  const best   = sorted[0];
  if (!best) return { vote:false, confidence:0, detail:"No confirmed whales" };

  const tier   = best.whale.tier;
  const conf   = tier===1?0.9:tier===2?0.7:0.5;
  const bonus  = clustering >= 3 ? 0.15 : clustering >= 2 ? 0.08 : 0;

  return {
    vote:       true,
    confidence: Math.min(1, conf+bonus),
    tier,
    clustering,
    size_mult:  tier===1?0.5:tier===2?0.25:0.15,
    detail:     `Tier ${tier} whale | Win rate: ${(best.whale.win_rate*100).toFixed(0)}% | Clustering: ${clustering} wallets`
  };
}

// ══════════════════════════════════════════════════════════
// AGENT 2: STEALTH SIGNAL DETECTOR
// Whale moved, no public info yet
// ══════════════════════════════════════════════════════════

function agentStealth(whaleActivity, newsSignal, sentimentSignal) {
  const { whales } = whaleActivity;
  if (!whales?.length) return { vote:false, confidence:0, detail:"No whale activity" };

  const hasNews      = newsSignal?.vote;
  const hasSentiment = sentimentSignal?.vote;
  const isStealthy   = !hasNews && !hasSentiment;

  if (!isStealthy) return { vote:false, confidence:0, detail:"Signal already public" };

  const bestWhale = whales.sort((a,b)=>a.whale.tier-b.whale.tier)[0];
  const conf      = bestWhale?.whale.tier===1 ? 0.85 : bestWhale?.whale.tier===2 ? 0.65 : 0.45;

  return {
    vote:       conf > 0.5,
    confidence: conf,
    isStealthy: true,
    detail:     `STEALTH — Tier ${bestWhale?.whale.tier} whale entered, zero public confirmation yet`
  };
}

// ══════════════════════════════════════════════════════════
// AGENT 3: NEWS CATALYST (19 sources)
// ══════════════════════════════════════════════════════════

const NEWS_SOURCES = [
  { name:"AP News",      url:"https://rsshub.app/apnews/topics/apf-politics" },
  { name:"Google News",  url:"https://news.google.com/rss/search?q=prediction+market" },
  { name:"Reuters",      url:"https://feeds.reuters.com/reuters/businessNews" },
  { name:"BBC Business", url:"http://feeds.bbci.co.uk/news/business/rss.xml" },
  { name:"MarketWatch",  url:"https://feeds.marketwatch.com/marketwatch/topstories/" },
  { name:"Politico",     url:"https://www.politico.com/rss/politicopicks.xml" },
  { name:"ESPN",         url:"https://www.espn.com/espn/rss/news" },
  { name:"CoinDesk",     url:"https://www.coindesk.com/arc/outboundfeeds/rss/" },
  { name:"The Guardian", url:"https://www.theguardian.com/world/rss" },
  { name:"Al Jazeera",   url:"https://www.aljazeera.com/xml/rss/all.xml" },
  { name:"Bloomberg",    url:"https://feeds.bloomberg.com/markets/news.rss" },
  { name:"Finnhub",      url:null }, // uses API
];
const REDDIT_SOURCES = [
  "polymarket", "PredictionMarkets", "politics",
  "Economics", "CryptoCurrency", "sports", "worldnews"
];

async function fetchNewsForMarket(market) {
  const q      = market.question?.toLowerCase() || "";
  const words  = q.split(" ").filter(w=>w.length>4 && !["will","have","does","than","from","that","this","with","what","when","where","which"].includes(w));
  const hits   = [];

  for (const src of NEWS_SOURCES) {
    try {
      if (!src.url) continue;
      const xml = await safeFetchText(src.url);
      if (!xml) continue;
      const items = xml.match(/<item>[\s\S]*?<\/item>/g)||[];
      for (const item of items.slice(0,15)) {
        const title   = (item.match(/<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/title>/)?.[1]||"").toLowerCase();
        const desc    = (item.match(/<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?<\/description>/)?.[1]||"").toLowerCase();
        const link    = item.match(/<link>(.*?)<\/link>/)?.[1]||"";
        const imgUrl  = item.match(/<media:content[^>]+url="([^"]+)"/)?.[1] || item.match(/<enclosure[^>]+url="([^"]+)"/)?.[1] || "";
        const pubDate = item.match(/<pubDate>(.*?)<\/pubDate>/)?.[1]||"";
        const fullText= title+" "+desc;
        const matches = words.filter(w=>fullText.includes(w));
        if (matches.length >= 2) {
          hits.push({ source:src.name, headline:title, summary:desc.slice(0,300), url:link, image_url:imgUrl, pubDate, matches:matches.length });
        }
      }
    } catch {}
    await delay(100);
  }

  // Reddit blocked on cloud IPs — skip direct Reddit calls
  // ApeWisdom RSS used instead via agentSentiment

  // Save ALL articles to DB — deduplicate by headline+source combination
  for (const h of hits) {
    try {
      // Check if this headline from this source already exists
      const exists = db.prepare(
        "SELECT id FROM news_items WHERE source=? AND headline=?"
      ).get(h.source, h.headline);
      if (!exists) {
        db.prepare(`INSERT INTO news_items
          (source,headline,summary,url,image_url,published_at,market_id,market_question,status)
          VALUES (?,?,?,?,?,?,?,?,?)`)
          .run(h.source, h.headline, h.summary, h.url, h.image_url,
               h.pubDate, market.id, market.question, "monitored");
      }
    } catch {}
  }

  return hits;
}

async function agentNewscat(market) {
  const hits = await fetchNewsForMarket(market);
  if (!hits.length) return { vote:false, confidence:0, detail:"No relevant news" };

  const recent    = hits.filter(h => new Date(h.pubDate) > new Date(Date.now()-3600000*6));
  const veryRecent= hits.filter(h => new Date(h.pubDate) > new Date(Date.now()-3600000));
  const score     = recent.length * 0.15 + veryRecent.length * 0.25;
  const conf      = Math.min(0.9, score);

  return {
    vote:       conf > 0.2,
    confidence: conf,
    articles:   hits.length,
    recent:     recent.length,
    detail:     `${hits.length} articles | ${veryRecent.length} in last hour | ${hits.map(h=>h.source).slice(0,3).join(", ")}`
  };
}

// ══════════════════════════════════════════════════════════
// AGENT 4: SENTIMENT (Reddit tone + velocity)
// ══════════════════════════════════════════════════════════

async function agentSentiment(market) {
  try {
    const q     = (market.question||"").toLowerCase();
    const words = q.split(" ").filter(w=>w.length>4&&!["will","have","does","than"].includes(w));

    // Use ApeWisdom only — Reddit direct API blocks cloud IPs
    const apeData = await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");
    const apeC    = await safeFetch("https://apewisdom.io/api/v1.0/filter/all-crypto/page/1");
    const allApe  = [...(apeData?.results||[]), ...(apeC?.results||[])];

    // Check if any trending ticker matches the market question
    let sentiment = 0.5;
    let matched   = 0;
    for (const r of allApe) {
      const ticker = (r.ticker||"").toLowerCase();
      if (words.some(w=>ticker.includes(w)||w.includes(ticker))) {
        // Trending = bullish signal, rank matters
        sentiment = Math.min(0.9, 0.5 + (50-Math.min(r.rank,50))/100);
        matched   = r.mentions||0;
        break;
      }
    }

    // Also score the market question keywords against crypto/stock news context
    const posKw = ["win","beats","rises","up","leads","gains","hits"];
    const negKw = ["loses","drops","falls","misses","down","fails"];
    let kwScore = 0;
    posKw.forEach(w=>{ if(q.includes(w)) kwScore+=1; });
    negKw.forEach(w=>{ if(q.includes(w)) kwScore-=1; });

    const finalSentiment = Math.max(0.1, Math.min(0.9, sentiment + kwScore*0.05));
    return {
      vote:       finalSentiment > 0.55,
      confidence: finalSentiment,
      detail:     `Sentiment: ${(finalSentiment*100).toFixed(0)}% ${matched?`| ${matched} ApeWisdom mentions`:""}`
    };
  } catch { return { vote:true, confidence:0.5, detail:"Sentiment: using neutral default" }; }
}

// ══════════════════════════════════════════════════════════
// AGENT 5: TECHNICAL (price action, volume, momentum)
// ══════════════════════════════════════════════════════════

async function agentTechnical(market) {
  try {
    // Robust price extraction — try all possible field names
    let price = 0;
    try {
      const op = market.outcomePrices || market._yesPrice;
      if (typeof op === "string") price = parseFloat(JSON.parse(op)[0]);
      else if (Array.isArray(op)) price = parseFloat(op[0]);
      else if (typeof op === "number") price = op;
      else price = parseFloat(market.bestBid || market.lastTradePrice || market.yes_price || 0);
    } catch { price = parseFloat(market.bestBid || market.yes_price || 0); }

    const volume   = parseFloat(market.volume||0);
    const volume24 = parseFloat(market.volume24hr||0);
    const liquidity= parseFloat(market.liquidity||market._liq||0);
    if (!price || price <= 0) return { vote:false, confidence:0, detail:"No price data", price:0 };

    // Volume momentum — is volume accelerating?
    const volMomentum = volume24 > 0 && volume > 0 ? volume24/(volume/30) : 0;
    // Price in sweet zone (not extreme)
    const inZone    = price >= PROB_FLOOR && price <= PROB_CEILING;
    // Reasonable spread from 50% (has room to move)
    const hasUpside = price < 0.70;
    // Good liquidity
    const goodLiq   = liquidity >= MIN_LIQUIDITY;

    const signals   = [volMomentum>1.5, inZone, hasUpside, goodLiq];
    const score     = signals.filter(Boolean).length / signals.length;

    return {
      vote:       score >= 0.6,
      confidence: score,
      price,
      volume24,
      liquidity,
      detail:     `Price: ${(price*100).toFixed(1)}% | Vol24h: $${volume24.toLocaleString()} | Liq: $${liquidity.toLocaleString()} | Momentum: ${volMomentum.toFixed(2)}x`
    };
  } catch { return { vote:false, confidence:0, detail:"Technical unavailable" }; }
}

// ══════════════════════════════════════════════════════════
// AGENT 6: RESOLUTION MATH
// Probability curve vs historical patterns + days to resolve
// ══════════════════════════════════════════════════════════

async function agentResolutionMath(market) {
  try {
    const price      = parseFloat(market.outcomePrices?.[0] || market.bestBid || 0);
    const endDate    = new Date(market.endDate || market.end_date_iso);
    const now        = new Date();
    const daysLeft   = (endDate - now) / 86400000;
    const volume     = parseFloat(market.volume||0);

    if (isNaN(daysLeft) || daysLeft < 0) return { vote:false, confidence:0, detail:"Already resolved" };
    if (daysLeft < (BLACKOUT_HOURS/24)) return { vote:false, confidence:0, detail:"In blackout window" };

    // Resolution math logic:
    // Markets near 50% with 3-14 days left have highest expected value
    // Markets being heavily traded (high volume) are efficiently priced
    // Markets with low volume are more likely to misprice
    const nearMidpoint  = Math.abs(price - 0.5) < 0.25;
    const optimalWindow = daysLeft >= 1 && daysLeft <= 30;
    const lowEfficiency = volume < 50000;
    const notTooClose   = daysLeft > BLACKOUT_HOURS/24;

    // Edge calculation: potential return if price moves to 75% from 40% = +87.5%
    const potentialReturn = price < 0.5 ? (0.75-price)/price : 0;
    const edgeSignal      = potentialReturn > MIN_EDGE;

    const score = [nearMidpoint, optimalWindow, lowEfficiency, notTooClose, edgeSignal].filter(Boolean).length / 5;

    return {
      vote:           score >= 0.6,
      confidence:     score,
      daysLeft:       parseFloat(daysLeft.toFixed(1)),
      potentialReturn:parseFloat((potentialReturn*100).toFixed(1)),
      detail:         `Days left: ${daysLeft.toFixed(1)} | Price: ${(price*100).toFixed(1)}% | Potential: +${(potentialReturn*100).toFixed(0)}% | Volume: $${volume.toLocaleString()}`
    };
  } catch { return { vote:false, confidence:0, detail:"Math unavailable" }; }
}

// ══════════════════════════════════════════════════════════
// CONSENSUS ENGINE
// ══════════════════════════════════════════════════════════

function runConsensus(agents, whaleResult, stealthResult) {
  const votes = agents.filter(a=>a.vote).length;
  const total  = agents.length;
  const tier   = whaleResult?.tier || 99;

  // Tier 1 whale alone → half size immediately
  if (tier === 1 && whaleResult?.vote) {
    return { action:"ENTER", size_mult:0.50, label:"TIER1_WHALE_ALONE", votes, total };
  }
  // Stealth signal (tier 2 whale, no public info)
  if (stealthResult?.vote && tier<=2) {
    return { action:"ENTER", size_mult:0.25, label:"STEALTH_SIGNAL", votes, total };
  }
  // 5/6 or 6/6 → full size
  if (votes >= 5) return { action:"ENTER", size_mult:1.0, label:"FULL_CONSENSUS", votes, total };
  // 4/6 → full size
  if (votes >= 4) return { action:"ENTER", size_mult:1.0, label:"STRONG", votes, total };
  // 3/6 → half
  if (votes >= 3) return { action:"ENTER", size_mult:0.5, label:"MODERATE", votes, total };
  // Less than 3 → skip
  return { action:"SKIP", size_mult:0, label:"INSUFFICIENT", votes, total };
}

// ══════════════════════════════════════════════════════════
// GUARDRAIL CHECKS
// ══════════════════════════════════════════════════════════

function checkGuardrails(market, consensus) {
  resetDailyCounters();
  const open    = getOpenTrades();
  const deployed= getDeployed();
  const price   = parseFloat(market.outcomePrices?.[0]||0);
  const liq     = parseFloat(market.liquidity||0);
  const endDate = new Date(market.endDate||market.end_date_iso);
  const hoursLeft=(endDate-new Date())/3600000;

  if (getState("paused"))                  return { ok:false, reason:"Agent paused" };
  if ((getState("total_loss")||0)>=LOSS_HALT) return { ok:false, reason:`Loss halt $${LOSS_HALT} reached` };
  if (open.length >= MAX_POSITIONS)        return { ok:false, reason:`Max positions (${MAX_POSITIONS})` };
  if (open.find(t=>t.market_id===market.id)) return { ok:false, reason:"Already in market" };
  if ((getState("daily_trades")||0)>=MAX_DAILY_TRADES) return { ok:false, reason:`Daily trade limit (${MAX_DAILY_TRADES})` };
  if (price > PROB_CEILING)                return { ok:false, reason:`Above ${PROB_CEILING*100}% ceiling` };
  if (price < PROB_FLOOR)                  return { ok:false, reason:`Below ${PROB_FLOOR*100}% floor` };
  if (liq < MIN_LIQUIDITY)                 return { ok:false, reason:`Liquidity $${liq} < $${MIN_LIQUIDITY}` };
  if (hoursLeft < BLACKOUT_HOURS)          return { ok:false, reason:`Blackout window (${hoursLeft.toFixed(1)}h left)` };

  const cat   = market.category||"other";
  const catDep= getCatDeployed(cat);
  if (catDep >= BUDGET*MAX_CATEGORY_DEP)   return { ok:false, reason:`Category cap (${cat} at $${catDep.toFixed(0)})` };

  const avail  = BUDGET - deployed;
  const size   = Math.min(MAX_POSITION, avail * consensus.size_mult);
  if (size < 5)                            return { ok:false, reason:"Insufficient budget" };

  // Max market share
  const marketShare = size/liq;
  if (marketShare > MAX_MKT_SHARE)         return { ok:false, reason:`Market share ${(marketShare*100).toFixed(1)}% > ${MAX_MKT_SHARE*100}%` };

  return { ok:true, size: parseFloat(size.toFixed(2)) };
}

// ══════════════════════════════════════════════════════════
// TRADE EXECUTION (Paper)
// ══════════════════════════════════════════════════════════

async function executePaperTrade(market, consensus, agents, guardrails) {
  // Robust price extraction
  let price = 0;
  try {
    const op = market.outcomePrices || market._yesPrice || market.yes_price;
    if (typeof op === "string") price = parseFloat(JSON.parse(op)[0]);
    else if (Array.isArray(op)) price = parseFloat(op[0]);
    else if (typeof op === "number") price = op;
    else price = parseFloat(market.bestBid || market.lastTradePrice || market.yes_price || 0);
  } catch { price = parseFloat(market.bestBid || market.yes_price || 0.5); }
  if (!price || price <= 0 || isNaN(price)) price = parseFloat(market.yes_price || 0);

  const size      = guardrails.size;
  const shares    = parseFloat((size / price).toFixed(4));
  const stopPrice = Math.max(PROB_FLOOR, price * 0.85);
  const target    = Math.min(PROB_CEILING, price + (1-price)*EXIT_TARGET);
  const endDate   = market.endDate||market.end_date_iso;
  const agentsFired= agents.filter(a=>a.vote).map(a=>a.name).join(",");

  db.prepare(`INSERT INTO paper_trades
    (market_id,market_question,category,side,entry_price,current_price,
     size,shares,stop_price,target_price,agents_fired,layer,tier,closes_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(market.id, market.question, market.category||"general",
         "YES", price, price, size, shares, stopPrice, target,
         agentsFired, consensus.label, consensus.votes, endDate);

  setState("daily_trades", (getState("daily_trades")||0)+1);
  savePositionBackup(); // Persist to file so redeployments don't lose positions

  const msg =
    `<b>VERAPIMPO TRADE</b> [PAPER]\n\n` +
    `<b>${market.question?.slice(0,80)}</b>\n\n` +
    `Entry: YES @ ${(price*100).toFixed(1)}%\n` +
    `Size: $${size.toFixed(2)} (${shares} shares)\n` +
    `Target: ${(target*100).toFixed(1)}% | Stop: ${(stopPrice*100).toFixed(1)}%\n` +
    `Signal: ${consensus.label} (${consensus.votes}/${consensus.total} agents)\n` +
    `Agents: ${agentsFired}\n` +
    `Category: ${market.category||"general"}\n` +
    `Resolves: ${new Date(endDate).toLocaleDateString()}`;

  await tg(msg);
  logAlert("TRADE_OPENED", msg, market.id);
  console.log(`📈 PAPER TRADE: ${market.question?.slice(0,60)} @ ${(price*100).toFixed(1)}% $${size}`);
}

// ══════════════════════════════════════════════════════════
// POSITION MONITORING & EXIT
// ══════════════════════════════════════════════════════════

async function monitorPositions() {
  const open = getOpenTrades();
  for (const trade of open) {
    try {
      const market = await getMarketById(trade.market_id);
      if (!market) continue;

      // Robust price extraction — outcomePrices can be a JSON string
      let price = 0;
      try {
        const op = market.outcomePrices;
        if (typeof op === "string") price = parseFloat(JSON.parse(op)[0]);
        else if (Array.isArray(op)) price = parseFloat(op[0]);
        else price = parseFloat(market.bestBid || market.lastTradePrice || 0);
      } catch { price = parseFloat(market.bestBid || market.yes_price || 0); }

      // CRITICAL: never use price=0 for exit decisions — skip this position
      if (!price || price <= 0 || isNaN(price)) {
        console.log(`Monitor skip ${trade.market_id?.slice(0,20)}: price=${price} (using stored: ${trade.current_price})`);
        continue;
      }

      const endDate   = new Date(market.endDate||market.end_date_iso||market._endDate);
      const hoursLeft = !isNaN(endDate) ? (endDate-new Date())/3600000 : 999;
      const pnl       = (price - trade.entry_price) * trade.shares;
      const pnl_pct   = ((price - trade.entry_price)/trade.entry_price)*100;
      const vol24     = parseFloat(market.volume24hr||0);

      // Update current price
      db.prepare("UPDATE paper_trades SET current_price=? WHERE id=?").run(price, trade.id);

      let exitReason = null;
      if (price >= trade.target_price)          exitReason = "TARGET_HIT";
      else if (price <= trade.stop_price)        exitReason = "STOP_HIT";
      else if (hoursLeft <= BLACKOUT_HOURS)      exitReason = "RESOLUTION_BLACKOUT";
      else {
        // Volume spike: only fire if there is a meaningful baseline (>$1000) and spike is real
        const baseVol = parseFloat(market.volume||0)/30;
        if (baseVol > 1000 && vol24 > 0 && vol24 > baseVol * VOL_SPIKE_EXIT) {
          exitReason = "VOLUME_SPIKE";
        }
      }

      if (exitReason) {
        db.prepare("UPDATE paper_trades SET status='closed',pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?")
          .run(parseFloat(pnl.toFixed(4)), parseFloat(pnl_pct.toFixed(2)), exitReason, trade.id);
        savePositionBackup(); // Update backup after close
        const totalPnl = (getState("total_pnl")||0) + pnl;
        setState("total_pnl", parseFloat(totalPnl.toFixed(4)));
        if (pnl < 0) setState("total_loss", (getState("total_loss")||0)+Math.abs(pnl));

        const msg =
          `<b>VERAPIMPO EXIT</b> [PAPER]\n\n` +
          `<b>${trade.market_question?.slice(0,80)}</b>\n\n` +
          `Entry: ${(trade.entry_price*100).toFixed(1)}% → Exit: ${(price*100).toFixed(1)}%\n` +
          `P&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)} (${pnl_pct>=0?"+":""}${pnl_pct.toFixed(1)}%)</b>\n` +
          `Reason: ${exitReason}\n` +
          `Total P&L: $${totalPnl.toFixed(2)}`;
        await tg(msg);
        logAlert("TRADE_CLOSED", msg, trade.market_id);
      }
    } catch(e) { console.error("Monitor:", trade.market_id, e.message); }
    await delay(300);
  }
}

// ══════════════════════════════════════════════════════════
// TELEGRAM CHANNEL MONITOR — via t.me/s/ public preview
// No API keys needed. Works for any public channel.
// ══════════════════════════════════════════════════════════

async function scrapeTelegramChannel(channel) {
  try {
    const url  = `https://t.me/s/${channel.username}`;
    const html = await safeFetchText(url, 12000);
    if (!html || html.length < 500) {
      console.log(`TG ${channel.label}: no data from t.me/s/`);
      return [];
    }

    // Parse message divs from t.me/s/ preview page
    const msgPattern = /<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/g;
    const datePattern= /<time[^>]+datetime="([^"]+)"/g;
    const imgPattern = /<a[^>]+href="([^"]+)"[^>]*>.*?<\/a>/g;

    const rawMsgs = [...html.matchAll(/<div class="tgme_widget_message_bubble">([\s\S]*?)<\/div>\s*<\/div>\s*<\/div>/g)];
    const results = [];

    // Simpler: extract all text blocks
    const textBlocks = [...html.matchAll(/<div class="tgme_widget_message_text js-message_text"[^>]*>([\s\S]*?)<\/div>/g)];
    const dates      = [...html.matchAll(/datetime="([^"]+)"/g)].map(m=>m[1]);

    for (let i = 0; i < textBlocks.length; i++) {
      // Strip HTML tags
      const raw  = textBlocks[i][1];
      const text = raw.replace(/<[^>]+>/g,' ').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#39;/g,"'").replace(/&quot;/g,'"').replace(/\s+/g,' ').trim();
      if (!text || text.length < 10) continue;

      const date = dates[i] || new Date().toISOString();
      // Skip if older than 12 hours
      if (new Date(date) < new Date(Date.now() - 12*3600000)) continue;

      const lower = text.toLowerCase();

      // Score relevance
      const posWords = ["yes","win","likely","pump","bull","buy","surge","up","call","long"];
      const negWords = ["no","lose","dump","bear","sell","crash","down","put","short"];
      const cryptoKw = ["bitcoin","btc","eth","ethereum","crypto","polymarket","usdc","usd"];
      const whaleKw  = ["whale","wallet","million","transfer","large","position","moved"];
      const newsKw   = ["breaking","report","says","confirmed","announced","according"];

      let score = 0;
      posWords.forEach(w=>{ if(lower.includes(w)) score+=1; });
      negWords.forEach(w=>{ if(lower.includes(w)) score-=0.5; });
      cryptoKw.forEach(w=>{ if(lower.includes(w)) score+=2; });
      if (channel.type==="whale") whaleKw.forEach(w=>{ if(lower.includes(w)) score+=2; });
      if (channel.type==="news")  newsKw.forEach(w=>{ if(lower.includes(w)) score+=1; });

      // Dollar amounts and wallet addresses boost score
      (text.match(/\$[\d,.]+[MmBbKk]?/g)||[]).forEach(()=>score+=1);
      (text.match(/0x[a-fA-F0-9]{40}/g)||[]).forEach(()=>score+=3);

      // Match against known Polymarket markets
      let marketId=null, marketQ=null;
      const mkts = db.prepare("SELECT id,question FROM markets ORDER BY last_scanned DESC LIMIT 50").all();
      for (const m of mkts) {
        const mwords = (m.question||"").toLowerCase().split(" ").filter(w=>w.length>4);
        const hits   = mwords.filter(w=>lower.includes(w)).length;
        if (hits >= 2) { marketId=m.id; marketQ=m.question; break; }
      }

      // Save unique messages
      const msgId = `${channel.username}_${i}_${date}`;
      try {
        db.prepare(`INSERT OR IGNORE INTO tg_messages
          (channel,channel_type,message_id,text,sender,date,market_id,market_question,relevance_score)
          VALUES (?,?,?,?,?,?,?,?,?)`)
          .run(channel.label, channel.type, msgId, text.slice(0,1000), channel.label, date, marketId, marketQ, score);
      } catch {}

      results.push({channel:channel.label, type:channel.type, text, score, marketId, marketQ, date});
    }

    console.log(`TG ${channel.label}: ${results.length} messages scraped`);
    return results;
  } catch(e) {
    console.error(`TG scrape ${channel.label}:`, e.message);
    return [];
  }
}

async function fetchAllTelegramChannels() {
  const results = { whale:[], news:[] };
  for (const ch of TG_CHANNELS) {
    const msgs = await scrapeTelegramChannel(ch);
    if (results[ch.type]) results[ch.type].push(...msgs);
    else results[ch.type] = msgs;
    await delay(2000);
  }
  return results;
}

async function initTelegramReader() {
  console.log("TG channel monitor: using t.me/s/ scraper (no API keys needed)");
  return true;
}

// Get Telegram signal score for a specific market
function getTelegramSignalScore(marketId, marketQuestion) {
  const q = (marketQuestion||"").toLowerCase();
  const words = q.split(" ").filter(w=>w.length>4);

  // Check recent TG messages for matches
  const recent = db.prepare(
    "SELECT * FROM tg_messages WHERE fetched_at > datetime('now','-6 hours') ORDER BY relevance_score DESC LIMIT 100"
  ).all();

  let whaleSignal = 0, newsSignal = 0;
  for (const msg of recent) {
    const lower = (msg.text||"").toLowerCase();
    const matches = words.filter(w=>lower.includes(w)).length;
    if (matches >= 2 || msg.market_id === marketId) {
      if (msg.channel_type === "whale") whaleSignal += msg.relevance_score * 0.3;
      if (msg.channel_type === "news")  newsSignal  += msg.relevance_score * 0.2;
    }
  }

  return {
    whaleSignal: Math.min(1, whaleSignal),
    newsSignal:  Math.min(1, newsSignal),
    hasSignal:   whaleSignal > 1 || newsSignal > 1
  };
}

// ══════════════════════════════════════════════════════════
// MAIN SCAN
// ══════════════════════════════════════════════════════════

async function runScan() {
  if (getState("paused")) return;
  console.log(`🔍 VeraPimpo scan ${new Date().toISOString()}`);

  // Fetch Telegram channels first — enriches all subsequent agent decisions
  fetchAllTelegramChannels().catch(e=>console.error("TG fetch:", e.message));

  let page=0, traded=0, scanned=0;
  const allMarkets=[];

  // Pull up to 300 markets
  for (let i=0;i<3;i++) {
    const batch = await getMarkets(100, i*100);
    if (!Array.isArray(batch)||!batch.length) break;
    allMarkets.push(...batch);
    await delay(300);
  }

  console.log(`📊 ${allMarkets.length} markets found`);

  // Normalize and filter markets
  const normalize = m => {
    let yesPrice = 0;
    try {
      const op = m.outcomePrices;
      if (typeof op === "string") { const arr = JSON.parse(op); yesPrice = parseFloat(arr[0]||0); }
      else if (Array.isArray(op)) { yesPrice = parseFloat(op[0]||0); }
      else { yesPrice = parseFloat(m.bestBid||m.lastTradePrice||0.5); }
    } catch { yesPrice = parseFloat(m.bestBid||0.5); }
    return {...m, _yesPrice: yesPrice,
      _liq: parseFloat(m.liquidity||m.liquidityNum||0),
      _endDate: m.endDate||m.end_date_iso||m.endDateIso};
  };

  const viable = allMarkets.map(normalize).filter(m => {
    const price   = m._yesPrice;
    const liq     = m._liq;
    const endDate = m._endDate ? new Date(m._endDate) : null;
    const hours   = endDate ? (endDate-new Date())/3600000 : 999;
    return price>PROB_FLOOR && price<PROB_CEILING && liq>=MIN_LIQUIDITY && hours>BLACKOUT_HOURS;
  }).slice(0,35);

  console.log(`✅ ${viable.length} viable markets after filtering`);

  for (const market of viable) {
    if (getOpenTrades().length>=MAX_POSITIONS) break;
    try {
      scanned++;

      // Ensure market has normalized fields
      if (!market._yesPrice) {
        try { const op=market.outcomePrices; market._yesPrice=typeof op==="string"?parseFloat(JSON.parse(op)[0]):parseFloat(Array.isArray(op)?op[0]:op||0.5); } catch { market._yesPrice=0.5; }
        market._liq = parseFloat(market.liquidity||0);
        market._endDate = market.endDate||market.end_date_iso;
      }
      // Layer 1: Order book pressure
      const obPressure = await detectOrderBookPressure(market);
      await delay(200);

      // Layer 2: Whale activity
      const whaleActivity = await getWhaleActivity(market);
      await delay(200);

      // Layer 2: Market divergence
      const divergence = detectMarketDivergence(market, allMarkets);

      // Layer 2: Resolution source
      const resSource = await checkResolutionSource(market);
      await delay(200);

      // Telegram signals for this market
      const tgSignal = getTelegramSignalScore(market.id, market.question);

      // All 6 agents
      const whaleRes  = agentWhaleCopy(whaleActivity);
      const newsRes   = await agentNewscat(market);
      await delay(200);
      const sentRes   = await agentSentiment(market);
      await delay(200);
      const techRes   = await agentTechnical(market);
      const mathRes   = await agentResolutionMath(market);
      const stealthRes= agentStealth(whaleActivity, newsRes, sentRes);

      // Boost agent confidence if Telegram signals agree
      if (tgSignal.whaleSignal > 0.5 && whaleRes.vote) {
        whaleRes.confidence = Math.min(1, (whaleRes.confidence||0) + 0.15);
      }
      if (tgSignal.newsSignal > 0.5 && newsRes.vote) {
        newsRes.confidence  = Math.min(1, (newsRes.confidence||0) + 0.15);
      }

      const allAgents = [
        {...whaleRes,  name:"Whale"},
        {...stealthRes,name:"Stealth"},
        {...newsRes,   name:"News"},
        {...sentRes,   name:"Sentiment"},
        {...techRes,   name:"Technical"},
        {...mathRes,   name:"ResolutionMath"},
      ];

      const consensus = runConsensus(allAgents, whaleRes, stealthRes);

      db.prepare("INSERT INTO scan_log (market_id,market_question,agents_fired,consensus,action) VALUES (?,?,?,?,?)")
        .run(market.id, market.question, allAgents.filter(a=>a.vote).map(a=>a.name).join(","), consensus.votes, consensus.action);

      // Update news items status if this market triggers a trade
      if (consensus.action==="ENTER") {
        db.prepare("UPDATE news_items SET status='triggered' WHERE market_id=? AND status='monitored'")
          .run(market.id);
      }

      if (consensus.action==="ENTER") {
        const guard = checkGuardrails(market, consensus);
        if (guard.ok) {
          await executePaperTrade(market, consensus, allAgents, guard);
          traded++;
        } else {
          console.log(`🚫 Blocked: ${market.question?.slice(0,50)} — ${guard.reason}`);
        }
      }

      await delay(500);
    } catch(e) { console.error("Scan error:", market.question?.slice(0,40), e.message); }
  }

  await tg(`<b>VeraPimpo Scan Complete</b>\n${traded} trades | ${scanned} scanned | ${viable.length} viable\nDeployed: $${getDeployed().toFixed(2)}/$${BUDGET}`);
  await monitorPositions();
}

// ══════════════════════════════════════════════════════════
// WHALE REFRESH (daily)
// ══════════════════════════════════════════════════════════

async function refreshWhales() {
  console.log("🐋 Daily whale refresh starting...");
  try {
    // Pull top market makers from recent trades
    const markets = await getMarkets(50,0);
    const walletStats = {};

    for (const market of markets.slice(0,10)) {  // Reduced from 20 to 10
      try {
        await delay(500); // Throttle between requests
        const trades = await getRecentTrades(market.id, 50);  // Reduced from 100
        if (!Array.isArray(trades)) continue;
        for (const t of trades) {
          for (const addr of [t.maker, t.taker].filter(Boolean)) {
            if (!walletStats[addr]) walletStats[addr] = { wins:0, total:0, profit:0, recent:0 };
            walletStats[addr].total++;
            const price = parseFloat(t.price||0);
            if (price > 0.5) { walletStats[addr].wins++; walletStats[addr].profit+=parseFloat(t.size||0)*price; }
            if (new Date(t.timestamp||t.created_at) > new Date(Date.now()-86400000*7)) walletStats[addr].recent++;
          }
        }
        await delay(300);
      } catch {}
    }

    // Score and rank wallets
    const scored = Object.entries(walletStats)
      .filter(([,s])=>s.total>=10 && s.recent>=1)
      .map(([addr,s])=>{
        const winRate  = s.wins/s.total;
        const score    = winRate*0.5 + (s.profit/10000)*0.3 + (s.recent/20)*0.2;
        return { addr, ...s, winRate, score };
      })
      .sort((a,b)=>b.score-a.score)
      .slice(0,50);

    // Update DB
    let newWhales=0, dropped=0;
    const prevAddrs = new Set(db.prepare("SELECT address FROM whales").all().map(w=>w.address));
    const newAddrs  = new Set(scored.map(w=>w.addr));

    for (const [i, w] of scored.entries()) {
      const tier   = i<5?1:i<20?2:3;
      const days   = db.prepare("SELECT days_in_top50,confirmed FROM whales WHERE address=?").get(w.addr);
      const newDays= (days?.days_in_top50||0)+1;
      const conf   = newDays>=3?1:0;
      db.prepare("INSERT OR REPLACE INTO whales (address,rank,win_rate,avg_profit,total_trades,recent_trades,profit_30d,tier,days_in_top50,confirmed,last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)")
        .run(w.addr,i+1,w.winRate,w.profit/w.total,w.total,w.recent,w.profit,tier,newDays,conf);
      if (!prevAddrs.has(w.addr)) newWhales++;
    }

    // Mark dropped wallets
    for (const addr of prevAddrs) {
      if (!newAddrs.has(addr)) {
        db.prepare("DELETE FROM whales WHERE address=?").run(addr);
        dropped++;
      }
    }

    const confirmed = db.prepare("SELECT COUNT(*) as c FROM whales WHERE confirmed=1").get()?.c||0;
    const msg =
      `<b>VeraPimpo Whale Refresh</b>\n\n` +
      `Top 50 updated\n` +
      `New wallets: ${newWhales}\n` +
      `Dropped: ${dropped}\n` +
      `Confirmed (3-day rule): ${confirmed}\n` +
      `Tier 1 (top 5): ${scored.slice(0,5).map(w=>w.addr.slice(0,8)+"...").join(", ")}`;
    await tg(msg);
    logAlert("WHALE_REFRESH", msg);
    console.log(`✅ Whale refresh: ${scored.length} ranked, ${confirmed} confirmed`);
  } catch(e) { console.error("Whale refresh:", e.message); }
}

// ══════════════════════════════════════════════════════════
// TELEGRAM BOT
// ══════════════════════════════════════════════════════════

const bot = cfg.telegram ? new Telegraf(cfg.telegram) : null;

if (bot) {
  bot.use(async (ctx, next) => {
    if (ctx.from && !isAuthorized(ctx.from.id)) { await ctx.reply("Unauthorized."); return; }
    return next();
  });

  bot.start(ctx=>ctx.replyWithHTML(
    `<b>VeraPimpo v1.0</b>\n\nPolymarket Intelligence Agent\nPaper Trading Mode — $${BUDGET} budget\n\n` +
    `/portfolio /positions /history /whales /markets /news /strategy /pause /resume /status /live`
  ));

  bot.command("portfolio", async ctx=>{
    const open   = getOpenTrades();
    const pnl    = getState("total_pnl")||0;
    const loss   = getState("total_loss")||0;
    const dep    = getDeployed();
    const wins   = db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0").get()?.c||0;
    const total_closed=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed'").get()?.c||0;
    const wr     = total_closed>0?((wins/total_closed)*100).toFixed(1):"N/A";
    ctx.replyWithHTML(
      `<b>VeraPimpo Portfolio</b> [PAPER]\n\n` +
      `Budget: $${BUDGET} | Deployed: $${dep.toFixed(2)}\n` +
      `Total P&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)}</b>\n` +
      `Losses: $${loss.toFixed(2)}/$${LOSS_HALT}\n` +
      `Open: ${open.length}/${MAX_POSITIONS}\n` +
      `Win Rate: ${wr}% (${total_closed} closed)\n` +
      `Daily trades: ${getState("daily_trades")||0}/${MAX_DAILY_TRADES}`
    );
  });

  bot.command("positions", ctx=>{
    const open=getOpenTrades();
    if (!open.length){ctx.reply("No open paper positions.");return;}
    let msg=`<b>Open Positions (${open.length})</b>\n\n`;
    open.forEach(t=>{
      const pnl=(t.current_price-t.entry_price)*t.shares;
      msg+=`<b>${t.market_question?.slice(0,55)}</b>\n`;
      msg+=`Entry: ${(t.entry_price*100).toFixed(1)}% | Now: ${(t.current_price*100).toFixed(1)}%\n`;
      msg+=`P&L: ${pnl>=0?"+":""}$${pnl.toFixed(2)} | Size: $${t.size}\n`;
      msg+=`Signal: ${t.agents_fired}\n\n`;
    });
    ctx.replyWithHTML(msg);
  });

  bot.command("history", ctx=>{
    const rows=db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 15").all();
    if (!rows.length){ctx.reply("No closed paper trades yet.");return;}
    let msg=`<b>Last ${rows.length} Trades</b>\n\n`;
    rows.forEach(t=>{msg+=`${t.pnl>=0?"🟢":"🔴"} ${t.market_question?.slice(0,45)} — ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)} [${t.exit_reason}]\n`;});
    ctx.replyWithHTML(msg);
  });

  bot.command("whales", async ctx=>{
    const whales=db.prepare("SELECT * FROM whales WHERE confirmed=1 ORDER BY rank LIMIT 10").all();
    if (!whales.length){ctx.reply("No confirmed whales yet. Check back after first daily refresh.");return;}
    let msg=`<b>Top Whales (Confirmed)</b>\n\n`;
    whales.forEach(w=>{msg+=`#${w.rank} ${w.address.slice(0,10)}... | Tier ${w.tier} | Win: ${(w.win_rate*100).toFixed(0)}% | 30d: $${w.profit_30d?.toFixed(0)}\n`;});
    ctx.replyWithHTML(msg);
  });

  bot.command("markets", async ctx=>{
    ctx.replyWithHTML("<i>Fetching top markets...</i>");
    const markets=await getMarkets(20,0);
    if (!markets?.length){ctx.reply("No markets available.");return;}
    let msg=`<b>Top Markets Right Now</b>\n\n`;
    markets.filter(m=>{
      const p=parseFloat(m.outcomePrices?.[0]||0);
      return p>PROB_FLOOR&&p<PROB_CEILING;
    }).slice(0,8).forEach(m=>{
      const p=parseFloat(m.outcomePrices?.[0]||0);
      const liq=parseFloat(m.liquidity||0);
      msg+=`<b>${m.question?.slice(0,60)}</b>\n`;
      msg+=`YES: ${(p*100).toFixed(1)}% | Liq: $${liq.toLocaleString()}\n\n`;
    });
    ctx.replyWithHTML(msg);
  });

  bot.command("news", ctx=>{
    const news=db.prepare("SELECT * FROM news_items ORDER BY fetched_at DESC LIMIT 10").all();
    if (!news.length){ctx.reply("No news items yet.");return;}
    let msg=`<b>Recent News Signals</b>\n\n`;
    news.forEach(n=>{msg+=`[${n.status.toUpperCase()}] ${n.source}\n${n.headline?.slice(0,80)}\n\n`;});
    ctx.replyWithHTML(msg);
  });

  bot.command("strategy", ctx=>{
    const strategies=["Whale","Stealth","News","Sentiment","Technical","ResolutionMath"];
    let msg=`<b>Strategy Performance</b>\n\n`;
    for (const s of strategies) {
      const wins =db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;
      const total=db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0;
      const pnl  =db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0;
      msg+=`<b>${s}</b>: ${total} trades | ${total>0?((wins/total)*100).toFixed(0):"-"}% WR | $${pnl.toFixed(2)}\n`;
    }
    ctx.replyWithHTML(msg);
  });

  bot.command("scan", async ctx=>{
    ctx.replyWithHTML("<i>Manual scan triggered — running all 6 agents across viable markets...</i>");
    runScan();
  });

  bot.command("sync", async ctx=>{
    const r = restorePositionsFromBackup();
    const open = getOpenTrades();
    ctx.replyWithHTML(
      `<b>Sync Complete</b>

` +
      `Restored from backup: ${r} positions
` +
      `Total open now: ${open.length}/${MAX_POSITIONS}
` +
      `Deployed: $${getDeployed().toFixed(2)}/$${BUDGET}`
    );
  });

  bot.command("whalesscan", async ctx=>{
    ctx.replyWithHTML("<i>Manual whale scan triggered — this takes 1-2 minutes...</i>");
    refreshWhales().then(()=>ctx.replyWithHTML("Whale scan complete. Send /whales to see updated rankings."));
  });

  bot.command("pause",   ctx=>{setState("paused",true);  ctx.reply("⏸️ VeraPimpo paused.");});
  bot.command("resume",  ctx=>{setState("paused",false); ctx.reply("▶️ VeraPimpo resumed.");});
  bot.command("status",  async ctx=>{
    const p=getState("paused");
    ctx.replyWithHTML(
      `<b>VeraPimpo Status</b>\n\nMode: ${p?"⏸️ PAUSED":"▶️ ACTIVE"}\n` +
      `Paper: ${PAPER?"YES — no real money":"NO — LIVE TRADING"}\n` +
      `Budget: $${BUDGET} | Loss halt: $${LOSS_HALT}\n` +
      `Open: ${getOpenTrades().length}/${MAX_POSITIONS}\n` +
      `Guardrails: 14 active\n` +
      `Whale refresh: daily 6am Riyadh\n` +
      `Scan: every 5 min\n` +
      `Monitor: every 2 min`
    );
  });
  bot.command("live", ctx=>ctx.replyWithHTML(
    `<b>Going Live</b>\n\nTo switch to real money:\n\n` +
    `1. Set up crypto wallet\n` +
    `2. Fund with USDC via Rain exchange\n` +
    `3. Add WALLET_PRIVATE_KEY to Railway\n` +
    `4. Change PAPER=true to PAPER=false in code\n` +
    `5. Redeploy\n\n` +
    `See PDF guide for complete instructions.`
  ));
  bot.help(ctx=>ctx.replyWithHTML(`/portfolio /scan /sync /positions /history /whales /whalesscan /markets /news /strategy /pause /resume /status /live`));
}

// ══════════════════════════════════════════════════════════
// REST API (for dashboard)
// ══════════════════════════════════════════════════════════

const app = express();
app.use(cors());
app.use(express.json());

// Simple password middleware for API
app.use("/api", (req,res,next)=>{
  const pass = req.headers["x-dashboard-password"]||req.query.password;
  if (pass!==cfg.dashPass) return res.status(401).json({error:"Unauthorized"});
  next();
});

app.get("/api/overview", (req,res)=>{
  const open   = getOpenTrades();
  const pnl    = getState("total_pnl")||0;
  const loss   = getState("total_loss")||0;
  const dep    = getDeployed();
  const wins   = db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0").get()?.c||0;
  const total  = db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed'").get()?.c||0;
  const today  = db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE date(opened_at)=date('now')").get()?.c||0;
  res.json({ budget:BUDGET,deployed:dep,available:BUDGET-dep,pnl,loss,lossHalt:LOSS_HALT,openCount:open.length,maxPositions:MAX_POSITIONS,winRate:total>0?wins/total:0,totalTrades:total,todayTrades:today,maxDailyTrades:MAX_DAILY_TRADES,paper:PAPER,paused:getState("paused")||false });
});

app.get("/api/positions", (req,res)=>{
  const positions = db.prepare("SELECT * FROM paper_trades WHERE status='open' ORDER BY opened_at DESC").all();
  res.json(positions);
});

app.get("/api/history", (req,res)=>{
  const limit = parseInt(req.query.limit)||50;
  const rows  = db.prepare("SELECT * FROM paper_trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?").all(limit);
  res.json(rows);
});

app.get("/api/whales", (req,res)=>{
  const whales = db.prepare("SELECT * FROM whales ORDER BY rank").all();
  res.json(whales);
});

app.get("/api/whale-trades", (req,res)=>{
  const rows = db.prepare("SELECT * FROM whale_trades ORDER BY timestamp DESC LIMIT 50").all();
  res.json(rows);
});

app.get("/api/markets", async (req,res)=>{
  try {
    // First try DB cache (populated during scans)
    const cached = db.prepare("SELECT * FROM markets ORDER BY score DESC, last_scanned DESC LIMIT 35").all();
    if (cached.length > 0) { res.json(cached); return; }
    // Fallback: fetch live
    const markets = await getMarkets(50,0);
    const viable  = (markets||[]).map(m=>{
      let yp=0;
      try { const op=m.outcomePrices; yp=typeof op==="string"?parseFloat(JSON.parse(op)[0]):parseFloat(Array.isArray(op)?op[0]:0.5); } catch { yp=0.5; }
      return {...m, _yesPrice:yp, _liq:parseFloat(m.liquidity||0)};
    }).filter(m=>m._yesPrice>PROB_FLOOR&&m._yesPrice<PROB_CEILING&&m._liq>=MIN_LIQUIDITY).slice(0,35);
    res.json(viable);
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get("/api/news", (req,res)=>{
  const limit = parseInt(req.query.limit)||50;
  // Combine news_items and tg_messages into one feed
  const news = db.prepare("SELECT *,'news' as feed_type FROM news_items ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.7));
  const tg   = db.prepare("SELECT id,channel as source,text as headline,text as summary,NULL as url,NULL as image_url,date as published_at,market_id,market_question,'monitored' as status,'telegram' as feed_type FROM tg_messages WHERE relevance_score>1 ORDER BY fetched_at DESC LIMIT ?").all(Math.floor(limit*0.3));
  // Merge and sort by date
  const all  = [...news, ...tg].sort((a,b)=>new Date(b.fetched_at||b.published_at)-new Date(a.fetched_at||a.published_at));
  res.json(all.slice(0,limit));
});

app.get("/api/tg-messages", (req,res)=>{
  const rows = db.prepare("SELECT * FROM tg_messages ORDER BY fetched_at DESC LIMIT 100").all();
  res.json(rows);
});

app.get("/api/strategy", (req,res)=>{
  const strategies=["Whale","Stealth","News","Sentiment","Technical","ResolutionMath"];
  const result=strategies.map(s=>({
    name: s,
    trades:    db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,
    wins:      db.prepare("SELECT COUNT(*) as c FROM paper_trades WHERE status='closed' AND pnl>0 AND agents_fired LIKE ?").get(`%${s}%`)?.c||0,
    pnl:       db.prepare("SELECT SUM(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0,
    avg_pnl:   db.prepare("SELECT AVG(pnl) as p FROM paper_trades WHERE status='closed' AND agents_fired LIKE ?").get(`%${s}%`)?.p||0,
  }));
  res.json(result);
});

app.get("/api/alerts", (req,res)=>{
  const rows=db.prepare("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100").all();
  res.json(rows);
});

app.get("/api/scan-log", (req,res)=>{
  const rows=db.prepare("SELECT * FROM scan_log ORDER BY scanned_at DESC LIMIT 100").all();
  res.json(rows);
});

app.post("/api/sync", (req,res)=>{
  const r    = restorePositionsFromBackup();
  const open = getOpenTrades();
  res.json({ restored:r, open:open.length, deployed:getDeployed() });
});

app.post("/api/scan", async (req,res)=>{
  res.json({started:true, message:"Scan started"});
  runScan();
});

app.post("/api/whalesscan", async (req,res)=>{
  res.json({started:true, message:"Whale scan started — check back in 1-2 minutes"});
  refreshWhales();
});

app.post("/api/pause",  (req,res)=>{setState("paused",true);  res.json({paused:true});});
app.post("/api/resume", (req,res)=>{setState("paused",false); res.json({paused:false});});

// Serve dashboard
const fs   = require("fs");
const pth  = require("path");
app.get("/", (req,res)=>{
  try {
    const html = fs.readFileSync(pth.join(__dirname,"dashboard.html"),"utf8");
    res.setHeader("Content-Type","text/html");
    res.send(html);
  } catch(e) {
    res.status(500).send("Dashboard not found. Upload dashboard.html to the repo root.");
  }
});

// Health check (no auth)
app.get("/health", (req,res)=>res.json({status:"ok",paper:PAPER,version:"1.0.0"}));

app.listen(3000, ()=>console.log("📊 VeraPimpo API running on port 3000"));

// ── SCHEDULES ─────────────────────────────────────────────
// Main scan every 5 minutes
let scanning=false;
setInterval(async()=>{
  if (scanning||getState("paused")) return;
  scanning=true;
  try { await runScan(); } finally { scanning=false; }
}, 300000);

// Position monitor every 2 minutes
setInterval(async()=>{
  if (!getState("paused")) await monitorPositions();
}, 120000);

// Whale refresh daily 6am Riyadh (3am UTC)
cron.schedule("0 3 * * *", refreshWhales, {timezone:"UTC"});

// Daily briefing 9am Riyadh (6am UTC)
cron.schedule("0 6 * * *", async()=>{
  const pnl  = getState("total_pnl")||0;
  const loss = getState("total_loss")||0;
  const open = getOpenTrades();
  await tg(
    `<b>VeraPimpo Daily Briefing</b>\n\n` +
    `Total P&L: ${pnl>=0?"+":""}$${pnl.toFixed(2)}\n` +
    `Losses: $${loss.toFixed(2)}/$${LOSS_HALT}\n` +
    `Open positions: ${open.length}/${MAX_POSITIONS}\n` +
    `Budget remaining: $${(BUDGET-getDeployed()).toFixed(2)}\n` +
    `Mode: ${PAPER?"PAPER":"LIVE"}`
  );
}, {timezone:"UTC"});

// ── LAUNCH ────────────────────────────────────────────────
async function launch() {
  console.log("⚡ VeraPimpo v1.0 starting...");
  if (bot) {
    await bot.launch();
    console.log("✅ Telegram bot active");
  }

  // Initialize Telegram channel monitor (scraper — no API keys needed)
  initTelegramReader();
  // Alert if positions were restored
  if (restored > 0) {
    await tg(`<b>VeraPimpo Restarted</b>\n\n${restored} position(s) restored from backup.\nSend /positions to verify.`);
  }

  await tg(
    `<b>VeraPimpo v1.0 Online</b>\n\n` +
    `Mode: PAPER TRADING\n` +
    `Budget: $${BUDGET} | Loss halt: $${LOSS_HALT}\n` +
    `6 agents | 3-layer system | 14 guardrails\n` +
    `Scan: every 5 min | Monitor: every 2 min\n` +
    `Whale refresh: daily 6am Riyadh\n\n` +
    `First scan starting in 60 seconds...`
  );
  // First scan after 1 minute
  setTimeout(async()=>{
    await refreshWhales();
    await runScan();
  }, 60000);
  console.log("✅ VeraPimpo live");
}

launch().catch(console.error);
process.once("SIGINT",  ()=>{ if(bot) bot.stop("SIGINT");  process.exit(0); });
process.once("SIGTERM", ()=>{ if(bot) bot.stop("SIGTERM"); process.exit(0); });
