// ════════════════════════════════════════════════════════════════════════
// MomBot v1.2 — Polymarket BTC 5-minute UP/DOWN momentum trader
// ════════════════════════════════════════════════════════════════════════
// Built from scratch — no legacy code inherited from v1.1
//
// HARDCODED RULES (non-negotiable):
//   • Side is always BUY — never SELL (no SELL code path exists)
//   • Order type is always TAKER — buys at ask price for immediate fill
//   • No MAKER orders placed — orders never sit in the book
//
// TRADING RULES:
//   • Gap threshold:        $20 (absolute Chainlink delta vs PTB)
//   • Ask price range:      $0.30 - $0.60
//   • Firing window:        600ms - 350ms before market close
//   • Order size:           Scaled by wallet balance (see SCALING table)
//
// SCALING (shares per trade based on wallet balance):
//      $0 -  $49  →  5 shares
//     $50 -  $99  → 10 shares
//    $100 - $149  → 15 shares
//    $150 - $250  → 20 shares
//    $251 - $399  → 25 shares
//    $400+        → 25 shares (continues)
//
// SAFEGUARDS:
//   • Loss safeguard:    Pause if 3 losses occur within any rolling window of 6 trades
//                        (manual restart required)
//   • Liquidity safeguard: Pause new cycles if balance ≤ $7.50 (waiting for redemptions)
//                          (auto-resumes when balance grows)
//
// LOGGING:
//   • >1s remaining:     1 log line per second
//   • Last second only:  millisecond-level tick logging
//
// ════════════════════════════════════════════════════════════════════════

import { ClobClient, Side } from '@polymarket/clob-client-v2';
import { createWalletClient, http } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { polygon } from 'viem/chains';
import { readFileSync, appendFileSync } from 'fs';
import WebSocket from 'ws';

// ════════════════════════════════════════════════════════════════════════
// CONFIG
// ════════════════════════════════════════════════════════════════════════

const CONFIG = {
    ENV_PATH:        '/home/tarryn_mangera/mombot_live/.env',
    LOG_PATH:        '/home/tarryn_mangera/mombot_live/mombot_v12.log',
    RTDS_URL:        'wss://ws-live-data.polymarket.com',
    ORDERBOOK_URL:   'wss://ws-subscriptions-clob.polymarket.com/ws/market',
    GAMMA_API:       'https://gamma-api.polymarket.com/events',
    CLOB_HOST:       'https://clob.polymarket.com',
    CHAIN_ID:        137,
    SIGNATURE_TYPE:  3,

    // Trading rules
    GAP_THRESHOLD:   20,        // Minimum absolute gap in USD
    MIN_ASK:         0.30,      // Minimum ask price
    MAX_ASK:         0.60,      // Maximum ask price
    FIRE_WINDOW_HI:  600,       // ms before close - upper bound
    FIRE_WINDOW_LO:  350,       // ms before close - lower bound

    // Safeguards — rolling window check
    LOSS_WINDOW_SIZE:    6,    // Look at last N trades
    LOSS_WINDOW_THRESHOLD: 3,  // Pause if this many losses occur within window

    // Liquidity safeguard — pause trading while waiting for redemptions
    MIN_BALANCE_TO_TRADE: 7.50, // If balance <= this, pause until balance grows
    BALANCE_PAUSE_RECHECK: 60000, // ms between balance rechecks while paused

    // Timing
    PTB_LOCK_TIMEOUT: 10000,    // ms to wait for PTB lock
    TICK_INTERVAL:    100,      // ms between firing window checks
    CYCLE_PAUSE:      1000,     // ms pause between cycles
    RECONNECT_DELAY:  2000,     // ms before reconnecting RTDS
    RTDS_PING:        5000,     // ms between RTDS pings
};

// ════════════════════════════════════════════════════════════════════════
// MARKET TIME FORMATTER — matches Polymarket UI labels
// ════════════════════════════════════════════════════════════════════════

function formatMarketLabel(openTs, closeTs) {
    // Polymarket displays markets in ET (Eastern Time)
    // SA Time (SAST) is UTC+2, ET (EDT) is UTC-4 → SA is 6 hours ahead of ET
    const openDate  = new Date(openTs * 1000);
    const closeDate = new Date(closeTs * 1000);

    // ET formatter (America/New_York handles EDT/EST automatically)
    const etFmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
    });

    // SA formatter (Africa/Johannesburg)
    const saFmt = new Intl.DateTimeFormat('en-ZA', {
        timeZone: 'Africa/Johannesburg',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });

    const etOpen  = etFmt.format(openDate);
    const etClose = etFmt.format(closeDate);
    const saOpen  = saFmt.format(openDate);
    const saClose = saFmt.format(closeDate);

    const etLabel = `${etOpen}-${etClose} ET`;
    const saLabel = `${saOpen}-${saClose} SAST`;
    const url     = `https://polymarket.com/event/btc-updown-5m-${closeTs}`;

    return { etLabel, saLabel, url, closeTs };
}

// ════════════════════════════════════════════════════════════════════════
// SCALING TABLE
// ════════════════════════════════════════════════════════════════════════

function sharesForBalance(balance) {
    if (balance < 50)   return 5;
    if (balance < 100)  return 10;
    if (balance < 150)  return 15;
    if (balance < 251)  return 20;
    return 25;
}

// ════════════════════════════════════════════════════════════════════════
// LOGGING
// ════════════════════════════════════════════════════════════════════════

function log(level, msg) {
    const line = `${new Date().toISOString()} | ${level} | ${msg}`;
    console.log(line);
    try { appendFileSync(CONFIG.LOG_PATH, line + '\n'); } catch(e) {}
}

const info  = (m) => log('INFO',  m);
const warn  = (m) => log('WARN',  m);
const error = (m) => log('ERROR', m);
const trade = (m) => log('TRADE', m);

// ════════════════════════════════════════════════════════════════════════
// ENV LOADER
// ════════════════════════════════════════════════════════════════════════

function loadEnv() {
    const content = readFileSync(CONFIG.ENV_PATH, 'utf8');
    const env = {};
    for (const line of content.split('\n')) {
        if (!line.includes('=')) continue;
        const [k, ...rest] = line.split('=');
        env[k.trim()] = rest.join('=').trim();
    }
    const required = ['POLY_PRIVATE_KEY', 'POLY_API_KEY', 'POLY_API_SECRET', 'POLY_PASSPHRASE', 'POLY_FUNDER'];
    for (const key of required) {
        if (!env[key]) throw new Error(`Missing required env var: ${key}`);
    }
    return env;
}

// ════════════════════════════════════════════════════════════════════════
// SHARED STATE
// ════════════════════════════════════════════════════════════════════════

const state = {
    chainlink: null,        // { price, ts } - latest BTC tick from RTDS
    rtdsConnected: false,
    recentOutcomes: [],     // Rolling window of last N trade outcomes ('WIN' or 'LOSS')
    walletBalance: 0,
    tradeHistory: [],       // [{ cycle, direction, gap, askPrice, cost, orderId, txHash, timestamp, outcome }]
};

// ════════════════════════════════════════════════════════════════════════
// SAFEGUARD CHECK — rolling window
// ════════════════════════════════════════════════════════════════════════

function safeguardTriggered() {
    if (state.recentOutcomes.length < CONFIG.LOSS_WINDOW_SIZE) return false;
    const lossCount = state.recentOutcomes.filter(o => o === 'LOSS').length;
    return lossCount >= CONFIG.LOSS_WINDOW_THRESHOLD;
}

function recordOutcome(outcome) {
    state.recentOutcomes.push(outcome);
    // Keep only the last N
    if (state.recentOutcomes.length > CONFIG.LOSS_WINDOW_SIZE) {
        state.recentOutcomes.shift();
    }
    const wins   = state.recentOutcomes.filter(o => o === 'WIN').length;
    const losses = state.recentOutcomes.filter(o => o === 'LOSS').length;
    trade(`OUTCOME WINDOW | last ${state.recentOutcomes.length} trades: ${wins}W / ${losses}L | sequence=[${state.recentOutcomes.join(',')}]`);
}

// ════════════════════════════════════════════════════════════════════════
// PERSISTENT RTDS CONNECTION
// ════════════════════════════════════════════════════════════════════════

function connectRTDS() {
    const ws = new WebSocket(CONFIG.RTDS_URL);

    ws.on('open', () => {
        ws.send(JSON.stringify({
            action: 'subscribe',
            subscriptions: [{
                topic: 'crypto_prices_chainlink',
                type: '*',
                filters: JSON.stringify({ symbol: 'btc/usd' }),
            }],
        }));
        state.rtdsConnected = true;
        info('RTDS connected — subscribed to btc/usd Chainlink feed');

        // Keepalive
        const ping = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send('PING');
            } else {
                clearInterval(ping);
            }
        }, CONFIG.RTDS_PING);
    });

    ws.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw.toString());
            const payload = msg.payload;
            if (!payload || !payload.value) return;
            const price = parseFloat(payload.value);
            const ts    = payload.timestamp;
            if (price > 0 && ts) {
                state.chainlink = { price, ts };
            }
        } catch (e) {
            // ignore malformed messages
        }
    });

    ws.on('close', (code) => {
        state.rtdsConnected = false;
        warn(`RTDS closed (code=${code}) — reconnecting in ${CONFIG.RECONNECT_DELAY}ms`);
        setTimeout(connectRTDS, CONFIG.RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        error(`RTDS error: ${err.message}`);
    });
}

// ════════════════════════════════════════════════════════════════════════
// ORDERBOOK SUBSCRIPTION (per cycle)
// ════════════════════════════════════════════════════════════════════════

function subscribeOrderbook(upToken, dnToken) {
    const book = {
        up: { ask: 0, bid: 0, liquidity: 0 },
        dn: { ask: 0, bid: 0, liquidity: 0 },
        ws: null,
    };

    const ws = new WebSocket(CONFIG.ORDERBOOK_URL);
    book.ws = ws;

    ws.on('open', () => {
        ws.send(JSON.stringify({
            assets_ids: [upToken, dnToken],
            type: 'market',
            custom_feature_enabled: true,
        }));
    });

    ws.on('message', (raw) => {
        const msg = raw.toString();
        if (msg === 'PONG' || !msg) return;
        try {
            const events = JSON.parse(msg);
            const list   = Array.isArray(events) ? events : [events];
            for (const evt of list) {
                const tokenId = evt.asset_id || evt.token_id;
                if (!tokenId) continue;
                const target = (tokenId === upToken) ? book.up
                             : (tokenId === dnToken) ? book.dn
                             : null;
                if (!target) continue;

                if (evt.event_type === 'best_bid_ask') {
                    target.bid = parseFloat(evt.best_bid || 0);
                    target.ask = parseFloat(evt.best_ask || 0);
                }
                if (evt.event_type === 'book') {
                    target.liquidity = (evt.asks || [])
                        .slice(0, 5)
                        .reduce((sum, a) => sum + parseFloat(a.size || 0), 0);
                }
            }
        } catch (e) {
            // ignore malformed
        }
    });

    ws.on('error', (err) => warn(`Orderbook WS error: ${err.message}`));

    return book;
}

// ════════════════════════════════════════════════════════════════════════
// MARKET TOKEN FETCH
// ════════════════════════════════════════════════════════════════════════

async function fetchMarketTokens(closeTs) {
    const slug = `btc-updown-5m-${closeTs}`;
    const url  = `${CONFIG.GAMMA_API}?slug=${slug}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`Gamma API status ${resp.status}`);
    const data = await resp.json();
    if (!data || !data[0] || !data[0].markets || !data[0].markets[0]) {
        throw new Error('Market not found in Gamma API response');
    }
    const market = data[0].markets[0];
    const tokens = JSON.parse(market.clobTokenIds);
    return { upToken: tokens[0], dnToken: tokens[1], slug };
}

// ════════════════════════════════════════════════════════════════════════
// CLOB CLIENT FACTORY
// ════════════════════════════════════════════════════════════════════════

function buildClobClient(env) {
    const account = privateKeyToAccount(env.POLY_PRIVATE_KEY);
    const signer  = createWalletClient({
        account,
        transport: http(),
        chain: polygon,
    });
    return new ClobClient({
        host: CONFIG.CLOB_HOST,
        chain: CONFIG.CHAIN_ID,
        signer,
        creds: {
            key: env.POLY_API_KEY,
            secret: env.POLY_API_SECRET,
            passphrase: env.POLY_PASSPHRASE,
        },
        signatureType: CONFIG.SIGNATURE_TYPE,
        funderAddress: env.POLY_FUNDER,
    });
}

// ════════════════════════════════════════════════════════════════════════
// BALANCE FETCH
// ════════════════════════════════════════════════════════════════════════

async function fetchBalance(client) {
    try {
        const result = await client.getBalanceAllowance({ asset_type: 'COLLATERAL' });
        const balance = parseInt(result.balance || '0') / 1_000_000;
        return balance;
    } catch (e) {
        warn(`Balance fetch failed: ${e.message}`);
        return state.walletBalance; // fall back to last known
    }
}

// ════════════════════════════════════════════════════════════════════════
// PTB LOCK (first Chainlink tick ≥ openTs)
// ════════════════════════════════════════════════════════════════════════

async function lockPTB(openTs) {
    const deadline = Date.now() + CONFIG.PTB_LOCK_TIMEOUT;
    while (Date.now() < deadline) {
        if (state.chainlink) {
            const tickSec = Math.floor(state.chainlink.ts / 1000);
            if (tickSec >= openTs) {
                const lag = tickSec - openTs;
                info(`PTB LOCKED | price=$${state.chainlink.price.toFixed(2)} | lag=${lag}s`);
                return { price: state.chainlink.price, lag };
            }
        }
        await sleep(50);
    }
    // Fallback — use whatever we have
    if (state.chainlink) {
        warn(`PTB FALLBACK | using stale tick $${state.chainlink.price.toFixed(2)}`);
        return { price: state.chainlink.price, lag: -1 };
    }
    throw new Error('No Chainlink data available for PTB');
}

// ════════════════════════════════════════════════════════════════════════
// ORDER PLACEMENT — TAKER BUY ONLY (hardcoded)
// ════════════════════════════════════════════════════════════════════════

async function placeTakerBuy(client, tokenId, askPrice, shares, context) {
    // HARDCODED: side is always BUY. There is no SELL path in this bot.
    const order = await client.createAndPostOrder(
        {
            tokenID: tokenId,
            price:   askPrice,
            size:    shares,
            side:    Side.BUY,    // ← LOCKED. Never change to Side.SELL.
        },
        {
            tickSize: '0.01',
            negRisk:  false,
        }
    );
    return order;
}

// ════════════════════════════════════════════════════════════════════════
// MARKET CYCLE
// ════════════════════════════════════════════════════════════════════════

async function runCycle(client) {
    const nowTs   = Math.floor(Date.now() / 1000);
    const closeTs = (Math.floor(nowTs / 300) + 1) * 300;
    const openTs  = closeTs - 300;

    const market = formatMarketLabel(openTs, closeTs);

    info('─'.repeat(70));
    info(`CYCLE | ${market.etLabel} | ${market.saLabel}`);
    info(`Market: btc-updown-5m-${closeTs} | ${market.url}`);
    info(`RTDS=${state.rtdsConnected ? 'connected' : 'DISCONNECTED'} | balance=$${state.walletBalance.toFixed(2)} | shares/trade=${sharesForBalance(state.walletBalance)}`);

    // Wait for market open
    const msUntilOpen = Math.max(0, (openTs - Date.now() / 1000) * 1000);
    if (msUntilOpen > 0) {
        info(`Waiting ${(msUntilOpen/1000).toFixed(1)}s for market open`);
        await sleep(msUntilOpen);
    }

    // Fetch market tokens
    let upToken, dnToken;
    try {
        const tokens = await fetchMarketTokens(closeTs);
        upToken = tokens.upToken;
        dnToken = tokens.dnToken;
        info(`Market tokens fetched | slug=${tokens.slug}`);
    } catch (e) {
        warn(`Skip cycle — market fetch failed: ${e.message}`);
        return { fired: false, won: null };
    }

    // Lock PTB
    let ptb;
    try {
        ptb = await lockPTB(openTs);
    } catch (e) {
        warn(`Skip cycle — PTB lock failed: ${e.message}`);
        return { fired: false, won: null };
    }
    const ptbPrice = ptb.price;

    // Subscribe to orderbook
    const book = subscribeOrderbook(upToken, dnToken);

    // Firing loop
    const result = await new Promise((resolve) => {
        let fired = false;
        let lastLogSec = -1;

        const timer = setInterval(async () => {
            if (fired) return;
            if (!state.chainlink) return;

            const msLeft   = (closeTs * 1000) - Date.now();
            const secsLeft = Math.ceil(msLeft / 1000);
            const cl       = state.chainlink.price;
            const gap      = cl - ptbPrice;
            const absGap   = Math.abs(gap);
            const isUp     = gap >= 0;
            const dirName  = isUp ? 'UP' : 'DN';
            const ask      = isUp ? book.up.ask : book.dn.ask;
            const bid      = isUp ? book.up.bid : book.dn.bid;
            const liq      = isUp ? book.up.liquidity : book.dn.liquidity;

            // Throttled tick log:
            //   • More than 1s remaining → 1 log per second
            //   • Last second (≤1000ms) → every tick (millisecond-level)
            const inLastSecond = msLeft <= 1000 && msLeft > 0;
            const secondChanged = secsLeft !== lastLogSec && secsLeft >= 0;

            if (inLastSecond || secondChanged) {
                lastLogSec = secsLeft;
                const timeMarker = inLastSecond ? `${msLeft}ms` : `${secsLeft}s`;
                info(
                    `${timeMarker} left | PTB=$${ptbPrice.toFixed(2)} CL=$${cl.toFixed(2)} | ` +
                    `${dirName} gap=$${gap.toFixed(2)} | Ask:${ask.toFixed(3)} Bid:${bid.toFixed(3)} Liq:${Math.round(liq)}`
                );
            }

            // FIRING WINDOW CHECK — 600ms to 350ms remaining
            if (msLeft <= CONFIG.FIRE_WINDOW_HI && msLeft >= CONFIG.FIRE_WINDOW_LO) {

                // GAP CHECK — must exceed threshold
                if (absGap < CONFIG.GAP_THRESHOLD) {
                    return; // wait for next tick — gap may grow
                }

                // ASK PRICE CHECKS
                if (ask <= 0) {
                    fired = true;
                    clearInterval(timer);
                    book.ws.close();
                    warn(`SKIP | No valid ask | gap=$${gap.toFixed(2)} | msLeft=${msLeft}`);
                    resolve({ fired: false, won: null });
                    return;
                }
                if (ask < CONFIG.MIN_ASK) {
                    fired = true;
                    clearInterval(timer);
                    book.ws.close();
                    warn(`SKIP | Ask $${ask.toFixed(3)} below MIN $${CONFIG.MIN_ASK} | gap=$${gap.toFixed(2)}`);
                    resolve({ fired: false, won: null });
                    return;
                }
                if (ask > CONFIG.MAX_ASK) {
                    fired = true;
                    clearInterval(timer);
                    book.ws.close();
                    warn(`SKIP | Ask $${ask.toFixed(3)} above MAX $${CONFIG.MAX_ASK} | gap=$${gap.toFixed(2)}`);
                    resolve({ fired: false, won: null });
                    return;
                }

                // ALL CHECKS PASSED — FIRE
                fired = true;
                clearInterval(timer);
                book.ws.close();

                const shares  = sharesForBalance(state.walletBalance);
                const cost    = shares * ask;
                const tokenId = isUp ? upToken : dnToken;

                trade(`🔥 FIRE | ${market.etLabel} | ${market.saLabel} | ${dirName}@${ask.toFixed(3)} | ${shares} shares | $${cost.toFixed(2)} | gap=$${gap.toFixed(2)} | PTB=$${ptbPrice.toFixed(2)} CL=$${cl.toFixed(2)} | msLeft=${msLeft}ms`);
                trade(`     URL: ${market.url}`);

                try {
                    const order = await placeTakerBuy(client, tokenId, ask, shares, {
                        direction: dirName,
                        gap,
                        ptb: ptbPrice,
                        cl,
                    });

                    const success  = order && order.success === true;
                    const orderId  = order?.orderID || 'none';
                    const txHashes = order?.transactionsHashes || [];
                    const taken    = order?.takingAmount || 'unknown';

                    trade(`ORDER | success=${success} | id=${orderId} | filled=${taken} | tx=${txHashes.join(',')}`);

                    if (success) {
                        state.tradeHistory.push({
                            cycle:     closeTs,
                            direction: dirName,
                            gap,
                            askPrice:  ask,
                            cost,
                            orderId,
                            txHash:    txHashes[0] || null,
                            timestamp: new Date().toISOString(),
                        });
                    }

                    resolve({ fired: success, won: null }); // outcome determined later
                } catch (e) {
                    error(`ORDER FAILED | ${e.message}`);
                    resolve({ fired: false, won: null });
                }
                return;
            }

            // Market closed without firing
            if (msLeft <= 0) {
                clearInterval(timer);
                book.ws.close();
                info(`NO TRADE | final gap=$${gap.toFixed(2)} | absGap=$${absGap.toFixed(2)} | threshold=$${CONFIG.GAP_THRESHOLD}`);
                resolve({ fired: false, won: null });
            }
        }, CONFIG.TICK_INTERVAL);
    });

    return result;
}

// ════════════════════════════════════════════════════════════════════════
// OUTCOME RESOLUTION (post-cycle)
// ════════════════════════════════════════════════════════════════════════

async function resolvePreviousTrades(client) {
    // For now, we infer wins/losses from balance changes.
    // If balance went up by ~$5*shares minus cost, it was a win.
    // If balance dropped by ~cost, it was a loss.
    // This is approximate — proper resolution requires querying trade redemptions.

    const currentBalance = await fetchBalance(client);
    const lastBalance    = state.walletBalance;
    const delta          = currentBalance - lastBalance;

    if (state.tradeHistory.length === 0) {
        state.walletBalance = currentBalance;
        return;
    }

    const lastTrade = state.tradeHistory[state.tradeHistory.length - 1];
    if (lastTrade.outcome) {
        state.walletBalance = currentBalance;
        return; // already resolved
    }

    // Heuristic: if balance increased significantly, it's a win
    const expectedWinDelta = (lastTrade.askPrice >= 1 ? 0 : 1) * lastTrade.askPrice; // placeholder
    const shareValue = 1.00; // each winning share resolves to $1
    const shares = lastTrade.cost / lastTrade.askPrice;
    const winPayout = shares * shareValue;
    const winThreshold = winPayout - lastTrade.cost - 0.50; // allow $0.50 fuzz

    if (delta >= winThreshold) {
        lastTrade.outcome = 'WIN';
        recordOutcome('WIN');
        trade(`WIN | balance change +$${delta.toFixed(2)}`);
    } else if (delta < -lastTrade.cost / 2) {
        lastTrade.outcome = 'LOSS';
        recordOutcome('LOSS');
        trade(`LOSS | balance change $${delta.toFixed(2)}`);
    } else {
        // outcome unclear — leave unresolved for now
    }

    state.walletBalance = currentBalance;
}

// ════════════════════════════════════════════════════════════════════════
// UTILITIES
// ════════════════════════════════════════════════════════════════════════

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

// ════════════════════════════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════════════════════════════

async function main() {
    info('═'.repeat(70));
    info('MomBot v1.2 starting');
    info(`Gap threshold: $${CONFIG.GAP_THRESHOLD}`);
    info(`Ask range:     $${CONFIG.MIN_ASK} - $${CONFIG.MAX_ASK}`);
    info(`Fire window:   ${CONFIG.FIRE_WINDOW_HI}ms - ${CONFIG.FIRE_WINDOW_LO}ms before close`);
    info(`Loss safeguard:      pause if ${CONFIG.LOSS_WINDOW_THRESHOLD}+ losses in rolling window of ${CONFIG.LOSS_WINDOW_SIZE} trades`);
    info(`Liquidity safeguard: pause cycles if balance ≤ $${CONFIG.MIN_BALANCE_TO_TRADE}`);
    info(`Tick logging:        1/sec while waiting, every tick in final second`);
    info('═'.repeat(70));

    const env    = loadEnv();
    const client = buildClobClient(env);

    info(`POLY_FUNDER: ${env.POLY_FUNDER}`);

    // Start RTDS
    connectRTDS();
    info('Waiting 3s for RTDS to connect...');
    await sleep(3000);

    // Initial balance check
    state.walletBalance = await fetchBalance(client);
    info(`Initial wallet balance: $${state.walletBalance.toFixed(2)}`);

    // Main loop
    while (true) {
        // Safeguard check — rolling window
        if (safeguardTriggered()) {
            const losses = state.recentOutcomes.filter(o => o === 'LOSS').length;
            error(`⛔ PAUSED — ${losses} losses within last ${state.recentOutcomes.length} trades. Manual restart required.`);
            error(`Sequence: [${state.recentOutcomes.join(',')}]`);
            info('Process will idle indefinitely. Kill with Ctrl+C or "pkill -f node" to stop.');
            // Idle indefinitely — operator must intervene
            while (true) await sleep(60000);
        }

        // Liquidity safeguard — pause until pending redemptions credit
        try {
            await resolvePreviousTrades(client);
        } catch (e) {
            warn(`Outcome resolution skipped: ${e.message}`);
        }

        if (state.walletBalance <= CONFIG.MIN_BALANCE_TO_TRADE) {
            warn(`💰 LIQUIDITY PAUSE | balance=$${state.walletBalance.toFixed(2)} ≤ $${CONFIG.MIN_BALANCE_TO_TRADE} | waiting for redemptions...`);
            while (state.walletBalance <= CONFIG.MIN_BALANCE_TO_TRADE) {
                await sleep(CONFIG.BALANCE_PAUSE_RECHECK);
                const newBal = await fetchBalance(client);
                if (newBal > state.walletBalance) {
                    info(`Balance update: $${state.walletBalance.toFixed(2)} → $${newBal.toFixed(2)}`);
                }
                state.walletBalance = newBal;
            }
            info(`✅ LIQUIDITY OK | balance=$${state.walletBalance.toFixed(2)} > $${CONFIG.MIN_BALANCE_TO_TRADE} | resuming trading`);
        }

        try {
            await runCycle(client);
        } catch (e) {
            error(`Cycle error: ${e.message}`);
            error(e.stack);
        }

        await sleep(CONFIG.CYCLE_PAUSE);
    }
}

// ════════════════════════════════════════════════════════════════════════
// ENTRY POINT
// ════════════════════════════════════════════════════════════════════════

main().catch((e) => {
    error(`FATAL: ${e.message}`);
    error(e.stack);
    process.exit(1);
});
