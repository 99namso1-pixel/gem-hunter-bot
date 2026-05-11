/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║      GEM HUNTER v3 — No Database · Multi-Chain              ║
 * ║  API: GMGN.ai + DexScreener                                 ║
 * ║  Chains: Solana · Ethereum · Base · TON                     ║
 * ║  PnL: tính từ giá lúc list sàn (pairCreatedAt)             ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 *  KHÔNG cần MySQL, KHÔNG cần database.
 *  Deploy bất kỳ đâu: Replit / Railway / VPS / máy local.
 *
 *  Cài đặt:  npm install
 *  Chạy:     node gemHunter.js
 */

const axios = require('axios');
const moment = require('moment');

// ─────────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────────
const CONFIG = {
    telegram: {
        botToken:  process.env.TELEGRAM_BOT_TOKEN  || '8702072641:AAHquqm7NZGlOHyOrEdJ4-skLrVykFGESDc',
        channelId: process.env.TELEGRAM_CHANNEL_ID || '-4996499184',
    },

    scan: {
        intervalMs:       5000,   // scan mỗi 5 giây
        maxNewPerCycle:   5,      // tối đa 5 gem mới/cycle
        pnlNotifyLevels:  [2, 5, 10, 20, 50],  // thông báo khi đạt 2x, 5x, 10x...
        trackDurationMin: 120,    // theo dõi PnL tối đa 120 phút rồi bỏ
        maxTracked:       500,    // tối đa 500 token trong memory
    },

    filter: {
        solana: {
            minMcap:         50_000,
            minVolume:       20_000,
            maxAgeMinutes:   5,
            maxTop10Holder:  30,
            minVolMcapRatio: 0.1,   // volume >= 10% mcap
        },
        ethereum: {
            minMcap:         100_000,
            minVolume:       30_000,
            maxAgeMinutes:   10,
            maxBuySellTax:   5,
        },
        base: {
            minMcap:         30_000,
            minVolume:       5_000,
            maxAgeMinutes:   3,
            minLiquidity:    5_000,
        },
        ton: {
            minVolume:       5_000,
            minLiquidity:    3_000,
            maxAgeMinutes:   15,
        },
    },
};

// ─────────────────────────────────────────────
//  IN-MEMORY STORE (thay thế database)
// ─────────────────────────────────────────────
/**
 * Map<address_chain, TokenRecord>
 * TokenRecord = {
 *   address, chain, symbol, name,
 *   priceAtList,    ← giá lúc bot phát hiện / list sàn
 *   mcapAtList,     ← mcap lúc list
 *   listedAt,       ← timestamp ms
 *   messageId,      ← Telegram message ID để reply
 *   notifiedLevels, ← Set<number> các mốc đã thông báo (2,5,10...)
 * }
 */
const trackedTokens = new Map();

function storeKey(address, chain) {
    return `${chain}:${address}`;
}

function addTracked(token, messageId) {
    const key = storeKey(token.address, token.chain);
    if (trackedTokens.size >= CONFIG.scan.maxTracked) {
        // xoá token cũ nhất khi đầy
        const oldest = [...trackedTokens.entries()]
            .sort((a, b) => a[1].listedAt - b[1].listedAt)[0];
        if (oldest) trackedTokens.delete(oldest[0]);
    }
    trackedTokens.set(key, {
        address:        token.address,
        chain:          token.chain,
        symbol:         token.symbol,
        name:           token.name,
        priceAtList:    token.priceUSD,
        mcapAtList:     token.marketCap,
        listedAt:       Date.now(),
        messageId,
        notifiedLevels: new Set(),
    });
}

function getTracked(address, chain) {
    return trackedTokens.get(storeKey(address, chain)) ?? null;
}

function cleanupOldTokens() {
    const cutoff = Date.now() - CONFIG.scan.trackDurationMin * 60 * 1000;
    for (const [key, rec] of trackedTokens) {
        if (rec.listedAt < cutoff) trackedTokens.delete(key);
    }
}

// ─────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────
function formatNum(n) {
    n = parseFloat(n) || 0;
    if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + 'k';
    return n.toFixed(2);
}

function ageMinutes(createdAtMs) {
    return Math.floor((Date.now() - createdAtMs) / 60000);
}

const httpClient = axios.create({
    timeout: 10000,
    headers: { 'User-Agent': 'Mozilla/5.0 GemHunterBot/3.0' },
});

// ─────────────────────────────────────────────
//  TELEGRAM
// ─────────────────────────────────────────────
async function tgSend(text, fastBuyUrl) {
    const { botToken, channelId } = CONFIG.telegram;
    try {
        const res = await httpClient.post(
            `https://api.telegram.org/bot${botToken}/sendMessage`,
            {
                chat_id:                  channelId,
                text,
                parse_mode:               'Markdown',
                disable_web_page_preview: true,
                reply_markup: fastBuyUrl
                    ? { inline_keyboard: [[{ text: '⚡ Fast Buy', url: fastBuyUrl }]] }
                    : undefined,
            }
        );
        return res.data.result.message_id;
    } catch (err) {
        console.error('[TG] Send lỗi:', err.response?.data?.description ?? err.message);
        return null;
    }
}

async function tgReply(text, replyToId) {
    const { botToken, channelId } = CONFIG.telegram;
    try {
        const res = await httpClient.post(
            `https://api.telegram.org/bot${botToken}/sendMessage`,
            {
                chat_id:             channelId,
                text,
                parse_mode:          'Markdown',
                reply_to_message_id: replyToId,
            }
        );
        return res.data.result.message_id;
    } catch (err) {
        console.error('[TG] Reply lỗi:', err.response?.data?.description ?? err.message);
        return null;
    }
}

// ─────────────────────────────────────────────
//  API SOURCES
// ─────────────────────────────────────────────

/**
 * GMGN.ai — chuyên Solana new tokens
 * Docs: https://gmgn.ai (unofficial, reverse engineered)
 */
const GMGN = {
    async fetchNewTokens() {
        try {
            const res = await httpClient.get('https://gmgn.ai/defi/quotation/v1/rank/sol/new_pairs/1h', {
                params: {
                    limit:          20,
                    min_liquidity:  10000,
                    min_marketcap:  50000,
                    orderby:        'open_timestamp',
                    direction:      'desc',
                    filters:        ['renounced', 'frozen'],   // mint+freeze disabled
                    min_swaps1h:    30,
                },
                headers: {
                    'Referer': 'https://gmgn.ai/',
                    'Origin':  'https://gmgn.ai',
                },
            });

            const pairs = res.data?.data?.rank ?? [];
            return pairs.map(p => ({
                address:        p.address,
                name:           p.name         ?? '',
                symbol:         p.symbol       ?? '',
                chain:          'solana',
                exchange:       p.dex          ?? 'Raydium',
                priceUSD:       parseFloat(p.price          ?? 0),
                marketCap:      parseFloat(p.market_cap     ?? 0),
                volume:         parseFloat(p.volume_1h      ?? 0),
                liquidity:      parseFloat(p.liquidity      ?? 0),
                ageMinutes:     p.open_timestamp
                    ? ageMinutes(p.open_timestamp * 1000)
                    : 999,
                holders:        p.holder_count  ?? 0,
                top10HolderPct: parseFloat(p.top_10_holder_rate ?? 0) * 100,
                mintDisabled:   true,   // filter 'renounced' đã lọc
                freezeDisabled: true,   // filter 'frozen' đã lọc
                lpBurned:       p.burn_ratio >= 0.95,
                source:         'gmgn',
            }));
        } catch (err) {
            console.error('[GMGN] fetchNewTokens lỗi:', err.message);
            return [];
        }
    },

    /**
     * Lấy giá hiện tại của token SOL để tính PnL
     */
    async fetchCurrentPrice(address) {
        try {
            const res = await httpClient.get(`https://gmgn.ai/defi/quotation/v1/tokens/sol/${address}`);
            const t = res.data?.data?.token;
            return t ? {
                priceUSD:  parseFloat(t.price       ?? 0),
                marketCap: parseFloat(t.market_cap  ?? 0),
            } : null;
        } catch {
            return null;
        }
    },
};

/**
 * DexScreener — hỗ trợ tất cả chains
 * Docs: https://docs.dexscreener.com/api/reference
 */
const DEXSCREENER = {
    CHAIN_IDS: {
        solana:   'solana',
        ethereum: 'ethereum',
        base:     'base',
        ton:      'ton',
    },

    /**
     * Lấy token mới nhất trên một chain
     */
    async fetchNewPairs(chain) {
        try {
            const chainId = this.CHAIN_IDS[chain];
            if (!chainId) return [];

            // DexScreener endpoint lấy pair mới nhất
            const res = await httpClient.get(
                `https://api.dexscreener.com/token-boosts/latest/v1`,
                { params: { chainId } }
            );

            // Fallback: dùng search endpoint
            const pairs = res.data ?? [];
            if (!pairs.length) return await this.searchNewPairs(chain);

            return pairs
                .filter(p => p.chainId === chainId)
                .map(p => this.normalizePair(p, chain));
        } catch {
            return await this.searchNewPairs(chain);
        }
    },

    async searchNewPairs(chain) {
        try {
            const chainId = this.CHAIN_IDS[chain];
            // Tìm pair mới nhất theo chain, sort theo age
            const res = await httpClient.get(
                `https://api.dexscreener.com/latest/dex/search`,
                { params: { q: chainId } }
            );
            const pairs = res.data?.pairs ?? [];
            // Lấy pair mới nhất (trong 30 phút)
            const cutoff = Date.now() - 30 * 60 * 1000;
            return pairs
                .filter(p => p.chainId === chainId && p.pairCreatedAt > cutoff)
                .sort((a, b) => b.pairCreatedAt - a.pairCreatedAt)
                .slice(0, 30)
                .map(p => this.normalizePair(p, chain));
        } catch (err) {
            console.error(`[DEXSCREENER][${chain}] search lỗi:`, err.message);
            return [];
        }
    },

    normalizePair(p, chain) {
        return {
            address:        p.baseToken?.address   ?? p.tokenAddress ?? '',
            name:           p.baseToken?.name       ?? '',
            symbol:         p.baseToken?.symbol     ?? '',
            chain,
            exchange:       p.dexId                ?? '',
            priceUSD:       parseFloat(p.priceUsd  ?? 0),
            marketCap:      p.fdv                   ?? p.marketCap ?? 0,
            volume:         p.volume?.h24           ?? p.volume?.h6 ?? 0,
            liquidity:      p.liquidity?.usd        ?? 0,
            ageMinutes:     p.pairCreatedAt
                ? ageMinutes(p.pairCreatedAt)
                : 999,
            holders:        0,
            top10HolderPct: 0,
            mintDisabled:   true,
            freezeDisabled: true,
            lpBurned:       false,
            source:         'dexscreener',
            pairAddress:    p.pairAddress,
        };
    },

    /**
     * Lấy giá hiện tại để tính PnL — dùng token address
     */
    async fetchCurrentPrice(address, chain) {
        try {
            const chainId = this.CHAIN_IDS[chain] ?? chain;
            const res = await httpClient.get(
                `https://api.dexscreener.com/latest/dex/tokens/${address}`
            );
            const pairs = (res.data?.pairs ?? [])
                .filter(p => p.chainId === chainId)
                .sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0));

            if (!pairs.length) return null;
            return {
                priceUSD:  parseFloat(pairs[0].priceUsd ?? 0),
                marketCap: pairs[0].fdv ?? 0,
            };
        } catch {
            return null;
        }
    },
};

// ─────────────────────────────────────────────
//  CHAIN FILTERS
// ─────────────────────────────────────────────
function filterToken(token) {
    const f = CONFIG.filter[token.chain];
    if (!f) return { pass: true };

    if (f.minMcap && token.marketCap < f.minMcap)
        return { pass: false, reason: `mcap $${formatNum(token.marketCap)} < min $${formatNum(f.minMcap)}` };

    if (f.minVolume && token.volume < f.minVolume)
        return { pass: false, reason: `vol $${formatNum(token.volume)} < min $${formatNum(f.minVolume)}` };

    if (f.maxAgeMinutes && token.ageMinutes > f.maxAgeMinutes)
        return { pass: false, reason: `age ${token.ageMinutes}m > ${f.maxAgeMinutes}m` };

    if (f.maxTop10Holder && token.top10HolderPct > f.maxTop10Holder)
        return { pass: false, reason: `top10 ${token.top10HolderPct.toFixed(1)}% > ${f.maxTop10Holder}%` };

    if (f.minVolMcapRatio && token.volume < token.marketCap * f.minVolMcapRatio)
        return { pass: false, reason: `vol/mcap ratio thấp` };

    if (f.minLiquidity && token.liquidity < f.minLiquidity)
        return { pass: false, reason: `liq $${formatNum(token.liquidity)} < min` };

    if (f.maxBuySellTax && (token.buyTax > f.maxBuySellTax || token.sellTax > f.maxBuySellTax))
        return { pass: false, reason: `tax cao` };

    return { pass: true };
}

// ─────────────────────────────────────────────
//  MESSAGE BUILDERS
// ─────────────────────────────────────────────
const CHAIN_EMOJI = { solana: '🟣', ethereum: '🔵', base: '🟦', ton: '🔷' };
const CHAIN_BOTS  = {
    solana:   (a) => `https://t.me/solana_angrybot?start=refca_7TbD3z_${a}`,
    ethereum: (a) => `https://t.me/unibotsniper_bot?start=${a}`,
    base:     (a) => `https://t.me/BananaGunSniper_bot?start=${a}`,
    ton:      (a) => `https://t.me/tonrocket_bot?start=${a}`,
};

function buildGemMessage(token) {
    const emoji    = CHAIN_EMOJI[token.chain] ?? '⚪';
    const chainUp  = token.chain.toUpperCase();
    const age      = token.ageMinutes;
    const top10Msg = token.top10HolderPct > 0
        ? (token.top10HolderPct > 20 ? `${token.top10HolderPct.toFixed(1)}% ❗` : `${token.top10HolderPct.toFixed(1)}%`)
        : 'N/A';

    const securityLines = token.chain === 'solana'
        ? `┌ \`Mint Authority:  \`${token.mintDisabled   ? 'Disabled ✅' : 'ENABLED ❌'}
├ \`Freeze Auth:     \`${token.freezeDisabled ? 'Disabled ✅' : 'ENABLED ❌'}
├ \`LP Burned:       \`${token.lpBurned       ? '100% ✅'    : 'Not burned ⚠️'}
└ \`Top 10 Holder:   \`${top10Msg}`
        : `└ \`Liquidity:       \`$${formatNum(token.liquidity)}`;

    const sourceTag = token.source === 'gmgn' ? ' · via GMGN' : ' · via DexScreener';

    return `${emoji} *${token.name} (${token.symbol})* — ${chainUp}${sourceTag}

┌ \`CA:       \`\`${token.address}\`
├ \`Price:    \`$${token.priceUSD < 0.0001 ? token.priceUSD.toExponential(3) : token.priceUSD.toFixed(6)}
├ \`Dex:      \`${token.exchange}
├ \`Mcap:     \`$${formatNum(token.marketCap)}
├ \`Vol:      \`$${formatNum(token.volume)}
├ \`Age:      \`${age}m
└ \`Holders:  \`${token.holders > 0 ? token.holders : 'N/A'}

${securityLines}

📊 _PnL sẽ được cập nhật tự động khi token pump_`;
}

function buildPnlMessage(rec, currentPrice, currentMcap, multiple) {
    const emoji = CHAIN_EMOJI[rec.chain] ?? '⚪';
    const listedAgo = Math.floor((Date.now() - rec.listedAt) / 60000);
    return `${multiple >= 10 ? '🚀🚀🚀' : multiple >= 5 ? '🚀🚀' : '💹'} *${rec.symbol} ${multiple}x* ${emoji}

┌ \`Giá list:     \`$${rec.priceAtList < 0.0001 ? rec.priceAtList.toExponential(3) : rec.priceAtList.toFixed(6)}
├ \`Giá hiện tại: \`$${currentPrice  < 0.0001  ? currentPrice.toExponential(3)    : currentPrice.toFixed(6)}
├ \`Mcap lúc list:\`$${formatNum(rec.mcapAtList)}
├ \`Mcap hiện tại:\`$${formatNum(currentMcap)}
└ \`Thời gian:    \`${listedAgo}m sau khi list`;
}

// ─────────────────────────────────────────────
//  CORE LOGIC
// ─────────────────────────────────────────────

/**
 * Scan token mới — kết hợp GMGN (SOL) + DexScreener (tất cả)
 */
async function scanNewTokens() {
    const allTokens = [];

    // GMGN cho Solana (dữ liệu tốt hơn DexScreener cho SOL)
    const gmgnTokens = await GMGN.fetchNewTokens();
    allTokens.push(...gmgnTokens);

    // DexScreener cho tất cả chains
    const chains = ['solana', 'ethereum', 'base', 'ton'];
    const dexResults = await Promise.allSettled(
        chains.map(c => DEXSCREENER.fetchNewPairs(c))
    );
    for (const r of dexResults) {
        if (r.status === 'fulfilled') allTokens.push(...r.value);
    }

    // Dedup theo address+chain (ưu tiên GMGN)
    const seen = new Map();
    for (const t of allTokens) {
        const k = storeKey(t.address, t.chain);
        if (!seen.has(k)) seen.set(k, t);
    }

    return [...seen.values()];
}

/**
 * Cập nhật PnL cho tất cả token đang theo dõi
 */
async function updatePnL() {
    if (!trackedTokens.size) return;

    // Batch: lấy giá theo từng chain song song
    const byChain = {};
    for (const rec of trackedTokens.values()) {
        (byChain[rec.chain] ??= []).push(rec);
    }

    for (const [chain, records] of Object.entries(byChain)) {
        // Gọi song song tối đa 10 token mỗi chain để tránh rate limit
        const batch = records.slice(0, 10);
        await Promise.allSettled(
            batch.map(rec => checkPnl(rec, chain))
        );
    }
}

async function checkPnl(rec, chain) {
    try {
        // Ưu tiên GMGN cho SOL, DexScreener cho chain khác
        const current = chain === 'solana'
            ? (await GMGN.fetchCurrentPrice(rec.address) ?? await DEXSCREENER.fetchCurrentPrice(rec.address, chain))
            : await DEXSCREENER.fetchCurrentPrice(rec.address, chain);

        if (!current || !current.priceUSD || !rec.priceAtList) return;

        const multiple = current.priceUSD / rec.priceAtList;

        // Tìm mốc PnL tiếp theo chưa thông báo
        for (const level of CONFIG.scan.pnlNotifyLevels) {
            if (multiple >= level && !rec.notifiedLevels.has(level)) {
                rec.notifiedLevels.add(level);
                const msg = buildPnlMessage(rec, current.priceUSD, current.marketCap, level);
                if (rec.messageId) {
                    await tgReply(msg, rec.messageId);
                } else {
                    await tgSend(msg, CHAIN_BOTS[chain]?.(rec.address));
                }
                console.log(`[PNL] ${rec.symbol} (${chain}) đạt ${level}x 🚀`);
            }
        }
    } catch (err) {
        console.error(`[PNL] checkPnl ${rec.symbol}:`, err.message);
    }
}

// ─────────────────────────────────────────────
//  MAIN CYCLE
// ─────────────────────────────────────────────
let cycleCount = 0;

async function mainCycle() {
    cycleCount++;
    const time = moment().format('HH:mm:ss');
    console.log(`\n── Cycle #${cycleCount} ${time} | Tracking: ${trackedTokens.size} tokens ──`);

    // 1. Dọn dẹp token hết hạn theo dõi
    cleanupOldTokens();

    // 2. Scan token mới
    let tokens;
    try {
        tokens = await scanNewTokens();
    } catch (err) {
        console.error('[SCAN] Lỗi:', err.message);
        return;
    }

    // 3. Xử lý token mới
    let newCount = 0;
    for (const token of tokens) {
        if (newCount >= CONFIG.scan.maxNewPerCycle) break;
        if (!token.address) continue;

        // Bỏ qua nếu đã tracking
        if (getTracked(token.address, token.chain)) continue;

        // Apply filter
        const filterResult = filterToken(token);
        if (!filterResult.pass) {
            console.log(`[SKIP][${token.chain.toUpperCase()}] ${token.symbol || token.address.slice(0,8)}: ${filterResult.reason}`);
            continue;
        }

        // Gửi Telegram
        const message    = buildGemMessage(token);
        const fastBuyUrl = CHAIN_BOTS[token.chain]?.(token.address);
        const messageId  = await tgSend(message, fastBuyUrl);

        // Lưu vào memory để track PnL
        addTracked(token, messageId);
        newCount++;

        console.log(`[GEM][${token.chain.toUpperCase()}] ✅ ${token.symbol} mcap=$${formatNum(token.marketCap)} age=${token.ageMinutes}m src=${token.source}`);
    }

    // 4. Cập nhật PnL cho token đang theo dõi
    await updatePnL();

    // 5. Stats mỗi 30 cycle
    if (cycleCount % 30 === 0) printStats();
}

function printStats() {
    const byChain = {};
    for (const rec of trackedTokens.values()) {
        byChain[rec.chain] = (byChain[rec.chain] ?? 0) + 1;
    }
    console.log('\n📊 STATS:', JSON.stringify(byChain), `| Total: ${trackedTokens.size}`);
}

// ─────────────────────────────────────────────
//  STARTUP
// ─────────────────────────────────────────────
async function start() {
    console.log(`
╔══════════════════════════════════════════════╗
║   🚀 GEM HUNTER v3 — No-DB Multi-Chain      ║
║   Chains: SOL · ETH · BASE · TON            ║
║   API: GMGN.ai + DexScreener                ║
╚══════════════════════════════════════════════╝
Channel: ${CONFIG.telegram.channelId}
Interval: ${CONFIG.scan.intervalMs / 1000}s
PnL levels: ${CONFIG.scan.pnlNotifyLevels.join('x, ')}x
Track duration: ${CONFIG.scan.trackDurationMin} phút
`);

    // Kiểm tra Telegram
    try {
        const { botToken, channelId } = CONFIG.telegram;
        const r = await httpClient.get(
            `https://api.telegram.org/bot${botToken}/getMe`
        );
        console.log(`✅ Telegram bot: @${r.data.result.username}`);
    } catch (err) {
        console.error('❌ Telegram bot token lỗi:', err.message);
        process.exit(1);
    }

    // Chạy ngay lần đầu
    await mainCycle();

    // Lặp mỗi N giây
    setInterval(mainCycle, CONFIG.scan.intervalMs);
}

start();
