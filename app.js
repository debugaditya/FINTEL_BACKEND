const express    = require('express');
const cors       = require('cors');
const dotenv     = require('dotenv');
const { google } = require('googleapis');
const crypto     = require('crypto');
dotenv.config();
const { MongoClient, ServerApiVersion } = require('mongodb');
const Redis    = require('ioredis');
const rateLimit = require('express-rate-limit');
const http     = require('http');
const Razorpay = require('razorpay');
const admin    = require('firebase-admin');

// ─── Firebase Admin ────────────────────────────────────────────────────────────
try {
    const serviceAccount = require('./firebase-service-account.json');
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    console.log('[Firebase] Admin initialized successfully');
} catch (e) {
    console.warn('[Firebase] Push notifications disabled:', e.message);
}

const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: process.env.ALLOWED_ORIGIN || '*', methods: ['GET', 'POST', 'HEAD'] }));
app.use(express.json({ limit: "10kb" })); // prevent large payload attacks

// ─── Redis ─────────────────────────────────────────────────────────────────────
const redis = new Redis(process.env.REDIS_URL, { tls: { rejectUnauthorized: false } });
redis.on('connect', () => console.log('[Redis] Connected'));
redis.on('error',   (err) => console.error('[Redis] Error:', err));

// ─── News API keys & intervals ─────────────────────────────────────────────────
const newsio_bots    = [process.env.news_io1, process.env.news_io2, process.env.news_io3];
const marketaux_bots = [process.env.marketaux1, process.env.marketaux2, process.env.marketaux3, process.env.marketaux4, process.env.marketaux5];
const finhub_bots    = [process.env.finhub1];

const interval1 = 8  / newsio_bots.length    * 60 * 1000;
const interval2 = 16 / marketaux_bots.length * 60 * 1000;
const interval3 = 60 * 1000;

let cnt1 = 0, cnt2 = 0, cnt3 = 0;

// ─── MongoDB ───────────────────────────────────────────────────────────────────
let mongoClient;
let usersCollection;

const uri = process.env.MONGODB_URI;

const oauth2Client = new google.auth.OAuth2(
    process.env.YOUR_CLIENT_ID,
    process.env.YOUR_CLIENT_SECRET
);

const razorpay = new Razorpay({
    key_id:     process.env.RAZORPAY_KEY_ID,
    key_secret: process.env.RAZORPAY_KEY_SECRET
});

// Global limiter
const limiter = rateLimit({
    windowMs: 60 * 1000, max: 100,
    message: 'Too many requests, please try again later.',
    standardHeaders: true, legacyHeaders: false,
});
app.use(limiter);

// Strict limiter for auth routes — 10 requests/min per IP
const authLimiter = rateLimit({
    windowMs: 60 * 1000, max: 10,
    message: 'Too many auth attempts, please try again later.',
    standardHeaders: true, legacyHeaders: false,
});

// ─── Routes ────────────────────────────────────────────────────────────────────
app.head('/health', (req, res) => res.status(200).end());

// Sign in
app.post('/signin', authLimiter, async (req, res) => {
    const { code: accessToken, email } = req.body;
    if (!accessToken || !email) return res.status(400).json({ error: 'accessToken and email are required' });

    try {
        const googleRes = await fetch('https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=' + accessToken);
        const tokenInfo = await googleRes.json();
        if (tokenInfo.error || tokenInfo.email !== email) return res.status(401).json({ error: 'Invalid access token' });
    } catch (e) {
        return res.status(500).json({ error: 'Token verification failed' });
    }

    try {
        let isNew = false;
        const user = await usersCollection.findOne({ email });
        if (!user) {
            isNew = true;
            await usersCollection.insertOne({ email, expiry: Date.now() + 7 * 24 * 60 * 60 * 1000 });
        }
        const updatedUser = await usersCollection.findOne({ email });
        return res.json({
            message: 'Authentication successful',
            email,
            expiry:  updatedUser?.expiry || Date.now() + 7 * 24 * 60 * 60 * 1000,
            signup:  isNew ? 1 : 0,
        });
    } catch (e) {
        return res.status(500).json({ error: 'Database error' });
    }
});

// Save FCM token to Redis
app.post('/register-token', authLimiter, async (req, res) => {
    const { email, fcmToken } = req.body;
    if (!email || !fcmToken) return res.status(400).json({ error: 'email and fcmToken required' });
    // validate email format
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'Invalid email' });
    // validate FCM token format (must be non-trivial string)
    if (typeof fcmToken !== 'string' || fcmToken.length < 100) return res.status(400).json({ error: 'Invalid FCM token' });
    try {
        // verify user exists in DB before saving token
        const user = await usersCollection.findOne({ email });
        if (!user) return res.status(404).json({ error: 'User not found' });
        const expiry = user?.expiry || Date.now() + 7 * 24 * 60 * 60 * 1000;
        // store in Redis: fcm:{email} → { fcmToken, expiry }
        await redis.set(`fcm:${email}`, JSON.stringify({ fcmToken, expiry }));
        console.log(`[FCM] Token cached in Redis for ${email}`);
        res.json({ success: true });
    } catch (e) {
        console.error('[FCM] Token save error:', e);
        res.status(500).json({ error: 'DB error' });
    }
});

// Create Razorpay order
app.post('/create-order', async (req, res) => {
    const { days } = req.body;
    const amount = days === 30 ? 100000 : days === 365 ? 1000000 : 0;
    if (!amount) return res.status(400).json({ error: 'Invalid plan' });
    try {
        const order = await razorpay.orders.create({ amount, currency: 'INR', receipt: `receipt_${Date.now()}` });
        res.json({ orderId: order.id, amount, key: process.env.RAZORPAY_KEY_ID });
    } catch (e) {
        res.status(500).json({ error: 'Could not create order' });
    }
});

// Verify payment
app.post('/verify-payment', async (req, res) => {
    const { razorpay_order_id, razorpay_payment_id, razorpay_signature, email, days } = req.body;
    if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature || !email || !days)
        return res.status(400).json({ error: 'Missing required fields' });

    const body              = razorpay_order_id + '|' + razorpay_payment_id;
    const expectedSignature = crypto.createHmac('sha256', process.env.RAZORPAY_KEY_SECRET).update(body).digest('hex');
    if (expectedSignature !== razorpay_signature) return res.status(400).json({ error: 'Invalid payment signature' });

    try {
        const user      = await usersCollection.findOne({ email });
        const newExpiry = Math.max(user?.expiry || Date.now(), Date.now()) + days * 24 * 60 * 60 * 1000;
        await usersCollection.updateOne({ email }, { $set: { expiry: newExpiry } }, { upsert: true });

        // update Redis expiry too
        const cached = await redis.get(`fcm:${email}`);
        if (cached) {
            const parsed = JSON.parse(cached);
            await redis.set(`fcm:${email}`, JSON.stringify({ ...parsed, expiry: newExpiry }));
            console.log(`[FCM] Updated Redis expiry for ${email}`);
        }

        res.json({ message: 'Payment verified', expiry: newExpiry });
    } catch (e) {
        res.status(500).json({ error: 'Database error' });
    }
});

// Sign out — delete FCM token from Redis
app.post('/signout', async (req, res) => {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email required' });
    try {
        await redis.del(`fcm:${email}`);
        console.log(`[FCM] Token deleted from Redis for ${email}`);
        res.json({ message: 'Signout successful' });
    } catch (e) {
        res.status(500).json({ error: 'DB error' });
    }
});

// ─── Helpers ───────────────────────────────────────────────────────────────────
function normalize(text) {
    return text.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
}

function generateNotifId() {
    return Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}

// ─── AI Analysis ───────────────────────────────────────────────────────────────
async function analyzeArticle(article, event_hash) {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 60000); // 60s timeout
        const res = await fetch(process.env.PYTHON_BACKEND_URL || 'http://localhost:8000/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            signal: controller.signal,
            body: JSON.stringify({
                title:   article.title       || article.headline || '',
                content: article.description || article.content  || article.summary || article.text || '',
                url:     article.url         || article.link     || ''
            })
        });
        clearTimeout(timeout);
        if (!res.ok) throw new Error(`AI backend returned ${res.status}`);
        const result = await res.json();
        if (result) await sendNotification({ ...result, event_hash, timestamp: Date.now() });
    } catch (e) {
        console.error('[Analyze] Error:', e.message);
    }
}

// ─── Send FCM push to all active subscribers ───────────────────────────────────
async function sendNotification(result) {
    const notifId    = generateNotifId();
    const event_hash = result.event_hash || notifId;

    // Send FCM push to all active subscribers
    if (!admin.apps.length) {
        console.log('[FCM] Firebase not initialized, skipping push');
        return;
    }

    try {
        // use SCAN instead of KEYS — non-blocking, safe for production
        const keys = [];
        let cursor = '0';
        do {
            const [nextCursor, batch] = await redis.scan(cursor, 'MATCH', 'fcm:*', 'COUNT', 100);
            keys.push(...batch);
            cursor = nextCursor;
        } while (cursor !== '0');
        if (keys.length === 0) {
            console.log('[FCM] No active subscribers with FCM tokens');
            return;
        }

        // MGET fetches all values in one round trip instead of N individual GETs
        const values = await redis.mget(...keys);
        const now    = Date.now();
        const users  = [];
        const expiredKeys = [];

        values.forEach((val, idx) => {
            if (!val) return;
            try {
                const { fcmToken, expiry } = JSON.parse(val);
                if (expiry > now && fcmToken) {
                    const email = keys[idx].replace('fcm:', '');
                    users.push({ email, fcmToken, expiry });
                } else if (expiry <= now) {
                    expiredKeys.push(keys[idx]);
                }
            } catch (e) {}
        });

        // clean expired keys in one pipeline call
        if (expiredKeys.length > 0) {
            await redis.del(...expiredKeys);
            console.log(`[FCM] Cleaned ${expiredKeys.length} expired tokens`);
        }

        if (users.length === 0) {
            console.log('[FCM] No active subscribers with valid tokens');
            return;
        }

        const sentiment      = result.sentiment?.label || 'neutral';
        const emoji          = sentiment === 'positive' ? '📈' : sentiment === 'negative' ? '📉' : '📊';
        const sentimentLabel = sentiment === 'positive' ? 'BULLISH' : sentiment === 'negative' ? 'BEARISH' : 'NEUTRAL';
        const score          = String(result.sentiment?.score || 0);
        const impact         = parseFloat(score) >= 0.75 ? 'HIGH' : parseFloat(score) >= 0.45 ? 'MEDIUM' : 'LOW';

        // Build compact data payload (must be strings, under 4KB total)
        const dataPayload = {
            notifId,
            event_hash,
            title:             (result.title     || '').slice(0, 200),
            content:           (result.content   || '').slice(0, 500),
            sentiment_label:   sentiment,
            sentiment_score:   score,
            regions:           JSON.stringify(result.regions           || []),
            sectors:           JSON.stringify(result.sectors           || []),
            asset_classes:     JSON.stringify(result.asset_classes     || []),
            entities_affected: JSON.stringify(result.entities_affected || []),
            timestamp:         String(result.timestamp || Date.now()),
            impact,
            crux:              (result.crux || '').slice(0, 500),
        };

        const messages = users.map(user => ({
            token: user.fcmToken,
            notification: {
                title: `${emoji} ${sentimentLabel} · FINTEL`,
                body:  (result.title || 'New market alert').slice(0, 100),
            },
            data: dataPayload,
            android: {
                priority: 'high',
                notification: { sound: 'default', channelId: 'fintel_news' }
            }
        }));

        // Send in batches of 500
        for (let i = 0; i < messages.length; i += 500) {
            const batch    = messages.slice(i, i + 500);
            const response = await admin.messaging().sendEach(batch);
            console.log(`[FCM] Sent ${response.successCount}/${batch.length} notifications`);

            // Clean invalid tokens
            response.responses.forEach((resp, idx) => {
                if (!resp.success) {
                    const code = resp.error?.code;
                    if (code === 'messaging/invalid-registration-token' ||
                        code === 'messaging/registration-token-not-registered') {
                        const invalidEmail = users[i + idx]?.email;
                        if (invalidEmail) {
                            redis.del(`fcm:${invalidEmail}`);
                            console.log(`[FCM] Removed invalid token from Redis for ${invalidEmail}`);
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.error('[FCM] Push error:', e.message);
    }
}

// ─── Article processing queue ──────────────────────────────────────────────────
const articleQueue = [];
let   queueRunning = false;

async function processQueue() {
    if (queueRunning) return;
    queueRunning = true;
    while (articleQueue.length > 0) {
        const { article, hash } = articleQueue.shift();
        await analyzeArticle(article, hash);
    }
    queueRunning = false;
}

async function deduplication(articles) {
    for (const article of articles) {
        try {
            const url = article.url || article.link;
            if (!url) continue;

            const urlExists = await redis.sismember('news_urls', url);
            if (urlExists) { console.log('[Dedup] Skipping URL:', url.slice(0, 60)); continue; }

            const title = article.title || article.headline;
            if (!title) continue;

            const normalized = normalize(title);
            const hash       = crypto.createHash('sha256').update(normalized).digest('hex');

            const hashExists = await redis.sismember('headline_hashes', hash);
            if (hashExists) { console.log('[Dedup] Skipping duplicate title:', title.slice(0, 60)); continue; }

            await redis.sadd('news_urls', url);
            await redis.sadd('headline_hashes', hash);
            await redis.expire('news_urls', 86400);
            await redis.expire('headline_hashes', 86400);

            console.log('[Dedup] Queued:', title.slice(0, 60));
            articleQueue.push({ article, hash });
        } catch (err) {
            console.error('[Dedup] Error:', err);
        }
    }
    processQueue();
}

// ─── News pollers ──────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

async function pollNewsData() {
    while (true) {
        try {
            const key  = newsio_bots[cnt1 % newsio_bots.length];
            const url  = 'https://newsdata.io/api/1/latest?apikey=' + key + '&language=en&category=business';
            const res  = await fetch(url);
            const data = await res.json();
            const latest = data.results?.slice(0, 5) || [];
            deduplication(latest);
            cnt1 = (cnt1 + 1) % newsio_bots.length;
        } catch (err) { console.error('[Poll:NewsData] Error:', err.message); }
        await sleep(interval1);
    }
}

async function pollMarketAux() {
    while (true) {
        try {
            const key  = marketaux_bots[cnt2 % marketaux_bots.length];
            const url  = `https://api.marketaux.com/v1/news/all?api_token=${key}&language=en&filter_entities=true`;
            const res  = await fetch(url);
            const data = await res.json();
            const latest = data.data?.slice(0, 5) || [];
            deduplication(latest);
            cnt2 = (cnt2 + 1) % marketaux_bots.length;
        } catch (err) { console.error('[Poll:MarketAux] Error:', err.message); }
        await sleep(interval2);
    }
}

async function pollFinnhub() {
    while (true) {
        try {
            const key  = finhub_bots[cnt3 % finhub_bots.length];
            const url  = `https://finnhub.io/api/v1/news?category=general&token=${key}`;
            const res  = await fetch(url);
            const data = await res.json();
            const latest = Array.isArray(data) ? data.slice(0, 5) : [];
            deduplication(latest);
            cnt3 = (cnt3 + 1) % finhub_bots.length;
        } catch (err) { console.error('[Poll:Finnhub] Error:', err.message); }
        await sleep(interval3);
    }
}

// ─── MongoDB connect & start ───────────────────────────────────────────────────
async function connectMongo() {
    try {
        mongoClient = new MongoClient(uri, {
            serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true }
        });
        await mongoClient.connect();
        await mongoClient.db('NEWS_TRACKER').command({ ping: 1 });
        console.log('[MongoDB] Connected');
        usersCollection = mongoClient.db('NEWS_TRACKER').collection('users');
        // ensure index on email for fast lookups
        await usersCollection.createIndex({ email: 1 }, { unique: true });
        console.log('[MongoDB] Email index ensured');
    } catch (e) {
        console.error('[MongoDB] Connection error:', e);
        process.exit(1);
    }
}

const PORT = process.env.PORT || 5000;
connectMongo().then(() => {
    server.listen(PORT, '0.0.0.0', () => console.log(`[Server] Running on port ${PORT}`));
    pollNewsData();
    pollMarketAux();
    pollFinnhub();
});