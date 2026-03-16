const https = require('https');
const fs = require('fs');
const path = require('path');

// ── Config ──────────────────────────────────────────────────────────────────
const MUX_TOKEN_ID     = process.env.MUX_TOKEN_ID;
const MUX_TOKEN_SECRET = process.env.MUX_TOKEN_SECRET;
const CSV_PATH         = path.join(__dirname, 'metadata.csv');
const OUTPUT_PATH      = path.join(__dirname, 'feed.json');

// ── Helpers ──────────────────────────────────────────────────────────────────

// Simple HTTPS GET with Basic Auth
function muxGet(endpoint) {
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(`${MUX_TOKEN_ID}:${MUX_TOKEN_SECRET}`).toString('base64');
    const options = {
      hostname: 'api.mux.com',
      path: endpoint,
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${auth}`
      }
    };
    let body = '';
    const req = https.request(options, res => {
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`Failed to parse Mux response: ${body}`)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// Fetch ALL Mux assets (handles pagination)
async function fetchAllMuxAssets() {
  const assets = {};
  let cursor = null;

  do {
    const qs = cursor ? `?page_token=${cursor}` : '';
    const res = await muxGet(`/video/v1/assets${qs}`);
    if (!res.data) break;

    for (const asset of res.data) {
      // Index by each playback ID so we can look up by playbackId
      if (asset.playback_ids) {
        for (const pb of asset.playback_ids) {
          assets[pb.id] = asset;
        }
      }
    }
    cursor = res.next_cursor || null;
  } while (cursor);

  return assets;
}

// Parse CSV — handles quoted fields containing commas
function parseCSV(text) {
  const lines = text.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim().replace(/\r/g, ''));
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].replace(/\r/g, '');
    if (!line.trim()) continue;

    const values = [];
    let current = '';
    let inQuotes = false;

    for (let c = 0; c < line.length; c++) {
      const char = line[c];
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());

    const row = {};
    headers.forEach((h, idx) => row[h] = values[idx] || '');
    rows.push(row);
  }

  return rows;
}

// Map Mux resolution to quality label
function getQuality(asset) {
  if (!asset) return 'HD';
  const tier = asset.resolution_tier || asset.max_stored_resolution || '';
  if (tier.includes('2160') || tier === '4k') return '4K';
  if (tier.includes('1080') || tier === 'HD') return 'HD';
  if (tier.includes('720'))  return 'HD';
  return 'HD';
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  if (!MUX_TOKEN_ID || !MUX_TOKEN_SECRET) {
    console.error('❌  MUX_TOKEN_ID and MUX_TOKEN_SECRET environment variables are required.');
    process.exit(1);
  }

  console.log('📡  Fetching assets from Mux...');
  const muxAssets = await fetchAllMuxAssets();
  console.log(`✅  Found ${Object.keys(muxAssets).length} Mux assets`);

  console.log('📄  Reading metadata.csv...');
  const csvText = fs.readFileSync(CSV_PATH, 'utf8');
  const rows = parseCSV(csvText);
  console.log(`✅  Found ${rows.length} rows in metadata.csv`);

  const movies = [];

  for (const row of rows) {
    const playbackId = row.muxPlaybackId;
    if (!playbackId) {
      console.warn(`⚠️   Skipping row "${row.id}" — no muxPlaybackId`);
      continue;
    }

    const asset = muxAssets[playbackId];
    if (!asset) {
      console.warn(`⚠️   Mux asset not found for playbackId "${playbackId}" (id: ${row.id}) — using CSV fallback`);
    }

    const duration = asset ? Math.round(asset.duration) : 0;
    const quality  = getQuality(asset);

    movies.push({
      id: row.id,
      title: row.title,
      shortDescription: row.shortDescription,
      thumbnail: row.thumbnail,
      genres: [row.genre],
      releaseDate: row.releaseDate,
      content: {
        dateAdded: row.dateAdded,
        videos: [{
          url: `https://stream.mux.com/${playbackId}.m3u8`,
          quality,
          videoType: 'HLS'
        }],
        duration
      }
    });
  }

  const feed = {
    providerName: 'One World Network',
    lastUpdated: new Date().toISOString(),
    language: 'en',
    movies
  };

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(feed, null, 2));
  console.log(`\n🎉  feed.json written with ${movies.length} entries`);
}

main().catch(err => {
  console.error('❌  Error:', err.message);
  process.exit(1);
});
