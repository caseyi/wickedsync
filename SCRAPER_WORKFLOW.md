# WickedSync Browser Scraper Workflow

This describes how Claude-in-Chrome scrapes Gumroad download URLs
and posts them directly to the NAS WickedSync API.

## How it works

```
Chrome browser (Mac, home IP)
  │
  ├── Navigate to gumroad.com/d/[hash]
  ├── Click each download button
  ├── Capture CDN redirect URLs from network requests
  │     (files.gumroad.com / d2dw6lv4z9w0e2.cloudfront.net)
  │
  └── POST /api/ingest → NAS WickedSync (192.168.1.168:8088)
                              │
                              └── Download worker pulls CDN URLs
                                  (NAS shares same public IP → works)
```

## NAS addresses

| NAS      | IP               | Port | Categories       |
|----------|-----------------|------|------------------|
| Endor    | 192.168.1.168   | 8088 | Marvel           |
| Dagobah  | 192.168.1.140   | 8088 | Movies, VG       |

## Claude's scraping steps (per product)

1. Navigate to the product URL (l/ slug with discount code, or d/ hash directly)
2. If on a product page: find and click "View content" to get to d/ URL
3. Clear network requests log
4. Click every download button on the d/ page
5. Read network requests — filter for `cloudfront.net` or `files.gumroad.com`
6. Extract filename from URL path (URL-decoded last segment)
7. Add to the batch array
8. POST batch to the correct NAS when done with a group

## JavaScript used to POST to NAS

```javascript
const files = [
  {
    cdn_url: "https://d2dw6lv4z9w0e2.cloudfront.net/...",
    filename: "Wicked - Blade (Non Supported).zip",
    model_name: "Blade Sculpture",
    term: "Marvel"
  },
  // ...
];

const res = await fetch('http://192.168.1.168:8088/api/ingest', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ files })
});
const data = await res.json();
console.log(data);
// → { result: "ingested", total_files_queued: 6, ... }
```

## CDN URL expiry

CloudFront URLs expire in ~3 hours. Run the scraper and make sure the
NAS starts downloading within that window. With 6 concurrent downloads
and ~2 min/file, 162 files (27 products × ~6 variants each) takes
roughly 55 minutes — well within the window.

## Batch routing

Items 1–9 (Oct Marvel) + items 19–27 (Nov Marvel) → POST to Endor (192.168.1.168:8088)
Items 10–13 (Oct Movies) + items 14–18 (Oct VG) → POST to Dagobah (192.168.1.140:8088)
