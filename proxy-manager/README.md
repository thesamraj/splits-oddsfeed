# Proxy Manager

Simple proxy rotation service for collectors that need to bypass geo-restrictions.

## Configuration

Create a `proxies.json` file (gitignored) with your proxy list:

```json
[
  "http://user1:pass1@proxy1.example.com:8080",
  "socks5://user2:pass2@proxy2.example.com:1080",
  "http://user3:pass3@proxy3.example.com:3128"
]
```

## API Endpoints

- `GET /lease?book=kambi&region=us` - Lease a proxy
- `POST /release` - Release/ban a proxy
- `GET /health` - Health check

## Example Usage

```bash
# Lease a proxy
curl "http://localhost:8099/lease?book=kambi&region=us"
# Returns: {"proxy":"http://user:pass@host:port"}

# Release a problematic proxy
curl -X POST http://localhost:8099/release \
  -H "Content-Type: application/json" \
  -d '{"proxy":"http://user:pass@host:port","reason":"403_error","book":"kambi"}'
```

Banned proxies are automatically unbanned after 5 minutes.
