# Hub UI behind Cloudflare Access

The hub UI is read-only. It does not use an operator token, does not expose a
write endpoint, and its browser data contains only sanitized node, burst, and
event status.

1. Run the hub with `--ui-allow-cf-only` and keep `--listen` on its default
   `127.0.0.1:9377`. This opt-in serves `GET /ui` and `GET /ui/data.json`.
   Without it, both paths return 404.
2. Configure the Cloudflare Tunnel origin for `hub.robinco.dev` to the local
   hub address. Do not expose that origin directly to the Internet.
3. In Cloudflare Zero Trust, create an **Access application** for
   `https://hub.robinco.dev/ui*`. Add an Allow policy for the intended people,
   using Email one-time PIN and/or Google login. Leave the existing service
   token policy for node endpoints in place; the UI Access application is a
   separate browser-login policy.
4. Verify a browser request to `/ui` has both the Cloudflare routing header
   `Cf-Ray` (or `Cf-Connecting-Ip`) and the Access identity header
   `Cf-Access-Authenticated-User-Email`. The hub rejects a Cloudflare-routed
   request without that identity even though `cloudflared` reaches the origin
   from `127.0.0.1`. Loopback is accepted only when neither routing header is
   present, for a true local diagnostic request. Any other caller receives 404.

The UI fetches `/ui/data.json` every 15 seconds. It uses no external CDN or
browser token. The defense is the Cloudflare Access login policy plus the
hub's `Cf-Ray`/identity gate; keep Access in front of `/ui*` and do not expose
the origin.
