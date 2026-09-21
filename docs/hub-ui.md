# Hub UI behind Cloudflare Access

`/ui` is read-only. It does not use an operator token, does not expose a
write endpoint, and its browser data contains only sanitized node, burst, and
event status. The operator chat screen `/chat` is a separate surface with
browser write paths and a stricter gate; see `docs/hub-chat.md`.

Substitute your hub's public hostname for `hub.example.invalid` wherever it
appears below.

1. Run the hub with `--ui-allow-cf-only` and keep `--listen` on its default
   `127.0.0.1:9377`. This opt-in serves `GET /ui` and `GET /ui/data.json`.
   Without it, both paths return 404.
2. Configure the Cloudflare Tunnel origin for `hub.example.invalid` to the
   local hub address. Do not expose that origin directly to the Internet.
3. In Cloudflare Zero Trust, create an **Access application** for
   `https://hub.example.invalid` whose protected paths include both `/ui*` and
   `/chat*`. One application's AUD tag covers every path in it, so the single
   `--cf-access-aud` value below authorizes both surfaces; an application
   scoped to `/ui*` alone leaves `/chat` requests without a JWT and they get
   404. Add an Allow policy for the intended people, using Email one-time PIN
   and/or Google login. Leave the existing service token policy for node
   endpoints in place; the UI Access application is a separate browser-login
   policy.
4. Configure `--cf-access-team <team>` and `--cf-access-aud <aud>` (the
   application's AUD tag from the Access dashboard). With these set, a
   `Cf-Access-Jwt-Assertion` is verified against the team certs at
   `https://<team>.cloudflareaccess.com/cdn-cgi/access/certs` — RS256
   signature, `aud`, and expiry — and grants `/ui` reads regardless of
   transport.

## What the read gate guarantees — and what it does not

- A non-loopback peer is admitted only with a verified Access JWT. Unsigned
  identity headers (`Cf-Access-Authenticated-User-Email`, `Cf-Ray`,
  `Cf-Connecting-Ip`) never authorize a remote peer.
- Loopback peers are admitted without a JWT, except a loopback request that
  presents Cloudflare routing headers must also carry the Access identity
  header (that is how the same-host `cloudflared` connector arrives).
- **Known limit (follow-up):** the loopback branch trusts that same-host
  processes may *read* hub status. That trust predates this change and is
  deliberately **not** extended to `/chat` or any state-changing surface —
  those require a verified JWT or the operator token. Tightening loopback
  reads (e.g. requiring a JWT for `/ui` too) is tracked as a follow-up.
