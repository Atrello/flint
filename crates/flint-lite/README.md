# Flint Lite

A slim client for chatting with friends on a Steel server. It has no servers or guilds, just
friends, direct messages, group messages, and peer-to-peer screen share.

- Chat, friends, groups and offline delivery go through Steel.
- Screen share is peer to peer over WebRTC. Steel only relays the offer, answer and ICE
  candidates (`CALL_SIGNAL`). Frames never touch the server.
- Voice and video calls are not built yet.

## Run

```sh
cargo run -p flint-lite -- --server http://127.0.0.1:8080
```

Options: `--home DIR` keeps the saved sign-in in a separate folder, so two copies can run on one
machine as two users. `--test-pattern` shares a synthetic moving frame instead of the screen, for
machines without a display.

Steel must run with the call signaling routes (`/api/v10/calls/*`). For friends on different
networks, set `STEEL_TURN_URLS`, `STEEL_TURN_USERNAME` and `STEEL_TURN_CREDENTIAL` on the server
so direct connections can fall back to a relay.

## Test

```sh
cargo test -p flint-lite                      # unit tests and the WebRTC loopback test
FLINT_LITE_TEST_SERVER=http://127.0.0.1:8080 \
  cargo test -p flint-lite -- --ignored       # needs a running steeld: chat, friends, DM and share
```
