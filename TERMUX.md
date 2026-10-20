# devlink on Android (Termux)

Run the server on an Android phone instead of a PC. No root needed.
The client app and the server are then on the SAME device, so the client
line is exactly the same localhost line as on a PC.

## 1. Install Termux (from F-Droid, not the Play Store)

The Play Store build of Termux is outdated and unmaintained; its packages
are broken. Install **F-Droid** first (https://f-droid.org), then install
**Termux** from inside F-Droid.

## 2. One-time setup (paste once in Termux)

```
pkg update -y && pkg install -y git nodejs-lts && git clone https://github.com/vartec569/devlink.git ~/devlink && cd ~/devlink && npm install
```

Wait until it finishes (a few minutes on first run). `termux-setup.sh` in
this repo does the same steps and is safe to re-run.

## 3. Stop Android from killing the server

- Android Settings -> Apps -> Termux -> Battery -> **Unrestricted**
  (on Xiaomi/Poco phones this is called "No restrictions" in the app's
  battery saver setting — that step matters, their battery manager is
  aggressive).
- Keep the Termux session open while the server runs; `termux-start.sh`
  takes a wake lock automatically. Some phones still kill long-running
  background processes — if the session drops, just start again (step 4)
  and re-run the client line (step 5).

## 4. Start the server

```
cd ~/devlink && bash termux-start.sh
```

Leave it running. First start generates `.dl-secret` in the folder — the
controller needs that value once (same as on PC).

## 5. Connect the client

In the client app on the same phone, run the usual line:

```
loadstring(game:HttpGet("http://127.0.0.1:28432/bridge.lua"))()
```

It works unchanged because the server is on this device, so 127.0.0.1 is
the phone itself.

## 6. Controller access

The API listens on port 28431 on all interfaces of the phone. On a
tailnet, the controller reaches it at this phone's tailnet address, port
28431, with the secret from `.dl-secret`. The API is protected by that
secret; the bridge port (28432) stays bound to localhost, as always.

## Updating

```
cd ~/devlink && git pull && npm install
```

Then restart the server (step 4) and re-run the client line (step 5).
