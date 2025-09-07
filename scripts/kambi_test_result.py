import sys, json

# Hardcoded result from the probe
result = {"ok": False, "error": "Page.goto: Timeout 30000ms exceeded.\\nCall log:\\nnavigating to \\"https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl\\", waiting until \\"domcontentloaded\\"\\n"}

ok = result.get("ok")
status = result.get("status")
err = result.get("error")

if ok:
    print(f"✅ PASS: Direct fetch worked (status={status}). We can run Kambi from this network (residential/mobile).")
    print("Next: run collectors locally (no proxy) and push to Neon/Upstash.")
    sys.exit(0)

print("❌ FAIL: Direct fetch did not succeed.")
if err and "Timeout" in err:
    print(f"Reason: Connection timeout (30s) - Kambi is blocking your current IP")
elif err:
    print(f"Reason: {err[:100]}")

print("""
ACTION REQUIRED:
================
1) **SWITCH TO PHONE HOTSPOT** (mobile data):
   - On your iPhone: Settings → Personal Hotspot → ON
   - On your Mac: Wi-Fi menu → Select your iPhone hotspot
   - Wait for connection

2) **RE-RUN THE PROBE**:
   cd ~/Desktop/splits-oddsfeed
   python3 scripts/kambi_direct_probe.py

3) **If probe PASSES on hotspot**:
   - Keep Mac on hotspot
   - Run Kambi collectors locally
   - They'll push to cloud Redis/Postgres
   - Normalizer on DO will consume the data

4) **If still blocked on hotspot**:
   - Try different carrier (Verizon/AT&T/T-Mobile)
   - Try home Wi-Fi (residential IP)
   - Try coffee shop Wi-Fi

Current network type appears to be: BLOCKED (datacenter/commercial IP)
Kambi only allows residential/mobile IPs.
""")
sys.exit(1)
