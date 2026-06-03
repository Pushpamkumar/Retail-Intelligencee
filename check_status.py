import urllib.request
import urllib.error

try:
    url = "http://localhost:8000/store/live-status"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        print("Success:", response.read().decode())
except urllib.error.HTTPError as e:
    print("HTTP Error code:", e.code)
    try:
        print("HTTP Error response body:", e.read().decode())
    except Exception as read_err:
        print("Could not read body:", read_err)
except Exception as e:
    print("Other error:", e)
