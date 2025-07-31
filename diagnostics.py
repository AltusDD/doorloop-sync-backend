import socket
import ssl

host = "db.ssexobxvtuxwnblwplzh.supabase.co"
port = 5432

print(f"🔍 Diagnosing Supabase connection to {host}:{port}")

try:
    ip_addresses = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    print(f"✅ DNS Resolution: {len(ip_addresses)} addresses found")

    for _, _, _, _, sockaddr in ip_addresses:
        try:
            with socket.create_connection((sockaddr[0], port), timeout=5):
                print(f"✅ TCP OK to {sockaddr[0]}")
        except Exception as e:
            print(f"❌ TCP FAIL to {sockaddr[0]}: {e}")

    context = ssl.create_default_context()
    with socket.create_connection((host, port), timeout=5) as sock:
        with context.wrap_socket(sock, server_hostname=host) as ssock:
            print("✅ SSL handshake successful")

except Exception as e:
    print(f"❌ Overall network check failed: {e}")