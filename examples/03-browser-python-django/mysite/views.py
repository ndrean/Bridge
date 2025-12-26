import json
import time
import base64
import nkeys
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

# ⚠️ PASTE YOUR VALID SEED HERE (The one you generated with nacl, starting with SA...)
ACCOUNT_SEED = b"SAAIKDVVH5XBM56LB7BYVSK5DCDDKIFFQQWGMDRHUBTHCETC5RMYB2HTOM" 

# --- HELPER: Manual JWT Construction ---
# The nkeys library lacks a 'create_user' helper, so we build the JWT string manually.
def sign_jwt(account_kp, claims):
    header = {"typ": "jwt", "alg": "ed25519-nkey"}
    
    # 1. Base64URL Encode Helper (No padding '=')
    def b64url(data):
        return base64.urlsafe_b64encode(data).rstrip(b'=')

    # 2. Serialize Header & Claims to Compact JSON
    h_json = json.dumps(header, separators=(',', ':')).encode()
    c_json = json.dumps(claims, separators=(',', ':')).encode()
    
    # 3. Encode Parts
    h_enc = b64url(h_json)
    c_enc = b64url(c_json)
    
    # 4. Sign the "Header.Claims" string
    payload_to_sign = h_enc + b"." + c_enc
    # account_kp.sign() returns the raw signature bytes
    sig = account_kp.sign(payload_to_sign)
    
    # 5. Return: Header.Claims.Signature
    return (payload_to_sign + b"." + b64url(sig)).decode()

@csrf_exempt
def nats_auth(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'POST only'}, status=405)

    try:
        data = json.loads(request.body)
        user_pub_key = data.get('public_key')
        
        if not user_pub_key:
            return JsonResponse({'error': 'Missing public_key'}, status=400)

        # 1. Load the Account Keypair from Seed
        # This is the ONLY nkeys function we rely on now.
        account_kp = nkeys.from_seed(ACCOUNT_SEED)
        
        # 2. Define User Permissions (Claims)
        claims = {
            "jti": str(time.time()),      # Unique ID
            "iat": int(time.time()),      # Issued At
            "exp": int(time.time()) + 3600, # Expires in 1 hour
            "iss": account_kp.public_key.decode(), # Issuer (Us)
            "name": "browser_user",
            "sub": user_pub_key,          # Subject (The Browser's Public Key)
            "nats": {
                "pub": {},                # Read-Only
                "sub": {
                    "allow": ["cdc.>"]    # Allow reading cdc streams
                },
                "type": "user"            # Required claim type
            }
        }

        # 3. Sign it using our manual helper
        # WE REMOVED nkeys.create_user() HERE
        encoded_jwt = sign_jwt(account_kp, claims)

        return JsonResponse({'jwt': encoded_jwt})

    except Exception as e:
        print(f"Auth Error: {e}")
        return JsonResponse({'error': str(e)}, status=500)