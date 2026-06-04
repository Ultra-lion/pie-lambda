import uuid
import httpx
import asyncio



async def test(control_plane_ip, lambda_name, test_payload):
    consistency_key = str(uuid.uuid4())
    test_payload["payload"] = consistency_key

    async with httpx.AsyncClient(verify=False) as client:
        resp = await client.post(
            f"https://{control_plane_ip}:443/12-12-1122/functions/{lambda_name}/invocations",
            json=test_payload,
            timeout=100
        )
        print(resp.text)


control_plane_ip = "172.18.0.2"
lambda_name = "pie_test_lambda_1"
test_payload = {}

asyncio.run(test(control_plane_ip, lambda_name, test_payload))
    
