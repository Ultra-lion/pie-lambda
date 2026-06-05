import uuid
import httpx
import asyncio
import datetime
import json

async def test(control_plane_ip, lambda_name, test_payload):
    consistency_key = str(uuid.uuid4())
    test_payload["payload"] = consistency_key
    result = {}
    result["consistency_key"] = consistency_key
    try:
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.post(
                f"https://{control_plane_ip}:443/12-12-1122/functions/{lambda_name}/invocations",
                json=test_payload,
                timeout=30
            )
            if resp.status_code != 200:
                result["error"] = resp.text
                print("failure", resp.text)
            else:
                result["response"] = resp.text
    except Exception as e:
        print(e)
        print("failure", str(e))
        result["error"] = str(e)
    return result




async def load_test(control_plane_ip, lambda_name, test_payload, iters, ramp_up_factor):
    results = []
    for i in range(iters):
        tasks = []
        requests_no = i*ramp_up_factor
        print(requests_no)
        for _ in range(requests_no):
           tasks.append(asyncio.create_task(test(control_plane_ip, lambda_name, test_payload)))
        results.extend(await asyncio.gather(*tasks))
        for res in results:
            consistency_key = res.get("consistency_key")
            response = res.get("response")
            if not response:
                continue
            body = json.loads(response)
            body = json.loads(body)
            payload = body.get("payload")
            if payload != consistency_key:
                print(f"consistency key mismatch consistency_key={consistency_key} payload: {payload}")
    with open(f"outputs-{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json", "w") as f:
        json.dump(results, f, indent=4)
    return 

control_plane_ip = "172.18.0.2"
lambda_name = "pie_test_lambda_1"
test_payload = {}
iters = 100
ramp_up_factor = 2

asyncio.run(load_test(control_plane_ip, lambda_name, test_payload, iters, ramp_up_factor))
    
