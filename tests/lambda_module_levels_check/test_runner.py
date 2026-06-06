import uuid
import httpx
import asyncio
import datetime
import json

async def test(control_plane_ip, lambda_name):
    result = {}
    try:
        async with httpx.AsyncClient(verify=False) as client:
            resp = await client.post(
                f"http://{control_plane_ip}:444/12-12-1122/functions/{lambda_name}/invocations",
                json={},
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




async def load_test(control_plane_ip, lambda_name, iters, ramp_up_factor):
    results = []
    for i in range(iters):
        tasks = []
        requests_no = i*ramp_up_factor
        print(requests_no)
        for _ in range(requests_no):
           tasks.append(asyncio.create_task(test(control_plane_ip, lambda_name)))
        results.extend(await asyncio.gather(*tasks))
    with open(f"outputs-{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json", "w") as f:
        json.dump(results, f, indent=4)
    return 

control_plane_ip = "172.18.0.2"
lambda_name = "pie_lambda_module_levels_test"
iters = 10
ramp_up_factor = 1

asyncio.run(load_test(control_plane_ip, lambda_name, iters, ramp_up_factor))
    
