import time, json, urllib.request, subprocess, sys

JOB_ID = "2f184cd6-4bba-4647-aed5-01da30cd14ed"
job_url = f"http://localhost:8000/jobs/{JOB_ID}"
csv_path = "/Users/tilman/maps-lead-scraper/baubranche_nuernberg_50km.csv"

while True:
    r = urllib.request.urlopen(job_url)
    job = json.loads(r.read())
    s = job["status"]
    print(f"Status: {s} | {job['processed_locations']}/{job['total_locations']} | "
          f"{job['total_leads']} leads | {job['total_duplicates']} dupes | "
          f"{job['total_api_calls']} API calls", flush=True)
    if s in ("completed", "failed", "cancelled"):
        print(json.dumps(job, indent=2))
        csv_url = "http://localhost:8000/leads?country=de&format=csv&limit=10000"
        urllib.request.urlretrieve(csv_url, csv_path)
        result = subprocess.run(["wc", "-l", csv_path], capture_output=True, text=True)
        print(result.stdout.strip())
        print(f"CSV saved to: {csv_path}")
        break
    time.sleep(30)
